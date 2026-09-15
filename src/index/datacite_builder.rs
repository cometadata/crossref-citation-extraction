//! Build the DataCite index bundle from a verified monthly release.
//!
//! The registry is scanned exactly once. Each record's canonical DOI, and
//! for each usable landing page the normalized URL/DOI pair and the URL's
//! host, are appended to three temporary text files as hex-encoded keys
//! (so no delimiter can occur inside a key, and hex lexical order equals
//! byte order, which the FST builders require). Each file is then sorted
//! and deduplicated by the system `sort` under `LC_ALL=C` with an explicit
//! memory budget and an owned temporary directory, and the sorted streams
//! are decoded straight into `dois.fst`, `hosts.fst`, `urls.fst` and
//! `url-targets.bin`. Nothing holds the registry in memory.
//!
//! Everything is written inside an owned staging directory next to the
//! output. Only after the scan verified every source digest, every artifact
//! was fsynced and hashed, and the manifest marker was written last is the
//! staging directory renamed into place; any failure removes it, so an
//! output path either holds a complete bundle or nothing.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use anyhow::{anyhow, bail, ensure, Context, Result};
use fst::{MapBuilder, SetBuilder};
use log::info;
use url::Url;
use uuid::Uuid;

use crate::common::identifiers::normalize_landing_url;
use crate::index::datacite::{
    BundleManifest, IndexFile, BUNDLE_FILES, BUNDLE_KIND, DOIS_FILE, FORMAT_VERSION, HOSTS_FILE,
    MARKER_FILE, NORMALIZATION_VERSION, URLS_FILE, URL_TARGETS_FILE,
};
use crate::input::datacite_release::{MonthlyRelease, SourceStats};

/// Buffer size for reading sorted key files.
const READ_BUFFER: usize = 256 * 1024;
/// Buffer size for writing temporary key files and bundle artifacts.
const WRITE_BUFFER: usize = 1024 * 1024;
/// Temporary directory inside the staging directory, for the unsorted and
/// sorted key files and for `sort -T`.
const TEMP_DIR: &str = "tmp";
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Build the bundle for the release at `release_root` into `output_dir`.
///
/// `sort_memory_mb` is the external sort's memory budget in MiB and must
/// be positive. `output_dir` must not already hold a completed bundle, and
/// if it exists it must be an empty directory. Returns the manifest that
/// was written as the completion marker.
pub fn build_datacite_index(
    release_root: &Path,
    output_dir: &Path,
    sort_memory_mb: usize,
) -> Result<BundleManifest> {
    ensure!(
        sort_memory_mb > 0,
        "sort memory budget must be at least 1 MiB"
    );
    check_output_available(output_dir)?;
    let release = MonthlyRelease::open(release_root)?;
    let staging = Staging::create(output_dir)?;
    let manifest = build_into(&release, &staging.dir, sort_memory_mb).with_context(|| {
        format!(
            "failed to build DataCite index for {}",
            release_root.display()
        )
    })?;
    staging.promote(output_dir)?;
    info!(
        "DataCite index bundle for release {} written to {}",
        manifest.release,
        output_dir.display()
    );
    Ok(manifest)
}

/// Refuse an output path that already holds a completed bundle or any
/// other content.
fn check_output_available(output_dir: &Path) -> Result<()> {
    let marker = output_dir.join(MARKER_FILE);
    ensure!(
        !marker.exists(),
        "a completed DataCite index bundle already exists at {}",
        output_dir.display()
    );
    match fs::read_dir(output_dir) {
        Ok(mut entries) => ensure!(
            entries.next().is_none(),
            "output directory {} exists and is not empty",
            output_dir.display()
        ),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("failed to inspect {}", output_dir.display()))
        }
    }
    Ok(())
}

/// An owned staging directory beside the output, removed on drop unless it
/// was promoted.
struct Staging {
    dir: PathBuf,
    promoted: bool,
}

impl Staging {
    fn create(output_dir: &Path) -> Result<Self> {
        let name = output_dir
            .file_name()
            .ok_or_else(|| anyhow!("output path {} must name a directory", output_dir.display()))?
            .to_string_lossy();
        let parent = output_parent(output_dir);
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
        let dir = parent.join(format!(".{name}.staging-{}", Uuid::new_v4()));
        fs::create_dir(&dir)
            .with_context(|| format!("failed to create staging directory {}", dir.display()))?;
        Ok(Self {
            dir,
            promoted: false,
        })
    }

    /// Atomically rename the finished staging directory to `output_dir`.
    fn promote(mut self, output_dir: &Path) -> Result<()> {
        check_output_available(output_dir)?;
        if output_dir.exists() {
            // Verified empty above; rename cannot replace a directory
            // portably, so clear the way first.
            fs::remove_dir(output_dir)
                .with_context(|| format!("failed to remove empty {}", output_dir.display()))?;
        }
        fs::rename(&self.dir, output_dir).with_context(|| {
            format!(
                "failed to move {} to {}",
                self.dir.display(),
                output_dir.display()
            )
        })?;
        self.promoted = true;
        sync_directory(output_parent(output_dir))
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        if !self.promoted {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }
}

/// The directory that will contain `output_dir`; `.` for a bare name.
fn output_parent(output_dir: &Path) -> &Path {
    output_dir
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

/// Build every artifact and the marker inside `staging`.
fn build_into(
    release: &MonthlyRelease,
    staging: &Path,
    sort_memory_mb: usize,
) -> Result<BundleManifest> {
    let started = Instant::now();
    let temp = staging.join(TEMP_DIR);
    fs::create_dir(&temp).with_context(|| format!("failed to create {}", temp.display()))?;

    let scan = scan_release(release, &temp)?;
    info!(
        "Scanned release {} in {:.1}s; sorting keys with a {sort_memory_mb} MiB budget",
        release.release,
        started.elapsed().as_secs_f64()
    );

    let sorted = external_sort(&temp, "dois", sort_memory_mb)?;
    let dois = build_set(&sorted, &staging.join(DOIS_FILE))?;
    fs::remove_file(&sorted).with_context(|| format!("failed to remove {}", sorted.display()))?;
    ensure!(
        dois > 0,
        "release {} yields no DOIs; refusing to build an empty index",
        release.release
    );
    info!("Wrote {DOIS_FILE}: {dois} DOIs");

    let sorted = external_sort(&temp, "hosts", sort_memory_mb)?;
    let hosts = build_set(&sorted, &staging.join(HOSTS_FILE))?;
    fs::remove_file(&sorted).with_context(|| format!("failed to remove {}", sorted.display()))?;
    info!("Wrote {HOSTS_FILE}: {hosts} hosts");

    let sorted = external_sort(&temp, "urls", sort_memory_mb)?;
    let url_counts = build_url_map(
        &sorted,
        &staging.join(URLS_FILE),
        &staging.join(URL_TARGETS_FILE),
    )?;
    fs::remove_file(&sorted).with_context(|| format!("failed to remove {}", sorted.display()))?;
    info!(
        "Wrote {URLS_FILE} and {URL_TARGETS_FILE}: {} URLs, {} URL/DOI pairs, {} shared URLs",
        url_counts.urls, url_counts.pairs, url_counts.shared
    );
    fs::remove_dir_all(&temp).with_context(|| format!("failed to remove {}", temp.display()))?;

    let mut files = BTreeMap::new();
    for name in BUNDLE_FILES {
        files.insert(name.to_string(), IndexFile::of_file(&staging.join(name))?);
    }
    let counts = BTreeMap::from([
        ("metadata_records".to_string(), scan.stats.metadata_records),
        ("csv_records".to_string(), scan.stats.csv_records),
        ("dois".to_string(), dois),
        ("hosts".to_string(), hosts),
        ("urls".to_string(), url_counts.urls),
        ("url_pairs".to_string(), url_counts.pairs),
        ("shared_urls".to_string(), url_counts.shared),
        ("missing_urls".to_string(), scan.stats.missing_urls),
        (
            "invalid_urls".to_string(),
            scan.stats.invalid_urls + scan.unnormalizable_urls,
        ),
    ]);
    let manifest = BundleManifest {
        kind: BUNDLE_KIND.to_string(),
        format_version: FORMAT_VERSION,
        normalization_version: NORMALIZATION_VERSION,
        release: release.release.clone(),
        source_inventory_sha256: release.inventory_sha256.clone(),
        files,
        counts,
    };

    // Every artifact is synced and hashed; the marker goes last.
    let marker = staging.join(MARKER_FILE);
    let mut file =
        File::create(&marker).with_context(|| format!("failed to create {}", marker.display()))?;
    file.write_all(&manifest.to_marker_bytes()?)
        .and_then(|()| file.sync_all())
        .with_context(|| format!("failed to write {}", marker.display()))?;
    sync_directory(staging)?;
    info!(
        "Built DataCite index bundle in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    Ok(manifest)
}

/// Counts from the registry scan.
struct ScanCounts {
    stats: SourceStats,
    /// String `attributes.url` values that are not valid HTTP/HTTPS URLs.
    unnormalizable_urls: u64,
}

/// Scan the release once, appending hex-encoded keys to `<temp>/dois`,
/// `<temp>/hosts` and `<temp>/urls` (the latter as `url\tdoi` pairs).
fn scan_release(release: &MonthlyRelease, temp: &Path) -> Result<ScanCounts> {
    let mut dois = KeyWriter::create(&temp.join("dois"))?;
    let mut hosts = KeyWriter::create(&temp.join("hosts"))?;
    let mut urls = KeyWriter::create(&temp.join("urls"))?;
    let mut unnormalizable_urls = 0u64;
    let stats = release.scan(&mut |record| {
        dois.write_key(record.doi.as_bytes())?;
        let Some(raw) = record.url.as_deref() else {
            return Ok(());
        };
        let Some(url) = normalize_landing_url(raw) else {
            unnormalizable_urls += 1;
            return Ok(());
        };
        let parsed =
            Url::parse(&url).with_context(|| format!("normalized URL {url:?} does not parse"))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow!("normalized URL {url:?} has no host"))?;
        hosts.write_key(host.as_bytes())?;
        urls.write_pair(url.as_bytes(), record.doi.as_bytes())?;
        Ok(())
    })?;
    dois.finish()?;
    hosts.finish()?;
    urls.finish()?;
    Ok(ScanCounts {
        stats,
        unnormalizable_urls,
    })
}

/// Appends hex-encoded keys, one record per line, to a temporary file.
struct KeyWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    line: Vec<u8>,
}

impl KeyWriter {
    fn create(path: &Path) -> Result<Self> {
        let file =
            File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::with_capacity(WRITE_BUFFER, file),
            line: Vec::new(),
        })
    }

    fn write_key(&mut self, key: &[u8]) -> Result<()> {
        self.line.clear();
        push_hex(&mut self.line, key);
        self.line.push(b'\n');
        self.flush_line()
    }

    fn write_pair(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.line.clear();
        push_hex(&mut self.line, key);
        self.line.push(b'\t');
        push_hex(&mut self.line, value);
        self.line.push(b'\n');
        self.flush_line()
    }

    fn flush_line(&mut self) -> Result<()> {
        self.writer
            .write_all(&self.line)
            .with_context(|| format!("failed to write {}", self.path.display()))
    }

    fn finish(mut self) -> Result<()> {
        self.writer
            .flush()
            .with_context(|| format!("failed to flush {}", self.path.display()))
    }
}

/// Sort and deduplicate `<temp>/<name>` into `<temp>/<name>.sorted` with
/// the system `sort` in byte order, then remove the unsorted file.
fn external_sort(temp: &Path, name: &str, memory_mb: usize) -> Result<PathBuf> {
    let unsorted = temp.join(name);
    let sorted = temp.join(format!("{name}.sorted"));
    let output = Command::new("sort")
        .env("LC_ALL", "C")
        .arg("-u")
        .arg("-S")
        .arg(format!("{memory_mb}M"))
        .arg("-T")
        .arg(temp)
        .arg("-o")
        .arg(&sorted)
        .arg(&unsorted)
        .stdin(Stdio::null())
        .output()
        .context("failed to run the system `sort` command")?;
    ensure!(
        output.status.success(),
        "sort failed for {} ({}): {}",
        unsorted.display(),
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    fs::remove_file(&unsorted)
        .with_context(|| format!("failed to remove {}", unsorted.display()))?;
    Ok(sorted)
}

/// Reads hex-encoded records from a sorted key file, one per line.
struct SortedKeys {
    path: PathBuf,
    reader: BufReader<File>,
    line: Vec<u8>,
}

impl SortedKeys {
    fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        Ok(Self {
            path: path.to_path_buf(),
            reader: BufReader::with_capacity(READ_BUFFER, file),
            line: Vec::new(),
        })
    }

    /// The next line without its newline, or `None` at EOF.
    fn next_line(&mut self) -> Result<Option<&[u8]>> {
        self.line.clear();
        let read = self
            .reader
            .read_until(b'\n', &mut self.line)
            .with_context(|| format!("failed to read {}", self.path.display()))?;
        if read == 0 {
            return Ok(None);
        }
        if self.line.last() == Some(&b'\n') {
            self.line.pop();
        }
        Ok(Some(&self.line))
    }
}

/// Stream a sorted key file into an FST set; returns the key count.
fn build_set(sorted: &Path, output: &Path) -> Result<u64> {
    let mut keys = SortedKeys::open(sorted)?;
    let file =
        File::create(output).with_context(|| format!("failed to create {}", output.display()))?;
    let mut builder = SetBuilder::new(BufWriter::with_capacity(WRITE_BUFFER, file))?;
    let mut key = Vec::new();
    let mut count = 0u64;
    while let Some(line) = keys.next_line()? {
        decode_hex(line, &mut key).with_context(|| format!("{}", sorted.display()))?;
        builder
            .insert(&key)
            .with_context(|| format!("failed to insert into {}", output.display()))?;
        count += 1;
    }
    let writer = builder
        .into_inner()
        .with_context(|| format!("failed to finish {}", output.display()))?;
    finish_file(writer, output)?;
    Ok(count)
}

/// Counts from the URL map build.
struct UrlCounts {
    /// Distinct URLs.
    urls: u64,
    /// Distinct URL/DOI pairs.
    pairs: u64,
    /// URLs with two or more DOIs.
    shared: u64,
}

/// Stream sorted `url\tdoi` pairs into the URL map and its postings file.
///
/// Consecutive pairs with the same URL form one group. The group's
/// candidate count is written as a placeholder when the group starts and
/// patched once its last DOI has been streamed, so no group is held in
/// memory.
fn build_url_map(sorted: &Path, urls_path: &Path, targets_path: &Path) -> Result<UrlCounts> {
    let mut keys = SortedKeys::open(sorted)?;
    let file = File::create(urls_path)
        .with_context(|| format!("failed to create {}", urls_path.display()))?;
    let mut builder = MapBuilder::new(BufWriter::with_capacity(WRITE_BUFFER, file))?;
    let mut targets = PostingWriter::create(targets_path)?;
    let mut counts = UrlCounts {
        urls: 0,
        pairs: 0,
        shared: 0,
    };
    let mut url = Vec::new();
    let mut doi = Vec::new();
    let mut group: Option<Group> = None;
    while let Some(line) = keys.next_line()? {
        let tab = line
            .iter()
            .position(|&byte| byte == b'\t')
            .ok_or_else(|| anyhow!("{}: malformed URL record", sorted.display()))?;
        let (url_hex, doi_hex) = (&line[..tab], &line[tab + 1..]);
        decode_hex(doi_hex, &mut doi).with_context(|| format!("{}", sorted.display()))?;
        let current = match group {
            Some(ref mut current) if current.url_hex == url_hex => current,
            ref mut slot => {
                if let Some(finished) = slot.take() {
                    finished.close(&mut targets, &mut counts)?;
                }
                decode_hex(url_hex, &mut url).with_context(|| format!("{}", sorted.display()))?;
                let offset = targets.position();
                targets.write(&0u64.to_le_bytes())?;
                builder
                    .insert(&url, offset)
                    .with_context(|| format!("failed to insert into {}", urls_path.display()))?;
                counts.urls += 1;
                slot.insert(Group {
                    url_hex: url_hex.to_vec(),
                    offset,
                    count: 0,
                })
            }
        };
        let length = u32::try_from(doi.len())
            .map_err(|_| anyhow!("DOI of {} bytes exceeds the posting format", doi.len()))?;
        targets.write(&length.to_le_bytes())?;
        targets.write(&doi)?;
        counts.pairs += 1;
        current.count += 1;
    }
    if let Some(finished) = group.take() {
        finished.close(&mut targets, &mut counts)?;
    }
    let writer = builder
        .into_inner()
        .with_context(|| format!("failed to finish {}", urls_path.display()))?;
    finish_file(writer, urls_path)?;
    targets.finish()?;
    Ok(counts)
}

/// The URL group currently being streamed into the postings file.
struct Group {
    url_hex: Vec<u8>,
    /// Where the group's count placeholder was written.
    offset: u64,
    count: u64,
}

impl Group {
    fn close(self, targets: &mut PostingWriter, counts: &mut UrlCounts) -> Result<()> {
        targets.patch(self.offset, &self.count.to_le_bytes())?;
        if self.count >= 2 {
            counts.shared += 1;
        }
        Ok(())
    }
}

/// A buffered append-only writer for `url-targets.bin` that can also patch
/// bytes it wrote earlier: in the buffer when they are still pending,
/// otherwise in place on disk.
struct PostingWriter {
    path: PathBuf,
    file: File,
    buffer: Vec<u8>,
    /// Bytes already written to the file, i.e. the file offset of `buffer`.
    flushed: u64,
}

impl PostingWriter {
    fn create(path: &Path) -> Result<Self> {
        let file =
            File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            buffer: Vec::with_capacity(WRITE_BUFFER),
            flushed: 0,
        })
    }

    /// The offset the next write lands at.
    fn position(&self) -> u64 {
        self.flushed + self.buffer.len() as u64
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        if self.buffer.len() + bytes.len() > WRITE_BUFFER {
            self.flush()?;
        }
        if bytes.len() > WRITE_BUFFER {
            self.file
                .write_all(bytes)
                .with_context(|| format!("failed to write {}", self.path.display()))?;
            self.flushed += bytes.len() as u64;
        } else {
            self.buffer.extend_from_slice(bytes);
        }
        Ok(())
    }

    /// Overwrite `bytes` at `offset`, which must lie within what was
    /// written so far.
    fn patch(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        let end = offset + bytes.len() as u64;
        ensure!(
            end <= self.position(),
            "{}: patch at {offset} is beyond the written data",
            self.path.display()
        );
        if offset >= self.flushed {
            let start = (offset - self.flushed) as usize;
            self.buffer[start..start + bytes.len()].copy_from_slice(bytes);
            return Ok(());
        }
        // A patched region is written in one call, so it never straddles
        // the flush boundary.
        ensure!(
            end <= self.flushed,
            "{}: patch at {offset} straddles the flush boundary",
            self.path.display()
        );
        self.file
            .seek(SeekFrom::Start(offset))
            .and_then(|_| self.file.write_all(bytes))
            .and_then(|()| self.file.seek(SeekFrom::Start(self.flushed)))
            .with_context(|| format!("failed to patch {}", self.path.display()))?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.file
            .write_all(&self.buffer)
            .with_context(|| format!("failed to write {}", self.path.display()))?;
        self.flushed += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        self.flush()?;
        self.file
            .sync_all()
            .with_context(|| format!("failed to sync {}", self.path.display()))
    }
}

/// Flush a buffered artifact writer and fsync its file.
fn finish_file(writer: BufWriter<File>, path: &Path) -> Result<()> {
    let file = writer
        .into_inner()
        .map_err(|err| err.into_error())
        .with_context(|| format!("failed to flush {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync {}", path.display()))
}

/// fsync a directory so its entries are durable.
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .with_context(|| format!("failed to sync directory {}", path.display()))
}

/// Append the lowercase hex encoding of `bytes`.
fn push_hex(out: &mut Vec<u8>, bytes: &[u8]) {
    out.reserve(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX_DIGITS[usize::from(byte >> 4)]);
        out.push(HEX_DIGITS[usize::from(byte & 0x0f)]);
    }
}

/// Decode lowercase hex into `out`, replacing its contents.
fn decode_hex(text: &[u8], out: &mut Vec<u8>) -> Result<()> {
    ensure!(
        text.len().is_multiple_of(2),
        "odd-length hex record {:?}",
        String::from_utf8_lossy(text)
    );
    out.clear();
    out.reserve(text.len() / 2);
    for pair in text.chunks_exact(2) {
        let nibble = |digit: u8| match digit {
            b'0'..=b'9' => Ok(digit - b'0'),
            b'a'..=b'f' => Ok(digit - b'a' + 10),
            _ => bail!("invalid hex digit {:?}", digit as char),
        };
        out.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips_and_preserves_byte_order() {
        let mut encoded = Vec::new();
        push_hex(&mut encoded, "10.1234/é\t".as_bytes());
        assert_eq!(encoded, b"31302e313233342fc3a909");
        let mut decoded = Vec::new();
        decode_hex(&encoded, &mut decoded).unwrap();
        assert_eq!(decoded, "10.1234/é\t".as_bytes());

        let mut a = Vec::new();
        let mut b = Vec::new();
        push_hex(&mut a, b"ab");
        push_hex(&mut b, b"abc");
        a.push(b'\t');
        b.push(b'\t');
        assert!(a < b);
        assert!(decode_hex(b"abc", &mut decoded).is_err());
        assert!(decode_hex(b"zz", &mut decoded).is_err());
    }

    #[test]
    fn posting_writer_patches_pending_and_flushed_bytes() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("targets.bin");
        let mut writer = PostingWriter::create(&path)?;
        let first = writer.position();
        writer.write(&0u64.to_le_bytes())?;
        writer.write(b"x")?;
        writer.flush()?;
        let second = writer.position();
        writer.write(&0u64.to_le_bytes())?;
        writer.write(b"yy")?;
        writer.patch(first, &1u64.to_le_bytes())?;
        writer.patch(second, &2u64.to_le_bytes())?;
        assert!(writer.patch(second + 20, &[0]).is_err());
        writer.finish()?;
        let mut expected = 1u64.to_le_bytes().to_vec();
        expected.extend_from_slice(b"x");
        expected.extend_from_slice(&2u64.to_le_bytes());
        expected.extend_from_slice(b"yy");
        assert_eq!(fs::read(&path)?, expected);
        Ok(())
    }
}
