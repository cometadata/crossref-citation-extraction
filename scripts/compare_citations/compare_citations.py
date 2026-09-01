# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "datasets",
#     "requests",
#     "tqdm",
# ]
# ///
"""
Verify arXiv citation coverage in Crossref, OpenAlex, DataCite, and OpenCitations.

Samples citation pairs from the HuggingFace dataset and verifies whether
each citation exists in the reference metadata of external APIs.
"""

import argparse
import csv
import json
import logging
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from urllib.parse import quote

import requests
from datasets import load_dataset
from tqdm import tqdm

HUGGINGFACE_DATASET = "cometadata/crossref-arxiv-citations"
CROSSREF_API = "https://api.crossref.org/works"
OPENALEX_API = "https://api.openalex.org/works"
DATACITE_EVENTS_API = "https://api.datacite.org/events"
OPENCITATIONS_API = "https://api.opencitations.net/index/v2"

DEFAULT_CROSSREF_RPS_WITH_TOKEN = 50.0
DEFAULT_CROSSREF_RPS_WITHOUT_TOKEN = 5.0
DEFAULT_OPENALEX_RPS = 10.0
DEFAULT_DATACITE_RPS = 5.0
DEFAULT_OPENCITATIONS_RPS = 5.0

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class CitationSample:
    citing_doi: str
    arxiv_id: str
    arxiv_doi: str
    source: str
    raw_match: str
    has_structured_doi_in_source: bool
    doi_asserted_by: str | None = None


@dataclass
class VerificationResult:
    citing_doi: str
    arxiv_id: str
    source: str
    doi_asserted_by: str | None = None

    crossref_found: bool = False
    crossref_has_references: bool = False
    crossref_structured_doi_match: bool = False
    crossref_unstructured_match: bool = False
    crossref_reference_key: str | None = None
    crossref_error: str | None = None

    openalex_citing_found: bool = False
    openalex_arxiv_in_refs: bool = False
    openalex_arxiv_work_id: str | None = None
    openalex_error: str | None = None

    datacite_event_found: bool = False
    datacite_total_events: int | None = None
    datacite_error: str | None = None

    opencitations_citing_found: bool = False
    opencitations_arxiv_in_refs: bool = False
    opencitations_oci: str | None = None
    opencitations_error: str | None = None

    notes: str = ""


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify arXiv citation coverage in Crossref, OpenAlex, DataCite, and OpenCitations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run %(prog)s --sample 100 --mailto user@example.com
  uv run %(prog)s --sample 100 --mailto user@example.com --crossref-token $CROSSREF_API_TOKEN
  uv run %(prog)s --sample 100 --mailto user@example.com --skip-datacite --skip-opencitations

Environment variables:
  CROSSREF_API_TOKEN    Crossref Plus API token (used if --crossref-token not provided)
"""
    )

    input_group = parser.add_argument_group("Input")
    input_group.add_argument(
        "--dataset", type=str, default=HUGGINGFACE_DATASET,
        help=f"HuggingFace dataset name (default: {HUGGINGFACE_DATASET})"
    )
    input_group.add_argument(
        "--split", type=str, default="train",
        help="Dataset split to use (default: train)"
    )

    sample_group = parser.add_argument_group("Sampling")
    sample_group.add_argument(
        "--sample", type=int, required=True,
        help="Number of citation pairs to sample"
    )
    sample_group.add_argument(
        "--ratio", type=str, default=None,
        help="Ratio of asserted:mined samples (e.g., '50:50', '70:30'). "
             "If not specified, samples naturally from the dataset."
    )
    sample_group.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducible sampling (default: 42)"
    )

    api_group = parser.add_argument_group("API configuration")
    api_group.add_argument(
        "--mailto", required=True,
        help="Email for API polite pool access"
    )
    api_group.add_argument(
        "--crossref-token",
        help="Crossref Plus API token (default: CROSSREF_API_TOKEN env var)"
    )
    api_group.add_argument(
        "--crossref-rps", type=float,
        help=f"Crossref requests/sec (default: {DEFAULT_CROSSREF_RPS_WITH_TOKEN} with token, {DEFAULT_CROSSREF_RPS_WITHOUT_TOKEN} without)"
    )
    api_group.add_argument(
        "--openalex-rps", type=float, default=DEFAULT_OPENALEX_RPS,
        help=f"OpenAlex requests/sec (default: {DEFAULT_OPENALEX_RPS})"
    )
    api_group.add_argument(
        "--datacite-rps", type=float, default=DEFAULT_DATACITE_RPS,
        help=f"DataCite requests/sec (default: {DEFAULT_DATACITE_RPS})"
    )
    api_group.add_argument(
        "--opencitations-rps", type=float, default=DEFAULT_OPENCITATIONS_RPS,
        help=f"OpenCitations requests/sec (default: {DEFAULT_OPENCITATIONS_RPS})"
    )

    verify_group = parser.add_argument_group("Verification options")
    verify_group.add_argument(
        "--skip-openalex", action="store_true",
        help="Skip OpenAlex verification"
    )
    verify_group.add_argument(
        "--skip-datacite", action="store_true",
        help="Skip DataCite verification"
    )
    verify_group.add_argument(
        "--skip-opencitations", action="store_true",
        help="Skip OpenCitations verification"
    )

    output_group = parser.add_argument_group("Output")
    output_group.add_argument(
        "--output-csv", type=str, required=True,
        help="Path for CSV output file"
    )
    output_group.add_argument(
        "--output-json", type=str, required=True,
        help="Path for JSON summary file"
    )

    return parser.parse_args()


def get_crossref_token(args) -> str | None:
    """Get Crossref Plus token from args or environment."""
    if args.crossref_token:
        return args.crossref_token
    return os.environ.get("CROSSREF_API_TOKEN")


def parse_ratio(ratio_str: str | None) -> tuple[int, int] | None:
    """Parse ratio string like '50:50' into (asserted, mined) counts."""
    if not ratio_str:
        return None
    parts = ratio_str.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid ratio format '{ratio_str}', expected 'X:Y'")
    try:
        asserted = int(parts[0])
        mined = int(parts[1])
        return (asserted, mined)
    except ValueError:
        raise ValueError(f"Invalid ratio format '{ratio_str}', expected integers")


class RateLimiter:
    """Enforces minimum interval between requests."""

    def __init__(self, requests_per_second: float):
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0

    def wait(self) -> None:
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()


class APIClient:
    """Base API client with rate limiting and retries."""

    def __init__(self, mailto: str, rps: float = 1.0, headers: dict | None = None):
        self.mailto = mailto
        self.rate_limiter = RateLimiter(rps)

        self.headers = {"User-Agent": f"ArXivCitationVerifier/1.0 (mailto:{mailto})"}
        if headers:
            self.headers.update(headers)

    def get(self, url: str, params: dict | None = None) -> dict | None:
        self.rate_limiter.wait()

        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(
                    url, params=params, headers=self.headers, timeout=REQUEST_TIMEOUT
                )
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 404:
                    return None
                elif resp.status_code == 429:
                    header = resp.headers.get("Retry-After")
                    try:
                        retry_after = int(header) if header else 2 ** (attempt + 1)
                    except ValueError:  # HTTP-date form
                        retry_after = 2 ** (attempt + 1)
                    time.sleep(retry_after)
                    continue
                elif resp.status_code >= 500:
                    time.sleep(2 ** (attempt + 1))
                    continue
                else:
                    return None
            except requests.Timeout:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
                raise
            except requests.RequestException:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None

        return None


class CrossrefClient(APIClient):

    def __init__(self, mailto: str, token: str | None = None, rps: float | None = None):
        if rps is None:
            rps = DEFAULT_CROSSREF_RPS_WITH_TOKEN if token else DEFAULT_CROSSREF_RPS_WITHOUT_TOKEN

        headers = {}
        if token:
            headers["Crossref-Plus-API-Token"] = f"Bearer {token}"

        super().__init__(mailto, rps, headers)
        self.has_token = token is not None


def _sample_from_config(
    dataset_name: str,
    config: str,
    split: str,
    target_size: int,
    source_label: str,
    seed: int
) -> tuple[list[CitationSample], int]:
    """Sample citation pairs from a specific dataset config using reservoir sampling."""
    random.seed(seed)
    dataset = load_dataset(dataset_name, config, split=split, streaming=True)

    samples: list[CitationSample] = []
    seen = 0

    for row in tqdm(dataset, desc=f"Sampling {source_label}", unit="records"):
        arxiv_doi = row["arxiv_doi"]
        arxiv_id = row["arxiv_id"]
        cited_by = row.get("cited_by", [])

        for citation in cited_by:
            citing_doi = citation.get("doi")
            if not citing_doi:
                continue

            matches = citation.get("matches", [])
            if not matches:
                continue

            first_match = matches[0]
            raw_match = first_match.get("raw_match", "")
            ref = first_match.get("reference", {})
            has_structured_doi = "DOI" in ref
            doi_asserted_by = ref.get("doi-asserted-by")

            seen += 1

            sample = CitationSample(
                citing_doi=citing_doi.lower().replace("https://doi.org/", "").replace("http://doi.org/", ""),
                arxiv_id=arxiv_id,
                arxiv_doi=arxiv_doi,
                source=source_label,
                raw_match=raw_match,
                has_structured_doi_in_source=has_structured_doi,
                doi_asserted_by=doi_asserted_by
            )

            if len(samples) < target_size:
                samples.append(sample)
            else:
                j = random.randint(0, seen - 1)
                if j < target_size:
                    samples[j] = sample

        # Intentional speed/randomness trade-off: stop after seeing 10x the
        # target, so samples are drawn from the head of the (shuffled-on-
        # publish) dataset rather than a full pass over every record.
        if seen >= target_size * 10:
            break

    return samples, seen


def load_sample_citation_pairs(
    dataset_name: str,
    split: str,
    sample_size: int,
    seed: int = 42,
    ratio: tuple[int, int] | None = None
) -> list[CitationSample]:
    """Stream dataset and return reservoir-sampled citation pairs.

    If ratio is provided as (asserted_pct, mined_pct), loads from separate
    'asserted' and 'mined' configs. Otherwise loads from 'all' config.
    """
    if ratio:
        # Calculate target sizes for each category
        total_ratio = ratio[0] + ratio[1]
        asserted_target = int(sample_size * ratio[0] / total_ratio)
        mined_target = sample_size - asserted_target
        logger.info(f"Target ratio: {asserted_target} asserted, {mined_target} mined")

        logger.info("Sampling from 'asserted' config...")
        asserted_samples, seen_asserted = _sample_from_config(
            dataset_name, "asserted", split, asserted_target, "asserted", seed
        )

        logger.info("Sampling from 'mined' config...")
        mined_samples, seen_mined = _sample_from_config(
            dataset_name, "mined", split, mined_target, "mined", seed + 1
        )

        samples = asserted_samples + mined_samples
        random.seed(seed)
        random.shuffle(samples)

        logger.info(f"Sampled {len(asserted_samples)} asserted (from {seen_asserted:,}) + "
                   f"{len(mined_samples)} mined (from {seen_mined:,})")
    else:
        logger.info(f"Streaming citation pairs from {dataset_name} (all config)")
        samples, seen = _sample_from_config(
            dataset_name, "all", split, sample_size, "all", seed
        )
        logger.info(f"Sampled {len(samples)} citation pairs from {seen:,} seen")

    return samples


class CitationVerifier:

    ARXIV_PATTERN = re.compile(
        r'(?:arXiv[:\s./]*)?(\d{4}\.\d{4,5}(?:v\d+)?)',
        re.IGNORECASE
    )

    def __init__(
        self,
        mailto: str,
        crossref_token: str | None = None,
        crossref_rps: float | None = None,
        openalex_rps: float = DEFAULT_OPENALEX_RPS,
        datacite_rps: float = DEFAULT_DATACITE_RPS,
        opencitations_rps: float = DEFAULT_OPENCITATIONS_RPS,
        skip_openalex: bool = False,
        skip_datacite: bool = False,
        skip_opencitations: bool = False,
    ):
        self.crossref = CrossrefClient(mailto, crossref_token, crossref_rps)
        self.skip_openalex = skip_openalex
        self.skip_datacite = skip_datacite
        self.skip_opencitations = skip_opencitations

        if not skip_openalex:
            self.openalex = APIClient(mailto, openalex_rps)
        if not skip_datacite:
            self.datacite = APIClient(mailto, datacite_rps)
        if not skip_opencitations:
            self.opencitations = APIClient(mailto, opencitations_rps)

        self.mailto = mailto

    def verify_crossref(self, sample: CitationSample, result: VerificationResult):
        """Check if arXiv paper appears in Crossref references."""
        try:
            url = f"{CROSSREF_API}/{quote(sample.citing_doi, safe='')}"
            data = self.crossref.get(url)

            if not data:
                result.crossref_error = "not_found"
                return

            result.crossref_found = True
            work = data.get("message", {})
            references = work.get("reference", [])

            if not references:
                result.crossref_has_references = False
                return

            result.crossref_has_references = True
            arxiv_id = sample.arxiv_id

            for ref in references:
                ref_doi = ref.get("DOI", "").lower()
                unstructured = ref.get("unstructured", "")

                if ref_doi and ("arxiv" in ref_doi and arxiv_id in ref_doi):
                    result.crossref_structured_doi_match = True
                    result.crossref_reference_key = ref.get("key")
                    return

                if unstructured:
                    for match in self.ARXIV_PATTERN.findall(unstructured):
                        if match.split("v")[0] == arxiv_id.split("v")[0]:
                            result.crossref_unstructured_match = True
                            result.crossref_reference_key = ref.get("key")
                            return

                for field in ["article-title", "journal-title", "volume-title"]:
                    text = ref.get(field, "")
                    if arxiv_id in text:
                        result.crossref_unstructured_match = True
                        result.crossref_reference_key = ref.get("key")

        except Exception as e:  # noqa: BLE001 - record any API failure on the result
            result.crossref_error = str(e)

    def verify_openalex(self, sample: CitationSample, result: VerificationResult):
        """Check if arXiv paper appears in OpenAlex referenced_works."""
        if self.skip_openalex:
            result.openalex_error = "skipped"
            return

        try:
            arxiv_url = f"{OPENALEX_API}/doi:{quote(sample.arxiv_doi, safe='')}"
            arxiv_data = self.openalex.get(arxiv_url, params={"mailto": self.mailto, "include_xpac": "true"})

            if arxiv_data:
                result.openalex_arxiv_work_id = arxiv_data.get("id")

            citing_url = f"{OPENALEX_API}/doi:{quote(sample.citing_doi, safe='')}"
            citing_data = self.openalex.get(citing_url, params={"mailto": self.mailto, "include_xpac": "true"})

            if not citing_data:
                result.openalex_error = "citing_not_found"
                return

            result.openalex_citing_found = True
            referenced_works = citing_data.get("referenced_works", [])

            if (
                result.openalex_arxiv_work_id
                and result.openalex_arxiv_work_id in referenced_works
            ):
                result.openalex_arxiv_in_refs = True

        except Exception as e:  # noqa: BLE001 - record any API failure on the result
            result.openalex_error = str(e)

    def verify_datacite(self, sample: CitationSample, result: VerificationResult):
        """Check DataCite Event Data for citation relationship."""
        if self.skip_datacite:
            result.datacite_error = "skipped"
            return

        try:
            params = {
                "doi": sample.arxiv_doi,
                "page[size]": 1000,
            }
            data = self.datacite.get(DATACITE_EVENTS_API, params=params)

            if not data:
                result.datacite_error = "no_response"
                return

            result.datacite_total_events = data.get("meta", {}).get("total", 0)

            citing_doi_url = f"https://doi.org/{sample.citing_doi}".lower()
            for event in data.get("data", []):
                subj_id = event.get("attributes", {}).get("subj-id", "").lower()
                if subj_id == citing_doi_url:
                    result.datacite_event_found = True
                    return

        except Exception as e:  # noqa: BLE001 - record any API failure on the result
            result.datacite_error = str(e)

    def verify_opencitations(self, sample: CitationSample, result: VerificationResult):
        """Check if arXiv paper appears in OpenCitations references."""
        if self.skip_opencitations:
            result.opencitations_error = "skipped"
            return

        try:
            citing_id = f"doi:{sample.citing_doi}"
            url = f"{OPENCITATIONS_API}/references/{quote(citing_id, safe='')}"
            data = self.opencitations.get(url)

            if not data:
                result.opencitations_error = "no_references"
                return

            result.opencitations_citing_found = True
            arxiv_doi_lower = sample.arxiv_doi.lower()

            for ref in data:
                cited = ref.get("cited", "").lower()
                if arxiv_doi_lower in cited:
                    result.opencitations_arxiv_in_refs = True
                    result.opencitations_oci = ref.get("oci")
                    return

        except Exception as e:  # noqa: BLE001 - record any API failure on the result
            result.opencitations_error = str(e)


def _run_api_pass(fn, samples, results, desc, position):
    """Run one API's verification over all samples (one thread per API).

    Each thread writes disjoint attributes on shared VerificationResult
    objects, so no locking is needed.
    """
    for sample, result in tqdm(
        list(zip(samples, results)),
        desc=desc, unit="citation", position=position, leave=False,
    ):
        fn(sample, result)


def compute_summary(results: list[VerificationResult]) -> dict:
    """Compute summary statistics from verification results."""
    asserted = [r for r in results if r.source == "asserted"]
    mined = [r for r in results if r.source == "mined"]

    asserted_publisher = [r for r in asserted if r.doi_asserted_by == "publisher"]
    asserted_crossref = [r for r in asserted if r.doi_asserted_by == "crossref"]

    def compute_stats(subset: list[VerificationResult]) -> dict:
        total = len(subset)
        if total == 0:
            return {"count": 0}

        crossref_found = sum(1 for r in subset if r.crossref_found)
        has_refs = sum(1 for r in subset if r.crossref_has_references)
        structured_match = sum(1 for r in subset if r.crossref_structured_doi_match)
        unstructured_only = sum(1 for r in subset if r.crossref_unstructured_match and not r.crossref_structured_doi_match)
        no_match = sum(1 for r in subset if r.crossref_has_references and not r.crossref_structured_doi_match and not r.crossref_unstructured_match)

        openalex_found = sum(1 for r in subset if r.openalex_citing_found)
        openalex_ref_match = sum(1 for r in subset if r.openalex_arxiv_in_refs)

        datacite_event_match = sum(1 for r in subset if r.datacite_event_found)

        opencitations_found = sum(1 for r in subset if r.opencitations_citing_found)
        opencitations_ref_match = sum(1 for r in subset if r.opencitations_arxiv_in_refs)

        return {
            "count": total,
            "crossref_citing_found": crossref_found,
            "crossref_has_references": has_refs,
            "crossref_structured_doi_match": structured_match,
            "crossref_structured_match_rate": structured_match / total if total else 0,
            "crossref_unstructured_only": unstructured_only,
            "crossref_unstructured_only_rate": unstructured_only / total if total else 0,
            "crossref_no_match": no_match,
            "crossref_no_match_rate": no_match / total if total else 0,
            "openalex_citing_found": openalex_found,
            "openalex_arxiv_in_refs": openalex_ref_match,
            "openalex_match_rate": openalex_ref_match / total if total else 0,
            "datacite_event_found": datacite_event_match,
            "datacite_event_match_rate": datacite_event_match / total if total else 0,
            "opencitations_citing_found": opencitations_found,
            "opencitations_arxiv_in_refs": opencitations_ref_match,
            "opencitations_match_rate": opencitations_ref_match / total if total else 0,
        }

    return {
        "sample_size": {
            "asserted": len(asserted),
            "asserted_publisher": len(asserted_publisher),
            "asserted_crossref": len(asserted_crossref),
            "mined": len(mined),
            "total": len(results)
        },
        "asserted_results": compute_stats(asserted),
        "asserted_publisher_results": compute_stats(asserted_publisher),
        "asserted_crossref_results": compute_stats(asserted_crossref),
        "mined_results": compute_stats(mined),
    }


def write_csv_output(results: list[VerificationResult], output_path: str) -> None:
    fieldnames = [
        "citing_doi", "arxiv_id", "source", "doi_asserted_by",
        "crossref_found", "crossref_has_references",
        "crossref_structured_doi_match", "crossref_unstructured_match",
        "crossref_reference_key", "crossref_error",
        "openalex_citing_found", "openalex_arxiv_in_refs",
        "openalex_arxiv_work_id", "openalex_error",
        "datacite_event_found", "datacite_total_events", "datacite_error",
        "opencitations_citing_found", "opencitations_arxiv_in_refs",
        "opencitations_oci", "opencitations_error",
        "notes"
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(asdict(result))

    logger.info(f"CSV written to {output_path}")


def write_json_output(summary: dict, output_path: str) -> None:
    with open(output_path, "w") as f:
        json.dump(summary, f, indent=2)

    logger.info(f"JSON summary written to {output_path}")


def print_summary(summary: dict, args: argparse.Namespace) -> None:
    print("\n" + "=" * 60)
    print("VERIFICATION RESULTS")
    print("=" * 60)

    if summary["asserted_results"].get("count", 0) > 0:
        print("\nAsserted citations (structured DOI in reference):")
        print(f"  - Samples verified: {summary['asserted_results']['count']}")
        pub_count = summary['sample_size'].get('asserted_publisher', 0)
        cr_count = summary['sample_size'].get('asserted_crossref', 0)
        print(f"    - DOI asserted by publisher: {pub_count}")
        print(f"    - DOI asserted by Crossref: {cr_count}")
        print(f"  - Found in Crossref structured refs: {summary['asserted_results']['crossref_structured_doi_match']} ({summary['asserted_results']['crossref_structured_match_rate']*100:.1f}%)")
        print(f"  - Found in unstructured only: {summary['asserted_results']['crossref_unstructured_only']} ({summary['asserted_results']['crossref_unstructured_only_rate']*100:.1f}%)")
        print(f"  - Not found: {summary['asserted_results']['crossref_no_match']} ({summary['asserted_results']['crossref_no_match_rate']*100:.1f}%)")

    if summary["mined_results"].get("count", 0) > 0:
        print("\nMined citations (from unstructured text):")
        print(f"  - Samples verified: {summary['mined_results']['count']}")
        print(f"  - Found in Crossref structured refs: {summary['mined_results']['crossref_structured_doi_match']} ({summary['mined_results']['crossref_structured_match_rate']*100:.1f}%)")
        print(f"  - Found in unstructured only: {summary['mined_results']['crossref_unstructured_only']} ({summary['mined_results']['crossref_unstructured_only_rate']*100:.1f}%)")
        print(f"  - Not found: {summary['mined_results']['crossref_no_match']} ({summary['mined_results']['crossref_no_match_rate']*100:.1f}%)")

    if not args.skip_openalex:
        print("\nOpenAlex comparison:")
        if summary["asserted_results"].get("count", 0) > 0:
            print(f"  - Asserted match rate: {summary['asserted_results']['openalex_match_rate']*100:.1f}%")
            if summary["asserted_publisher_results"].get("count", 0) > 0:
                print(f"    - Publisher-asserted: {summary['asserted_publisher_results']['openalex_match_rate']*100:.1f}%")
            if summary["asserted_crossref_results"].get("count", 0) > 0:
                print(f"    - Crossref-asserted: {summary['asserted_crossref_results']['openalex_match_rate']*100:.1f}%")
        if summary["mined_results"].get("count", 0) > 0:
            print(f"  - Mined match rate: {summary['mined_results']['openalex_match_rate']*100:.1f}%")

    if not args.skip_datacite:
        print("\nDataCite Event Data comparison:")
        if summary["asserted_results"].get("count", 0) > 0:
            print(f"  - Asserted match rate: {summary['asserted_results']['datacite_event_match_rate']*100:.1f}%")
            if summary["asserted_publisher_results"].get("count", 0) > 0:
                print(f"    - Publisher-asserted: {summary['asserted_publisher_results']['datacite_event_match_rate']*100:.1f}%")
            if summary["asserted_crossref_results"].get("count", 0) > 0:
                print(f"    - Crossref-asserted: {summary['asserted_crossref_results']['datacite_event_match_rate']*100:.1f}%")
        if summary["mined_results"].get("count", 0) > 0:
            print(f"  - Mined match rate: {summary['mined_results']['datacite_event_match_rate']*100:.1f}%")

    if not args.skip_opencitations:
        print("\nOpenCitations comparison:")
        if summary["asserted_results"].get("count", 0) > 0:
            print(f"  - Asserted match rate: {summary['asserted_results']['opencitations_match_rate']*100:.1f}%")
            if summary["asserted_publisher_results"].get("count", 0) > 0:
                print(f"    - Publisher-asserted: {summary['asserted_publisher_results']['opencitations_match_rate']*100:.1f}%")
            if summary["asserted_crossref_results"].get("count", 0) > 0:
                print(f"    - Crossref-asserted: {summary['asserted_crossref_results']['opencitations_match_rate']*100:.1f}%")
        if summary["mined_results"].get("count", 0) > 0:
            print(f"  - Mined match rate: {summary['mined_results']['opencitations_match_rate']*100:.1f}%")


def main() -> int:
    args = parse_arguments()
    crossref_token = get_crossref_token(args)

    ratio = parse_ratio(args.ratio)

    print("Configuration:")
    print(f"  Dataset: {args.dataset}")
    print(f"  Sample size: {args.sample}")
    if ratio:
        print(f"  Ratio: {ratio[0]}:{ratio[1]} (asserted:mined)")
    else:
        print("  Ratio: natural (no stratification)")
    print(f"  Seed: {args.seed}")
    print(f"  mailto: {args.mailto}")
    if crossref_token:
        print(f"  Crossref: Plus API token detected (rate: {args.crossref_rps or DEFAULT_CROSSREF_RPS_WITH_TOKEN} RPS)")
    else:
        print(f"  Crossref: Anonymous/polite pool (rate: {args.crossref_rps or DEFAULT_CROSSREF_RPS_WITHOUT_TOKEN} RPS)")
    if args.skip_openalex:
        print("  OpenAlex: SKIPPED")
    else:
        print(f"  OpenAlex: {args.openalex_rps} RPS")
    if args.skip_datacite:
        print("  DataCite: SKIPPED")
    else:
        print(f"  DataCite: {args.datacite_rps} RPS")
    if args.skip_opencitations:
        print("  OpenCitations: SKIPPED")
    else:
        print(f"  OpenCitations: {args.opencitations_rps} RPS")
    print()

    samples = load_sample_citation_pairs(args.dataset, args.split, args.sample, args.seed, ratio)

    if not samples:
        logger.error("No samples collected")
        return 1

    asserted_count = sum(1 for s in samples if s.source == "asserted")
    mined_count = sum(1 for s in samples if s.source == "mined")
    print(f"\nSampled {len(samples)} citations: {asserted_count} asserted, {mined_count} mined")

    print(f"\nVerifying {len(samples)} citations against APIs...")

    verifier = CitationVerifier(
        mailto=args.mailto,
        crossref_token=crossref_token,
        crossref_rps=args.crossref_rps,
        openalex_rps=args.openalex_rps,
        datacite_rps=args.datacite_rps,
        opencitations_rps=args.opencitations_rps,
        skip_openalex=args.skip_openalex,
        skip_datacite=args.skip_datacite,
        skip_opencitations=args.skip_opencitations,
    )

    results = [
        VerificationResult(
            citing_doi=s.citing_doi,
            arxiv_id=s.arxiv_id,
            source=s.source,
            doi_asserted_by=s.doi_asserted_by,
        )
        for s in samples
    ]

    passes = [
        (verifier.verify_crossref, "Crossref"),
        (verifier.verify_openalex, "OpenAlex"),
        (verifier.verify_datacite, "DataCite"),
        (verifier.verify_opencitations, "OpenCitations"),
    ]
    with ThreadPoolExecutor(max_workers=len(passes)) as executor:
        futures = [
            executor.submit(_run_api_pass, fn, samples, results, desc, i)
            for i, (fn, desc) in enumerate(passes)
        ]
        for future in futures:
            future.result()  # re-raise any worker exception

    print("\nWriting results...")

    write_csv_output(results, args.output_csv)

    summary = compute_summary(results)
    write_json_output(summary, args.output_json)

    print_summary(summary, args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
