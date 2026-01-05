use indicatif::{ProgressBar, ProgressStyle};

/// Create a spinner progress bar for indeterminate progress
pub fn create_spinner(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} [{elapsed_precise}] {msg}")
            .expect("Failed to create progress style")
    );
    pb.set_message(message.to_string());
    pb
}

/// Create a progress bar for file reading (bytes)
pub fn create_bytes_progress_bar(total_bytes: u64) -> ProgressBar {
    let pb = ProgressBar::new(total_bytes);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {msg}")
            .expect("Failed to create progress style")
            .progress_chars("#>-")
    );
    pb
}

/// Create a progress bar for item counting
pub fn create_count_progress_bar(total_items: u64) -> ProgressBar {
    let pb = ProgressBar::new(total_items);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
            .expect("Failed to create progress style")
            .progress_chars("#>-")
    );
    pb
}
