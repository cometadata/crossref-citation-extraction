use log::debug;
use reqwest::Client;
use std::time::Duration;

/// Check if a DOI resolves via HTTP HEAD request
pub async fn check_doi_resolves(client: &Client, doi: &str, timeout: Duration) -> bool {
    let url = format!("https://doi.org/{}", doi);

    match client.head(&url).timeout(timeout).send().await {
        Ok(resp) => {
            let status = resp.status();
            status.is_redirection() || status.is_success()
        }
        Err(e) => {
            debug!("DOI resolution failed for {}: {}", doi, e);
            false
        }
    }
}

/// Create an HTTP client configured for DOI resolution
pub fn create_doi_client() -> reqwest::Result<Client> {
    Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
}
