use crate::{CodecError, ObjectStore};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use wfe_core::Timestamp;

#[derive(Clone, Debug)]
pub struct S3Config {
    /// `https://s3.us-east-1.amazonaws.com`, `http://minio:9000`, ...
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
}

impl S3Config {
    /// From `WFE_S3_ENDPOINT`, `WFE_S3_REGION`, `WFE_S3_BUCKET`, and
    /// `WFE_S3_ACCESS_KEY` / `WFE_S3_SECRET_KEY` (falling back to the
    /// usual `AWS_*` variables).
    pub fn from_env() -> Option<S3Config> {
        let var = |names: &[&str]| names.iter().find_map(|n| std::env::var(n).ok().filter(|v| !v.is_empty()));
        Some(S3Config {
            endpoint: var(&["WFE_S3_ENDPOINT", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"])?,
            region: var(&["WFE_S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"]).unwrap_or_else(|| "us-east-1".into()),
            bucket: var(&["WFE_S3_BUCKET"])?,
            access_key: var(&["WFE_S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"])?,
            secret_key: var(&["WFE_S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"])?,
        })
    }
}

pub struct S3Store {
    config: S3Config,
    agent: ureq::Agent,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC-SHA256 accepts keys of any length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn uri_encode(path: &str) -> String {
    let mut out = String::new();
    for b in path.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn amz_time(unix: u64) -> (String, String) {
    let t = Timestamp::from_micros(unix.saturating_mul(1_000_000)).civil();
    let date = format!("{:04}{:02}{:02}", t.year, t.month, t.day);
    let time = format!("{date}T{:02}{:02}{:02}Z", t.hour, t.minute, t.second);
    (date, time)
}

pub(crate) fn sign(
    config: &S3Config,
    method: &str,
    path: &str,
    host: &str,
    payload_hash: &str,
    unix: u64,
) -> (String, String) {
    let (date, time) = amz_time(unix);
    let canonical = format!(
        "{method}\n{path}\n\nhost:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{time}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload_hash}"
    );
    let scope = format!("{date}/{}/s3/aws4_request", config.region);
    let to_sign = format!("AWS4-HMAC-SHA256\n{time}\n{scope}\n{}", hex(&Sha256::digest(canonical.as_bytes())));
    let mut key = hmac(format!("AWS4{}", config.secret_key).as_bytes(), date.as_bytes());
    for part in [config.region.as_str(), "s3", "aws4_request"] {
        key = hmac(&key, part.as_bytes());
    }
    let signature = hex(&hmac(&key, to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}",
        config.access_key
    );
    (authorization, time)
}

impl S3Store {
    pub fn new(config: S3Config) -> S3Store {
        S3Store { config, agent: ureq::Agent::new_with_defaults() }
    }

    fn locate(&self, key: &str) -> (String, String, String) {
        let endpoint = self.config.endpoint.trim_end_matches('/');
        let host = endpoint.split("://").nth(1).unwrap_or(endpoint).to_string();
        let path = uri_encode(&format!("/{}/{}", self.config.bucket, key));
        (format!("{endpoint}{path}"), host, path)
    }

    fn now() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
    }
}

impl ObjectStore for S3Store {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), CodecError> {
        let (url, host, path) = self.locate(key);
        let hash = hex(&Sha256::digest(bytes));
        let (authorization, time) = sign(&self.config, "PUT", &path, &host, &hash, Self::now());
        self.agent
            .put(&url)
            .header("x-amz-content-sha256", &hash)
            .header("x-amz-date", &time)
            .header("authorization", &authorization)
            .send(bytes)
            .map_err(|e| CodecError::Store(format!("PUT {key}: {e}")))?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, CodecError> {
        let (url, host, path) = self.locate(key);
        let hash = hex(&Sha256::digest(b""));
        let (authorization, time) = sign(&self.config, "GET", &path, &host, &hash, Self::now());
        let mut response = self
            .agent
            .get(&url)
            .header("x-amz-content-sha256", &hash)
            .header("x-amz-date", &time)
            .header("authorization", &authorization)
            .call()
            .map_err(|e| CodecError::Store(format!("GET {key}: {e}")))?;
        response
            .body_mut()
            .with_config()
            .limit(1 << 30)
            .read_to_vec()
            .map_err(|e| CodecError::Store(format!("GET {key}: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_the_documented_signing_key() {
        // https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv4-calculate.html
        let mut key = hmac(b"AWS4wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", b"20120215");
        for part in ["us-east-1", "iam", "aws4_request"] {
            key = hmac(&key, part.as_bytes());
        }
        assert_eq!(hex(&key), "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d");
    }

    #[test]
    fn formats_time() {
        assert_eq!(amz_time(1_369_353_600), ("20130524".to_string(), "20130524T000000Z".to_string()));
        assert_eq!(amz_time(951_782_399).1, "20000228T235959Z");
        assert_eq!(amz_time(1_709_251_199).1, "20240229T235959Z");
    }
}
