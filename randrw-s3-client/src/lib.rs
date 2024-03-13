#![feature(lazy_cell)]

use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::{anyhow, Result};
use bincode::Decode;
use log::error;
use reqwest::{Body, };
use tokio::io::AsyncRead;

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::new()
});

static SERVER_URL: LazyLock<String> = LazyLock::new(|| {
    std::env::var("RANDRW_S3_SERVER").unwrap()
});

pub async fn object_exist(
    key: &str,
) -> Result<bool> {
    let path = format!("{}/objectexist", SERVER_URL.as_str());

    let resp = CLIENT.get(path)
        .query(&[("key", key)])
        .send()
        .await?;

    if !resp.status().is_success() {
        let err = resp.text().await?;
        return Err(anyhow!(err));
    }

    let res = resp.text().await?;
    Ok(bool::from_str(&res)?)
}

pub async fn put_object(
    key: &str,
    data_len: u64,
    reader: impl AsyncRead + Send + 'static,
) -> Result<()> {
    let path = format!("{}/putobject", SERVER_URL.as_str());

    let resp = CLIENT.post(path)
        .query(&[("key", key)])
        .header("Content-Length", data_len)
        .body(Body::wrap_stream(tokio_util::io::ReaderStream::new(reader)))
        .send()
        .await?;

    if !resp.status().is_success() {
        let err = resp.text().await?;
        return Err(anyhow!(err));
    }
    Ok(())
}

pub async fn put_zero_object(
    key: &str,
    data_len: u64
) -> Result<()> {
    let path = format!("{}/putzeroobject", SERVER_URL.as_str());

    let resp = CLIENT.post(path)
        .query(&[("key", key), ("data_len", data_len.to_string().as_str())])
        .send()
        .await?;

    if !resp.status().is_success() {
        let err = resp.text().await?;
        return Err(anyhow!(err));
    }
    Ok(())
}

#[derive(Decode, Clone)]
pub struct Part {
    pub offset: u64,
    pub data: Vec<u8>,
}

pub async fn update_object(
    key: &str,
    offset: u64,
    data_len: u64,
    reader: impl AsyncRead + Send + 'static,
) -> Result<()> {
    let path = format!("{}/updateobject", SERVER_URL.as_str());

    let resp = CLIENT.post(path)
        .query(&[("key", key), ("offset", offset.to_string().as_str())])
        .header("Content-Length", data_len)
        .body(Body::wrap_stream(tokio_util::io::ReaderStream::new(reader)))
        .send()
        .await?;

    if !resp.status().is_success() {
        let err = resp.text().await?;
        return Err(anyhow!(err));
    }
    Ok(())
}

pub async fn get_object_with_ranges(
    key: &str,
    // (start position, length)
    ranges: &[(u64, u64)],
) -> Result<Vec<Part>> {
    const RETRY: u8 = 2;
    let mut count = 0;

    loop {
        let fut = async {
            let path = format!("{}/getobjectwithranges", SERVER_URL.as_str());

            let resp = CLIENT.get(path)
                .query(&[("key", key)])
                .json(ranges)
                .send()
                .await?;

            if !resp.status().is_success() {
                let err = resp.text().await?;
                return Err(anyhow!(err));
            }

            let buff = resp.bytes().await?;
            let parts: Vec<Part> = bincode::decode_from_slice(buff.as_ref(), bincode::config::standard())?.0;
            Ok(parts)
        };

        match fut.await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if count < RETRY {
                    error!("get_object_with_ranges: {:?}; retry", e);
                } else {
                    return Err(e);
                }
            }
        }
        count += 1;
    }
}