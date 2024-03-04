#![feature(lazy_cell)]

use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::Result;
use bincode::Decode;
use reqwest::{Body, Url};
use tokio::io::AsyncRead;

static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::new()
});

static SERVER_URL: LazyLock<Url> = LazyLock::new(|| {
    Url::from_str(&std::env::var("RANDRW_S3_SERVER").unwrap()).unwrap()
});

pub async fn put_object(
    key: &str,
    data_len: u64,
    reader: impl AsyncRead + Send + 'static,
) -> Result<()> {
    let path = SERVER_URL
        .join("putobject")?
        .join(key)?;

    CLIENT.post(path)
        .header("Content-Length", data_len)
        .body(Body::wrap_stream(tokio_util::io::ReaderStream::new(reader)))
        .send()
        .await?;

    Ok(())
}

pub async fn put_zero_object(
    key: &str,
    data_len: u64
) -> Result<()> {
    let path = SERVER_URL
        .join("putzeroobject")?
        .join(key)?
        .join(&data_len.to_string())?;

    println!("path: {}", path);

    let resp = CLIENT.post(path)
        .send()
        .await?;

    println!("code: {}", resp.status());

    if resp.status() != 200 {
        let err = resp.text().await?;
        eprintln!("{}", err);
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
    let path = SERVER_URL
        .join("updateobject")?
        .join(key)?
        .join(&offset.to_string())?;

    CLIENT.post(path)
        .header("Content-Length", data_len)
        .body(Body::wrap_stream(tokio_util::io::ReaderStream::new(reader)))
        .send()
        .await?;

    Ok(())
}

pub async fn get_object_with_ranges(
    key: &str,
    // (start position, length)
    ranges: &[(u64, u64)],
) -> Result<Vec<Part>> {
    let path = SERVER_URL
        .join("getobjectwithranges")?
        .join(key)?;

    let resp = CLIENT.get(path)
        .json(ranges)
        .send()
        .await?;

    let buff = resp.bytes().await?;
    let parts: Vec<Part> = bincode::decode_from_slice(buff.as_ref(), bincode::config::standard())?.0;
    Ok(parts)
}