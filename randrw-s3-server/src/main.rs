use std::cmp::min;
use std::collections::HashMap;
use std::io;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, ensure, Result};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_sdk_s3::primitives::ByteStream;
use aws_types::region::Region;
use aws_types::SdkConfig;
use bincode::Encode;
use clap::Parser;
use futures_util::TryStreamExt;
use log::{info, LevelFilter};
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use mimalloc::MiMalloc;
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio_util::bytes::Buf;
use tokio_util::io::StreamReader;
use warp::{Filter, Stream};
use warp::http::StatusCode;
use warp::hyper::Body;
use warp::reply::Response;

use crate::datastore::Datastore;

mod datastore;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

struct Context {
    part_size: u64,
    datastore: std::sync::RwLock<Datastore>,
    tasks: std::sync::Mutex<HashMap<String, Arc<tokio::sync::RwLock<()>>>>,
    s3client: Client,
    s3config: S3Config,
}

async fn put_object(
    ctx: &Context,
    key: String,
    content_len: u64,
    body: impl Stream<Item=Result<impl Buf, warp::Error>> + Unpin,
) -> Result<()> {
    let s3config = &ctx.s3config;
    let part_size = ctx.part_size;
    let mut body_len = content_len;
    let mut parts_len = content_len / part_size;

    if content_len % part_size != 0 {
        parts_len += 1;
    }

    let lock = ctx.tasks.lock()
        .unwrap()
        .entry(key.clone())
        .or_insert(Arc::new(tokio::sync::RwLock::new(())))
        .clone();

    let _guard = lock.write().await;

    let mut reader = StreamReader::new(body.map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
    let mut buff = vec![0u8; part_size as usize];
    let mut parts_num = 0;

    while body_len > 0 {
        let read_len = min(part_size, body_len);
        reader.read_exact(&mut buff[..read_len as usize]).await?;

        ctx.s3client.put_object()
            .bucket(&s3config.bucket)
            .key(format!("{}/{}", key, parts_num))
            .body(ByteStream::from(buff.clone()))
            .send()
            .await?;

        body_len -= read_len;
        parts_num += 1;
    }

    let obj = datastore::Object {
        key,
        total_size: content_len,
        part_size,
        parts_len,
    };

    ctx.datastore.write().unwrap().put(obj)?;
    Ok(())
}

async fn put_zero_object_api(
    key: KeyQuery,
    data_len_query: DataLenQuery,
    ctx: Arc<Context>,
) -> Response {
    let stream = futures_util::stream::repeat_with(|| Ok([0u8].as_slice()));

    let res = put_object(
        &ctx,
        key.key,
        data_len_query.data_len,
        stream
    ).await;

    match res {
        Ok(_) => Response::new(Body::empty()),
        Err(e) => {
            let mut resp = Response::new(Body::from(format!("{:?}", e)));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp
        }
    }
}

async fn put_object_api(
    key: KeyQuery,
    content_len: u64,
    body: impl Stream<Item=Result<impl Buf, warp::Error>> + Unpin,
    ctx: Arc<Context>,
) -> Response {
    let res = put_object(
        &ctx,
        key.key,
        content_len,
        body
    ).await;

    match res {
        Ok(_) => Response::new(Body::empty()),
        Err(e) => {
            let mut resp = Response::new(Body::from(e.to_string()));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp
        }
    }
}

async fn update_object(
    ctx: &Context,
    key: String,
    offset: u64,
    mut content_len: u64,
    body: impl Stream<Item=Result<impl Buf, warp::Error>> + Unpin,
) -> Result<()> {
    let s3config = &ctx.s3config;
    let s3client = &ctx.s3client;

    let lock = ctx.tasks.lock()
        .unwrap()
        .entry(key.clone())
        .or_insert(Arc::new(tokio::sync::RwLock::new(())))
        .clone();

    let _guard = lock.read().await;
    let obj = ctx.datastore.read().unwrap().get(&key).ok_or_else(|| anyhow!("{} not found", key))?;
    let part_size = obj.part_size;

    ensure!(offset + content_len <= obj.total_size);
    let mut reader = StreamReader::new(body.map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
    let mut buff = vec![0u8; part_size as usize];

    let mut parts_num = offset / part_size;

    // |..{....|......|
    if offset % part_size != 0 {
        let offset_in_part = (offset % part_size) as usize;
        let range = format!("bytes={}-{}", 0, offset_in_part - 1);

        let out = s3client.get_object()
            .bucket(&s3config.bucket)
            .key(format!("{}/{}", key, parts_num))
            .range(range)
            .send()
            .await?;

        let mut s3reader = out.body.into_async_read();
        s3reader.read_exact(&mut buff[..offset_in_part]).await?;

        reader.read_exact(&mut buff[offset_in_part..min(part_size as usize, offset_in_part + content_len as usize)]).await?;
    } else {
        reader.read_exact(&mut buff[..min(part_size as usize, content_len as usize)]).await?;
    }

    // |..{...}.|......|
    if (offset + content_len) / part_size == offset / part_size {
        let offset_in_part = ((offset + content_len) % part_size) as usize;
        let range = format!("bytes={}-{}", offset_in_part, part_size - 1);

        let out = s3client.get_object()
            .bucket(&s3config.bucket)
            .key(format!("{}/{}", key, parts_num))
            .range(range)
            .send()
            .await?;

        let mut s3reader = out.body.into_async_read();
        s3reader.read_exact(&mut buff[offset_in_part..]).await?;

        content_len = 0;
    } else {
        content_len -= part_size - (offset % part_size);
    }

    s3client.put_object()
        .bucket(&s3config.bucket)
        .key(format!("{}/{}", key, parts_num))
        .body(ByteStream::from(buff.clone()))
        .send()
        .await?;

    parts_num += 1;

    // |..{....|....}..|
    while content_len > 0 {
        let read_len = min(part_size, content_len);
        reader.read_exact(&mut buff[..read_len as usize]).await?;

        s3client.put_object()
            .bucket(&s3config.bucket)
            .key(format!("{}/{}", key, parts_num))
            .body(ByteStream::from(buff.clone()))
            .send()
            .await?;

        content_len -= read_len;
        parts_num += 1;
    }
    Ok(())
}

async fn update_object_api(
    key: KeyQuery,
    offset: OffsetQuery,
    content_len: u64,
    body: impl Stream<Item=Result<impl Buf, warp::Error>> + Unpin,
    ctx: Arc<Context>,
) -> Response {
    let res = update_object(
        &ctx,
        key.key,
        offset.offset,
        content_len,
        body
    ).await;

    match res {
        Ok(_) => Response::new(Body::empty()),
        Err(e) => {
            let mut resp = Response::new(Body::from(e.to_string()));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp
        }
    }
}

fn range_to_string(range: (Option<u64>, Option<u64>)) -> Result<String> {
    let str = match range {
        (Some(start), Some(end)) => format!("{}-{}", start, end),
        (Some(start), None) => format!("{}-", start),
        (None, Some(end)) => format!("-{}", end),
        _ => return Err(anyhow!("range params is invalid"))
    };
    Ok(str)
}

#[derive(Encode, Clone)]
struct Part {
    offset: u64,
    data: Vec<u8>,
}

async fn get_object_with_ranges(
    ctx: &Context,
    key: String,
    // (start position, length)
    ranges: &[(u64, u64)],
) -> Result<Vec<Part>> {
    let s3config = &ctx.s3config;
    let s3client = &ctx.s3client;

    if ranges.is_empty() {
        return Ok(Vec::new());
    }

    let lock = ctx.tasks.lock()
        .unwrap()
        .entry(key.clone())
        .or_insert(Arc::new(tokio::sync::RwLock::new(())))
        .clone();

    let _guard = lock.read().await;
    let obj = ctx.datastore.read().unwrap().get(&key).ok_or_else(|| anyhow!("{} not found", key))?;
    let part_size = obj.part_size;

    let mut parts = ranges.iter()
        .map(|(offset, len)| {
            Part {
                offset: *offset,
                data: vec![0u8; (*len) as usize],
            }
        })
        .collect::<Vec<_>>();

    let mut keymap = HashMap::new();

    for ((offset, _), part) in ranges.iter().zip(parts.iter_mut()) {
        let mut offset = *offset;
        let mut buff = part.data.as_mut_slice();
        let mut parts_num = offset / part_size;

        while !buff.is_empty() {
            let offset_in_part = (offset % part_size) as usize;
            let split_len = min(part_size as usize - offset_in_part, buff.len());

            let (l, r) = buff.split_at_mut(split_len);
            buff = r;

            let list = keymap.entry(parts_num).or_insert(Vec::new());
            list.push((offset_in_part, l));

            offset += split_len as u64;
            parts_num += 1;
        }
    }

    let mut futs = Vec::new();

    for (parts_num, parts) in keymap.iter_mut() {
        let ranges: Vec<(Option<u64>, Option<u64>)> = parts
            .iter()
            .map(|(start, part)| (Some((*start) as u64), Some((*start + part.len() - 1) as u64)))
            .collect();

        let mut ranges_str = String::from("bytes=");
        ranges_str.push_str(&range_to_string(ranges[0])?);

        for i in 1..ranges.len() {
            ranges_str.push_str(", ");
            ranges_str.push_str(&range_to_string(ranges[i])?);
        }

        let send = s3client.get_object()
            .bucket(&s3config.bucket)
            .key(format!("{}/{}", key, parts_num))
            .range(ranges_str)
            .send();

        let fut = async {
            let output = send.await?;
            let content_type = output.content_type.ok_or_else(|| anyhow!("Content-Type can't be empty"))?;

            if content_type.contains("octet-stream") {
                ensure!(parts.len() == 1);
                output.body.into_async_read().read_exact(parts[0].1).await?;
            } else {
                let boundary: String = sscanf::scanf!(
                    content_type,
                    "multipart/byteranges; boundary={}",
                    String
                ).map_or_else(|_| sscanf::scanf!(
                    content_type,
                    "multipart/byteranges;boundary={}",
                    String
                ), |v| Ok(v))
                    .map_err(|_| anyhow!("get boundary error"))?;

                let mut buff = Vec::new();
                output.body.into_async_read().read_to_end(&mut buff).await?;

                let mut multipart = multipart::server::Multipart::with_body(buff.as_slice(), boundary);
                let mut index = 0;

                while let Some(mut part_field) = multipart.read_entry()? {
                    part_field.data.read_exact(parts[index].1)?;
                    index += 1;
                }
            }
            Ok(())
        };

        futs.push(fut);
    }

    futures_util::future::try_join_all(futs).await?;
    Ok(parts)
}

async fn get_object_with_ranges_api(
    key: KeyQuery,
    // (start position, length)
    ranges: Vec<(u64, u64)>,
    ctx: Arc<Context>,
) -> Response {
    let res = get_object_with_ranges(
        &ctx,
        key.key,
        &ranges
    ).await;

    match res {
        Ok(data) => {
            let out = bincode::encode_to_vec(data, bincode::config::standard()).unwrap();
            Response::new(Body::from(out))
        },
        Err(e) => {
            let mut resp = Response::new(Body::from(e.to_string()));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
             resp
        }
    }
}

fn with_context(
    ctx: Arc<Context>
) -> impl Filter<Extract=(Arc<Context>, ), Error=std::convert::Infallible> + Clone {
    warp::any().map(move || ctx.clone())
}

#[derive(Deserialize)]
struct KeyQuery {
    key: String
}

#[derive(Deserialize)]
struct OffsetQuery {
    offset: u64,
}

#[derive(Deserialize)]
struct DataLenQuery {
    data_len: u64
}

async fn serve(
    bind: SocketAddr,
    s3_config_path: &Path,
    part_size: u64,
) -> Result<()> {
    let s3_config: S3Config = serde_json::from_reader(std::fs::File::open(s3_config_path)?)?;

    let mut builder = SdkConfig::builder()
        .endpoint_url(&s3_config.endpoint)
        .region(Region::new(s3_config.region.clone()));

    if let (Some(ak), Some(sk)) = (&s3_config.access_key, &s3_config.secret_key) {
        builder = builder.credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            ak,
            sk,
            None,
            None,
            "Static",
        )))
    }

    let s3client = Client::new(&builder.build());
    let curr = std::env::current_exe().ok();
    let parent = curr.as_deref().and_then(|v| v.parent()).unwrap_or(Path::new(""));

    let ctx = Context {
        part_size,
        datastore: std::sync::RwLock::new(Datastore::new(&parent.join("datastore.json"))?),
        tasks: std::sync::Mutex::new(HashMap::new()),
        s3client,
        s3config: s3_config,
    };

    let ctx = Arc::new(ctx);

    let put_object = warp::path!("putobject")
        .and(warp::post())
        .and(warp::query::<KeyQuery>())
        .and(warp::header::<u64>("content-length"))
        .and(warp::body::stream())
        .and(with_context(ctx.clone()))
        .then(put_object_api);

    let put_zero_object = warp::path!("putzeroobject")
        .and(warp::post())
        .and(warp::query::<KeyQuery>())
        .and(warp::query::<DataLenQuery>())
        .and(with_context(ctx.clone()))
        .then(put_zero_object_api);

    let update_object = warp::path!("updateobject")
        .and(warp::post())
        .and(warp::query::<KeyQuery>())
        .and(warp::query::<OffsetQuery>())
        .and(warp::header::<u64>("content-length"))
        .and(warp::body::stream())
        .and(with_context(ctx.clone()))
        .then(update_object_api);

    let get_object_with_ranges = warp::path!("getobjectwithranges")
        .and(warp::get())
        .and(warp::query::<KeyQuery>())
        .and(warp::body::json::<Vec<(u64, u64)>>())
        .and(with_context(ctx))
        .then(get_object_with_ranges_api);

    let router = put_object
        .or(put_zero_object)
        .or(update_object)
        .or(get_object_with_ranges);

    let serve = warp::serve(router);
    info!("Listening on http://{}", bind);
    serve.bind(bind).await;
    Ok(())
}

#[derive(Parser)]
#[command(version)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:30005")]
    bind_addr: SocketAddr,

    #[arg(short, long)]
    s3config_path: PathBuf,

    /// 1GB
    #[arg(short, long, default_value_t = 1073741824)]
    part_size: u64
}

fn logger_init() -> Result<()> {
    let pattern = if cfg!(debug_assertions) {
        "[{d(%Y-%m-%d %H:%M:%S)}] {h({l})} {f}:{L} - {m}{n}"
    } else {
        "[{d(%Y-%m-%d %H:%M:%S)}] {h({l})} {t} - {m}{n}"
    };

    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(pattern)))
        .build();

    let config = log4rs::Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(
            Root::builder()
                .appender("stdout")
                .build(LevelFilter::from_str(
                    std::env::var("RANDRW_S3").as_deref().unwrap_or("INFO"),
                )?),
        )?;

    log4rs::init_config(config)?;
    Ok(())
}

fn main() -> ExitCode {
    let args: Args = Args::parse();
    let rt = tokio::runtime::Runtime::new().unwrap();
    logger_init().unwrap();

    match rt.block_on(serve(args.bind_addr, args.s3config_path.as_path(), args.part_size)) {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{:?}", e);
            ExitCode::FAILURE
        }
    }
}
