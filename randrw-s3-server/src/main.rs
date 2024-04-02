// todo last part length bug

use std::{io, task, vec};
use std::cmp::min;
use std::collections::HashMap;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;
use std::vec::IntoIter;

use anyhow::{anyhow, ensure, Result};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
use aws_types::region::Region;
use aws_types::SdkConfig;
use bincode::Encode;
use clap::Parser;
use futures_util::future::MapOk;
use futures_util::TryFutureExt;
use futures_util::TryStreamExt;
use log::{debug, info, LevelFilter};
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use mimalloc::MiMalloc;
use rand::Rng;
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio_util::bytes::Buf;
use tokio_util::io::StreamReader;
use warp::{Filter, Stream};
use warp::http::StatusCode;
use warp::hyper::Body;
use warp::hyper::body::Bytes;
use warp::hyper::client::connect::dns::{GaiAddrs, GaiFuture, GaiResolver, Name};
use warp::hyper::client::HttpConnector;
use warp::hyper::service::Service;
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

async fn multipart_upload(
    client: &Client,
    bucket: &str,
    key: &str,
    mut body: Bytes
) -> Result<()> {
    // 447 MB
    const UPLOAD_PART_SIZE: usize = 447 * 1024 * 1024;

    let upload_out = client.create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = upload_out
        .upload_id()
        .ok_or_else(|| anyhow!("{}, must need upload id", key))?;

    let mut etags = Vec::new();
    let mut part_num = 1;

    while body.len() > 0 {
        let read_size = min(UPLOAD_PART_SIZE, body.len());
        let upload_data = body.split_to(read_size);

        let etag = client.upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_num)
            .body(ByteStream::from(upload_data))
            .send()
            .await?
            .e_tag
            .ok_or_else(|| anyhow!("{} must need e_tag", key))?;

        etags.push(etag);
        part_num += 1;
    }

    let parts = etags.into_iter()
        .enumerate()
        .map(|(i, e_tag)| {
            CompletedPart::builder()
                .part_number(i as i32 + 1)
                .e_tag(e_tag)
                .build()
        })
        .collect::<Vec<_>>();

    client.complete_multipart_upload()
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await?;

    Ok(())
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
        .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(())))
        .clone();

    let _guard = lock.write().await;

    let mut reader = StreamReader::new(body.map_err(|e| io::Error::new(io::ErrorKind::Other, e)));
    let mut buff = vec![0u8; part_size as usize];
    let mut parts_num = 0;

    let (_notify, notified) = tokio::sync::watch::channel(());
    let sem = tokio::sync::Semaphore::new(8);
    let sem = Arc::new(sem);

    let mut futus = Vec::new();

    while body_len > 0 {
        let sem_guard = sem.clone().acquire_owned().await?;

        let read_len = min(part_size, body_len);
        reader.read_exact(&mut buff[..read_len as usize]).await?;

        let mut notified = notified.clone();

        let content_len = ctx.s3client.head_object()
            .bucket(&s3config.bucket)
            .key(format!("{}/{}", key, parts_num))
            .send()
            .await
            .ok()
            .and_then(|out| out.content_length.map(|v| v as u64));

        if content_len != Some(part_size) {
            let s3client = ctx.s3client.clone();
            let bucket = s3config.bucket.clone();
            let key = format!("{}/{}", key, parts_num);
            let buff= Bytes::copy_from_slice(&buff);

            let fut = tokio::spawn(async move {
                let fut = async {
                    let _guard = sem_guard;
                    multipart_upload(&s3client, &bucket, &key, buff).await
                };

                tokio::select! {
                    res = fut => res,
                    _ = notified.changed() => Err(anyhow!("abort task"))
                }
            })
            .map_err(|e| anyhow!(e))
            .and_then(|r| async move { r });

            futus.push(fut);
        }

        body_len -= read_len;
        parts_num += 1;
    }

    futures_util::future::try_join_all(futus).await?;

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
    let buff = [0u8; 8192];
    let stream = futures_util::stream::repeat_with(|| Ok(buff.as_slice()));

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
            let mut resp = Response::new(Body::from(format!("{:?}", e)));
            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            resp
        }
    }
}

fn object_exist_api (
    key: KeyQuery,
    ctx: Arc<Context>,
) -> Response {
    let res = ctx.datastore.read().unwrap().get(&key.key).is_some();
    Response::new(Body::from(res.to_string()))
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
        .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(())))
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

    multipart_upload(s3client, &s3config.bucket, &format!("{}/{}", key, parts_num), Bytes::copy_from_slice(&buff)).await?;

    parts_num += 1;

    // |..{....|....}..|
    while content_len > 0 {
        let read_len = min(part_size, content_len);
        reader.read_exact(&mut buff[..read_len as usize]).await?;

        if read_len < part_size {
            let range = format!("bytes={}-{}", read_len, part_size - 1);

            let out = s3client.get_object()
                .bucket(&s3config.bucket)
                .key(format!("{}/{}", key, parts_num))
                .range(range)
                .send()
                .await?;

            let mut s3reader = out.body.into_async_read();
            s3reader.read_exact(&mut buff[read_len as usize..]).await?;
        }

        multipart_upload(s3client, &s3config.bucket, &format!("{}/{}", key, parts_num), Bytes::copy_from_slice(&buff)).await?;

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
            let mut resp = Response::new(Body::from(format!("{:?}", e)));
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
    let t = Instant::now();

    let s3config = &ctx.s3config;
    let s3client = &ctx.s3client;

    if ranges.is_empty() {
        return Ok(Vec::new());
    }

    let lock = ctx.tasks.lock()
        .unwrap()
        .entry(key.clone())
        .or_insert_with(|| Arc::new(tokio::sync::RwLock::new(())))
        .clone();

    let _guard = lock.read().await;

    let obj = ctx.datastore.read().unwrap().get(&key).ok_or_else(|| anyhow!("{} not found", key))?;
    let part_size = obj.part_size;

    let parts = ranges.iter()
        .map(|(offset, len)| {
            Part {
                offset: *offset,
                data: vec![0u8; (*len) as usize],
            }
        })
        .collect::<Vec<_>>();

    let parts = Box::leak(Box::new(parts));
    let parts_ptr = parts as *mut Vec<Part> as usize;

    let keymap: &mut HashMap<u64, Vec<(usize, &mut [u8])>> = Box::leak(Box::new(HashMap::new()));
    let keymap_ptr = keymap as *mut HashMap<u64, Vec<(usize, &mut [u8])>> as usize;

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
    let (notify, notified) = tokio::sync::watch::channel(());

    for (parts_num, parts) in keymap.iter_mut() {
        let ranges: Vec<(Option<u64>, Option<u64>)> = parts
            .iter()
            .map(|(start, part)| (Some((*start) as u64), Some((*start + part.len() - 1) as u64)))
            .collect();

        let mut ranges_str = String::from("bytes=");
        ranges_str.push_str(&range_to_string(ranges[0]).unwrap());

        for i in 1..ranges.len() {
            ranges_str.push_str(", ");
            ranges_str.push_str(&range_to_string(ranges[i]).unwrap());
        }

        let send = s3client.get_object()
            .bucket(&s3config.bucket)
            .key(format!("{}/{}", key, parts_num))
            .range(ranges_str)
            .send();

        let fut = async move {
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

        let mut notified = notified.clone();

        let fut = tokio::spawn(async move {
            tokio::select! {
                res = fut => res,
                _ = notified.changed() => Err(anyhow!("abort task"))
            }
        })
        .map_err(|e| anyhow!(e))
        .and_then(|r| async move { r });

        futs.push(fut);
    }

    let futs_res = futures_util::future::try_join_all(futs).await;
    drop(notified);
    let _ = notify.send(());
    notify.closed().await;

    let parts;

    unsafe {
        let _ = Box::from_raw(keymap_ptr as *mut HashMap<u64, Vec<(usize, &mut [u8])>>);
        parts = *Box::from_raw(parts_ptr as *mut Vec<Part>);
    }

    futs_res?;
    info!("get object with ranges use: {:?}, ranges: {}", t.elapsed(), ranges.len());
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
            let mut resp = Response::new(Body::from(format!("{:?}", e)));
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


#[derive(Clone)]
struct RoundRobin {
    inner: GaiResolver,
}

impl Service<Name> for RoundRobin {
    type Response = IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future = MapOk<GaiFuture, fn(GaiAddrs) -> IntoIter<SocketAddr>>;

    fn poll_ready(&mut self, cx: &mut task::Context<'_>) -> Poll<Result<(), io::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, name: Name) -> Self::Future {
        self.inner.call(name).map_ok(|v| {
            let mut list: Vec<SocketAddr> = v.collect();

            if list.len() > 1 {
                let i = rand::thread_rng().gen_range(0..list.len());
                list = vec![list[i]];
            }

            if !list.is_empty() {
                debug!("ip select {}", list[0]);
            }
            list.into_iter()
        })
    }
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

    let connector = HttpConnector::new_with_resolver(RoundRobin { inner: GaiResolver::new() });

    let client = HyperClientBuilder::new()
        .build(connector);

    builder.set_http_client(Some(client));

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

    let object_exist = warp::path!("objectexist")
        .and(warp::get())
        .and(warp::query::<KeyQuery>())
        .and(with_context(ctx.clone()))
        .map(object_exist_api);

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
        .or(get_object_with_ranges)
        .or(object_exist);

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
