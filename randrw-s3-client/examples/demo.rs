use std::io::Cursor;
use std::str::FromStr;
use rand::Rng;

#[tokio::main]
async fn main() {
    let mut args = std::env::args();
    args.next();

    let mode = args.next().unwrap();

    match mode.as_str() {
        "alloc" => {
            let key = args.next().unwrap();
            let size = u64::from_str(&args.next().unwrap()).unwrap();

            randrw_s3_client::put_zero_object(
                &key,
                size
            ).await.unwrap();
        }
        "update" => {
            let key = args.next().unwrap();
            let offset = u64::from_str(&args.next().unwrap()).unwrap();
            let size = u64::from_str(&args.next().unwrap()).unwrap();

            let data = vec![1u8; size as usize];

            randrw_s3_client::update_object(
                &key,
                offset,
                size,
                Cursor::new(data)
            ).await.unwrap();
        }
        "put" => {
            let key = args.next().unwrap();
            let file_path = args.next().unwrap();

            let f = tokio::fs::File::open(&file_path).await.unwrap();
            let md = f.metadata().await.unwrap();
            let file_size = md.len();

            randrw_s3_client::put_object(&key, file_size, f).await.unwrap();
        }
        "get" => {
            let key = args.next().unwrap();
            let chunks = usize::from_str(&args.next().unwrap()).unwrap();
            let mut rng = rand::thread_rng();

            let ranges = (0..32768).into_iter()
                .map(|_| (rng.gen_range(0u64..1024 * 1024 * 1024), 32u64))
                .collect::<Vec<_>>();

            let mut futs = Vec::new();

            for chunk in ranges.chunks(chunks) {
                let fut = async {
                    randrw_s3_client::get_object_with_ranges(&key, chunk).await.unwrap()
                };

                futs.push(fut);
            }

            futures_util::future::join_all(futs).await;
        }
        _ => unimplemented!()
    }
}