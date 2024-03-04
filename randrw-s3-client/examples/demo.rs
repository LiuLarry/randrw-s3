use std::io::Cursor;
use std::str::FromStr;

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
        "get" => {
            let key = args.next().unwrap();
            let offset = u64::from_str(&args.next().unwrap()).unwrap();
            let size = u64::from_str(&args.next().unwrap()).unwrap();

            let offset2 = u64::from_str(&args.next().unwrap()).unwrap();
            let size2 = u64::from_str(&args.next().unwrap()).unwrap();

            let parts = randrw_s3_client::get_object_with_ranges(
                &key,
                &[(offset, size), (offset2, size2)]
            ).await.unwrap();

            for part in parts {
                println!("offset: {}, data: {:?}", part.offset, part.data);
            }
        }
        _ => unimplemented!()
    }
}