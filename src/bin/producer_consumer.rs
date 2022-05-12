use std::{error::Error, time::Duration};

use chrono::{NaiveDateTime, TimeZone};
use chrono_tz::Asia::Shanghai;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};
use tokio::sync::mpsc;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = env!("KAFKA_ENDPOINT");
    let username = env!("KAFKA_USERNAME");
    let password = env!("KAFKA_PASSWORD");
    let cfg = ClientConfig::new()
        .set("bootstrap.servers", endpoint)
        .set("message.timeout.ms", "5000")
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "SCRAM-SHA-256")
        .set("sasl.username", username)
        .set("sasl.password", password)
        .set("group.id", "test-group")
        .clone();

    // let cfg = ClientConfig::new()
    //     .set("bootstrap.servers", "localhost:9092")
    //     .set("message.timeout.ms", "5000")
    //     .clone();

    let producer: FutureProducer = cfg.create().expect("");
    let (tx, mut rx) = mpsc::channel(10);
    for i in 0..10 {
        let p = producer.clone();
        let ttx = tx.clone();
        tokio::spawn(async move {
            let delivery_status = p
                .send(
                    FutureRecord::to("test")
                        .key(&format!("key-{i}"))
                        .payload(&format!("value-{i}")),
                    Duration::from_secs(0),
                )
                .await
                .unwrap();
            ttx.send(delivery_status).await.unwrap();
        });
    }

    let consumer: StreamConsumer = cfg.create()?;
    consumer.subscribe(&["test"])?;
    let (ctx, mut crx) = mpsc::channel(1);
    let cctx = ctx.clone();
    tokio::spawn(async move {
        loop {
            match consumer.recv().await {
                Err(e) => println!("error: {}", e),
                Ok(msg) => {
                    cctx.send(msg.detach()).await.unwrap();
                }
            }
        }
    });
    loop {
        tokio::select! {
            Some(msg) = rx.recv() => {
                println!("{:?}", msg);
            },
            Some(msg) = crx.recv() => {
                let key = msg.key_view::<str>().unwrap().unwrap();
                let value = msg.payload_view::<str>().unwrap().unwrap();
                let timestamp = msg.timestamp().to_millis().unwrap() / 1000;
                let nativetime = NaiveDateTime::from_timestamp(timestamp, 0);
                let timestamp = Shanghai.from_utc_datetime(&nativetime);
                let offset = msg.offset();
                println!("key: {key},value: {value},time: {timestamp},offset: {offset}");
            },
            _ = tokio::time::sleep(Duration::from_secs(200)) => {
                println!("timeout after 6 seconds");
                break;
            }
        }
    }
    Ok(())
}
