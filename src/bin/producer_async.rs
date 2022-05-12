use std::{error::Error, time::Duration};

use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
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
        .clone();

    // let cfg = ClientConfig::new()
    //     .set("bootstrap.servers", "localhost:9092")
    //     .set("message.timeout.ms", "5000")
    //     .clone();

    let producer: FutureProducer = cfg.create().expect("");
    let (tx, mut rx) = mpsc::channel(10);
    for i in 0..100 {
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
    loop {
        tokio::select! {
            Some(msg) = rx.recv() => {
                println!("{:?}", msg);
            },
            _ = tokio::time::sleep(Duration::from_secs(6)) => {
                println!("timeout after 6 seconds");
                break;
            }
        }
    }
    Ok(())
}
