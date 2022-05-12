use std::error::Error;

use chrono::{NaiveDateTime, TimeZone};
use chrono_tz::{Asia::Shanghai};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message,
};

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
    //     .set("group.id", "test-group")
    //     .clone();
    let consumer: StreamConsumer = cfg.create()?;

    consumer.subscribe(&["test"])?;
    loop {
        match consumer.recv().await {
            Err(e) => println!("err: {}", e),
            Ok(msg) => {
                let key = msg.key_view::<str>().unwrap().unwrap();
                let value = msg.payload_view::<str>().unwrap().unwrap();
                let timestamp = msg.timestamp().to_millis().unwrap() / 1000;
                let nativetime = NaiveDateTime::from_timestamp(timestamp, 0);
                let timestamp = Shanghai.from_utc_datetime(&nativetime);
                let offset = msg.offset();
				println!("key: {key},value: {value},time: {timestamp},offset: {offset}");
            }
        };
    }
}
