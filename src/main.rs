#[macro_use]
extern crate log;
extern crate glob;
extern crate amqp;
extern crate hyper;
extern crate env_logger;
extern crate rustc_serialize;

use std::env;
use std::thread;
use std::fs::File;
use std::error::Error;
use std::io::BufRead;
use std::io::BufReader;
use std::result::Result;

use glob::glob;
use hyper::Client;
use hyper::status::StatusCode;
use rustc_serialize::json;
use amqp::protocol;
use amqp::table;
use amqp::basic::Basic;
use amqp::session::Session;
use amqp::channel::{Channel, ConsumerCallBackFn};


fn process(channel: &mut Channel, deliver: protocol::basic::Deliver, headers: protocol::basic::BasicProperties, body: Vec<u8>){
    let glob_path = String::from_utf8_lossy(&body);
    info!("Got message with headers: {:?}", headers);
    info!("Got glob path: {:?}", glob_path);

    let mut payload = "".to_string();
    for path in glob(&glob_path).unwrap().filter_map(Result::ok) {
        debug!("Processing file path: {:?}", path);
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(why) => panic!("couldn't open {}: {}", path.display(),
                                                       Error::description(&why)),
        };
        for line in BufReader::new(file).lines() {
            let json = json::Json::from_str(&line.unwrap());
            match json {
                Ok(data) => {
                    let obj = data.as_object().unwrap();
                    let operation = obj.get("operation").unwrap().as_string().unwrap();
                    let username = obj.get("username").unwrap().as_string().unwrap();
                    let timestamp = obj.get("eventTime").unwrap().as_i64().unwrap() * 1000000;
                    let timestamp = data.find_path(&["eventTime"]).unwrap();
                    let stat = format!("operation_{},username={} value=1i {}\n", operation, username, timestamp);
                    payload = payload + &stat;

                },
                Err(e) => {
                    error!("error parsing line: {:?}", e);
                }
            }
        }
    }
    let influxdb_host = match env::var("INFLUXDB_SERVICE_HOST") {
        Ok(val) => val,
        Err(e) => panic!("couldn't find service host for influxdb")
    };
    let client = Client::new();
    let res = client.post(&format!("http://{}:8086/write?db=mydb", influxdb_host))
        .body(&payload)
        .send()
        .unwrap();
    assert_eq!(res.status, StatusCode::NoContent);
    channel.basic_ack(deliver.delivery_tag, false);
}


fn main() {
    env_logger::init().unwrap();

    let influxdb_host = match env::var("INFLUXDB_SERVICE_HOST") {
        Ok(val) => val,
        Err(e) => panic!("couldn't find service host for influxdb")
    };
    let rabbitmq_host = match env::var("RABBITMQ_SERVICE_HOST") {
        Ok(val) => val,
        Err(e) => panic!("couldn't find service host for rabbitmq")
    };

    let client = Client::new();
    let res = client.get(&format!("http://{}:8086/query?q=CREATE DATABASE mydb", influxdb_host))
        .send()
        .unwrap();
    assert_eq!(res.status, StatusCode::Ok);

    let amqp_url = format!("amqp://{}//", rabbitmq_host);
    let mut session = match Session::open_url(&amqp_url) {
        Ok(session) => session,
        Err(error) => panic!("Can't create session: {:?}", error)
    };
    let mut channel = session.open_channel(1).ok().expect("Can't open channel");

    let queue_name = "processor";
    //queue: &str, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, nowait: bool, arguments: Table
    let queue_declared = channel.queue_declare(queue_name, false, false, false, false, false, table::new());
    info!("Queue {:?} declared", queue_name);
    channel.basic_consume(
        process as ConsumerCallBackFn, queue_name, "", false, false, false, false, table::new());
    channel.start_consuming();

    let consumers_thread = thread::spawn(move || {
        channel.start_consuming();
        channel
    });

    // There is currently no way to stop the consumers, so we infinitely join thread.
    let mut channel = consumers_thread.join().ok().expect("Can't get channel from consumer thread");
    channel.close(200, "Closing channel".to_string());
    session.close(200, "Closing session".to_string());
}
