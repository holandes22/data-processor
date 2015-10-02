#[macro_use]
extern crate log;
extern crate glob;
extern crate etcd;
extern crate amqp;
extern crate hyper;
extern crate regex;
extern crate chrono;
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
use regex::Regex;
use hyper::Client;
use hyper::status::StatusCode;
use rustc_serialize::json;
use amqp::protocol;
use amqp::table;
use amqp::basic::Basic;
use amqp::session::Session;
use amqp::channel::{Channel, Consumer};
use etcd::Client as EtcdClient;
use chrono::{DateTime, UTC, Local, TimeZone};


fn handle_json(file: &File) -> String {
    debug!("Processing json");
    let mut payload = String::new();
    for line in BufReader::new(file).lines() {
        let json = json::Json::from_str(&line.unwrap());
        match json {
            Ok(data) => {
                let obj = data.as_object().unwrap();
                let operation = obj.get("operation").unwrap().as_string().unwrap();
                let username = obj.get("username").unwrap().as_string().unwrap();
                let timestamp = obj.get("eventTime").unwrap().as_i64().unwrap() * 1000000;
                let stat = format!("operation_{},username={} value=1i {}\n", operation, username, timestamp);
                payload = payload + &stat;

            },
            Err(e) => {
                error!("Error parsing line: {:?}. Skipping.", e);
            }
        }
    }
    return payload;
}


fn handle_text(file: &File) -> String {
    debug!("Processing text");
    let mut payload = String::new();
    let date_re = Regex::new(r"(?P<date>\S*) (?P<time>[\d:]+)").unwrap();
    let username_re = Regex::new(r".*ugi=(?P<user>[\w]+)").unwrap();
    let operation_re = Regex::new(r".*cmd=(?P<operation>[\w]+)").unwrap();

    for line in BufReader::new(file).lines() {
        let text = line.unwrap();
        let mut caps = date_re.captures(&text).unwrap();
        let timestamp = UTC.datetime_from_str(caps.at(0).unwrap(), "%Y-%m-%d %H:%M:%S").unwrap().timestamp();
        caps = username_re.captures(&text).unwrap();
        let username = caps.at(1).unwrap();
        caps = operation_re.captures(&text).unwrap();
        let operation = caps.at(1).unwrap();
        let stat = format!("operation_{},username={} value=1i {}\n", operation, username, timestamp);
        payload = payload + &stat;
    }
    return payload;
}


struct ProcessConsumer {
    influxdb_host: String,
    log_format: String
}

impl Consumer for ProcessConsumer {

    fn handle_delivery(&mut self, channel: &mut Channel, deliver: protocol::basic::Deliver, headers: protocol::basic::BasicProperties, body: Vec<u8>){
        let glob_path = String::from_utf8_lossy(&body);
        info!("Got glob path: {:?}", glob_path);

        let mut payload = String::new();
        for path in glob(&glob_path).unwrap().filter_map(Result::ok) {
            debug!("Processing file path: {:?}", path);
            let file = match File::open(&path) {
                Ok(file) => file,
                Err(why) => panic!("couldn't open {}: {}", path.display(),
                                                           Error::description(&why)),
            };
            //TODO: should not be a conf set, just see if first line starts with "{" to
            // select json or not
            match self.log_format.as_ref() {
                "json" => {
                    payload = handle_json(&file);
                },
                "text" => {
                    payload = handle_text(&file);
                },
                _ => panic!("No handler for log type")
            };
        }
        let client = Client::new();
        let res = client.post(&format!("http://{}:8086/write?db=mydb", &self.influxdb_host.to_string()))
            .body(&payload)
            .send()
            .unwrap();
        assert_eq!(res.status, StatusCode::NoContent);
        debug!("Added stats from file {:?}", glob_path);
        channel.basic_ack(deliver.delivery_tag, false);
    }
}


fn main() {
    env_logger::init().unwrap();

    let influxdb_host = match env::var("INFLUXDB_SERVICE_HOST") {
        Ok(val) => val,
        Err(_) => panic!("couldn't find service host for influxdb")
    };
    let rabbitmq_host = match env::var("RABBITMQ_SERVICE_HOST") {
        Ok(val) => val,
        Err(_) => panic!("couldn't find service host for rabbitmq")
    };
    let etcd_host = match env::var("ETCD_SERVICE_HOST") {
        Ok(val) => val,
        Err(_) => panic!("couldn't find service host for etcd")
    };

    let etcd_client = EtcdClient::new(&format!("http://{}:4001/", etcd_host)).unwrap();
    let response = etcd_client.get("/hdfs/log_format", false, false).ok().unwrap();
    let log_format = response.node.value.unwrap();
    info!("{:?}", &log_format);

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
    channel.queue_declare(queue_name, false, false, false, false, false, table::new());
    info!("Queue {:?} declared", queue_name);
    let process_consumer = ProcessConsumer { influxdb_host: influxdb_host, log_format: log_format };
    channel.basic_consume(process_consumer, queue_name, "", false, false, false, false, table::new());
    channel.start_consuming();
    channel.close(200, "Closing channel".to_string());
    session.close(200, "Closing session".to_string());
}
