extern crate glob;
extern crate hyper;
extern crate rustc_serialize;

use std::fs::File;
use std::error::Error;
use std::io::BufRead;
use std::io::BufReader;
use std::result::Result;
use std::env;

use glob::glob;
use rustc_serialize::json;
use hyper::Client;
use hyper::status::StatusCode;


fn main() {
    let mut payload = "".to_string();
    for path in glob(env::args[0]).unwrap().filter_map(Result::ok) {
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
                    let timestamp = data.find_path(&["eventTime"]).unwrap();
                    let stat = format!("operation_{},username={} value=1i {}\n", operation, username, timestamp);
                    payload = payload + &stat;

                },
                Err(e) => {
                    println!("error parsing line: {:?}", e);
                }
            }
        }
    }
    let client = Client::new();
    let res = client.post("http://localhost:8086/write?db=mydb")
        .body(&payload)
        .send()
        .unwrap();
    assert_eq!(res.status, StatusCode::NoContent);
}
