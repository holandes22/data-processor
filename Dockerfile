FROM debian:jessie

RUN apt-get update
RUN apt-get install -y libssl1.0.0

ADD target/release/process /process

CMD [ "./process" ]
