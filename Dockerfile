FROM ubuntu:14.04.3

RUN apt-get update && apt-get install -y curl && apt-get clean && rm -rf /var/lib/apt/lists

WORKDIR /usr/src/app
RUN mkdir -p /usr/src/app

#ONBUILD COPY ./target/release/process /usr/src/app/
#ONBUILD COPY  wait_to_start usr/src/app/
#COPY directly to /usr/src/app was not working
#Took workaround from https://github.com/docker/docker/issues/7511
COPY ./target/release/process /tmp/
RUN cp /tmp/process /usr/src/app && rm -rf /tmp/process

CMD [ "./process" ]
