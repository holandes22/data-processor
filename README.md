# data-collector

sudo docker build -t data-processor .

folder struct of mounted volume should be:

data/
    /collector
        /files
    /processor
        /queue
        /processed
    /log
    /run


You can inspect logs:

sudo docker exec -i -t <id> bash
less data/log/collector.log

# Compile
docker build -t process-build -f Dockerfile-compile .
docker create process-build
docker cp $id:/tmp/target/release/process dist
