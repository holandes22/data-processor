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
