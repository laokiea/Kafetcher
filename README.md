# Kafetcher
Kafka LAG watcher

## Usage
### Docker (recommend way)
Edit your config.json and configure your kafka server host, group_id, topics and prometheus server port

`docker build -t kafetcher:1.0 -f Dockerfile ./ && docker run -itd --restart=always --mount type=volume,source=kafetcher,target=/data/logs/kafetcher -p your_config_port:your_config_port kafetcher:1.0`
