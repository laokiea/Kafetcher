package main

import (
	"github.com/laokiea/kafetcher/kafka"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"net/http/pprof"
)

var (
	once sync.Once
	logPath = "/data/logs/kafetcher/"
)

const (
	DefaultPromPort = "8765"
	LagFetchDuration = 15
)

func main() {
	setLogFile()
	go PromHttpServerStart()
	go profileServer()
	ticker := time.NewTicker(time.Second * LagFetchDuration)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <- quit:
			log.Println("server quit.")
			return
		case <-ticker.C:
			once.Do(func() {
				err := kafka.GetConsumerOffsetPartitionNum()
				if err != nil {
					log.Printf("get consumer offset partitions num error: %v", err)
				}
			})
			f := kafka.NewKafkaFetcher()
			err := f.ConnectToAllBrokers()
			if err != nil {
				log.Fatalf("connect to other broker failed: %v", err)
			}
			f.ConstructKProperties()
			f.NewOffsetFetchRequest()
			if len(f.Done) > 0 {
				log.Printf("offset fetch request failed: %v", <-f.Done)
			}

			err = f.ListTopicOffset()
			if err != nil {
				log.Fatalf("list topic offset failed: %v", err)
			}
		}
	}
}

// a http server for exposing metrics
func PromHttpServerStart() {
	var port string
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if kafka.PromPort != "" {
		port = kafka.PromPort
	} else {
		port = DefaultPromPort
	}
	httpServer := &http.Server{
		Handler: mux,
		Addr: "0.0.0.0:"+port,
	}
	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatalf("start prom server failed: %v", err)
	}
}

func profileServer() {
	mux := http.NewServeMux()
	mux.Handle("/debug/pprof", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/heap", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	_ = (&http.Server{Addr: ":8764", Handler: mux}).ListenAndServe()
}

func setLogFile() {
	if _, err := os.Stat(logPath);os.IsNotExist(err) {
		err := os.MkdirAll(logPath, 0777)
		if err != nil {
			log.Fatalf("create directory failed: %v", err)
		}
	}
	f, err := os.OpenFile(logPath+time.Now().Format("20060102")+".log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0777)
	if err != nil {
		log.Fatalf("open log file failed: %v", err)
	}
	log.SetOutput(f)
}

