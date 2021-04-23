// Rule
// string: N(int16)+string
// array: N(int32)+object
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func init() {
	// from 1
	rc = 1
	// kafka server host
	bootStrapServer = flag.String("bootstrap-server", "", "kafka host")
	// consumer group id
	consumerGroup = flag.String("group", "", "consumer group name")
	// topic name
	topic = flag.String("topic", "", "consume topic")
	// partition index
	partition = flag.String("partition", "", "partition index")
	// all brokers connection
	brokersConn = make(map[int32]*net.Conn)
	// kafka
	K = &Kafka{
		Groups: make(map[string]*ConsumerGroup),
		Brokers: make(map[int32]*Broker),
	}
	//consumer offset leader
	consumerOffsetPartitionLeader = make(map[int32]int32)
	// prom gauge vector
	LagVec = getLagGaugeVec()
	parseConfig()
}

var (
	bootStrapServer 		      *string // kafka server host
	consumerGroup			   	  *string // consumer group id
	topic        			  	  *string // topic name
	partition    			      *string  // specify partition index
	rc            			      int32   // global count
	connections    			   	  sync.Map // connections map
	fetchers        		   	  sync.Map // requested fetchers
	brokersConn    			  	  map[int32]*net.Conn
	K                             *Kafka
	consumerOffsetPartitionNum    int32 // __consumer_offset topic partition num
	consumerOffsetPartitionLeader map[int32]int32
	LagVec                        *prometheus.GaugeVec
	PromPort                      string
)

// Default buffer size
const BufferSize = 4096
const ConsumerOffsetTopicName = "__consumer_offsets"

type ConsumerGroup struct {
	GroupId       string // group name
	Topics        map[string]*Topic // consumed topics
	CoordinatorId int32 // consumer group coordinator
}

type Partition struct {
	Index           int32  // partition index
	CommittedOffset int64 // committed offset of each partition
	MaxNumOffset    int64 // max num offset
	LeaderId        int32 // leader replica broker id of partition
}

type Topic struct {
	Name string  // topic name
	Partitions map[int32]*Partition
}

// Broker
type Broker struct {
	NodeId int32  // node id
	Host   string // host of broker
	Port   int32  // host port
}

type Kafka struct {
	// only controller broker can receive offsetFetch request and get partition info from other broker
	ControllerId int32 //The ID of the controller broker.
	Groups       map[string]*ConsumerGroup
	Brokers      map[int32]*Broker
}

type Fetcher struct {
	p               *Parser
	conn            net.Conn
	Done            chan error //Done
	ConsumerGroup   string
	Topics    	    []string
	Partitions      []int32
	CurrentOffset   uint32
	LogEndOffset    uint32
	Lag       	    uint32
	Brokers         []string
	ctx             context.Context

	// request type parser
	metadata        *Metadata
	offsetFetch     *OffsetFetch
	listOffset      *ListOffset
	describeLogDirs *DescribeLogDirs
}

// if no flag, parse config file
func parseConfig() {
	config, _ := ioutil.ReadFile("./config.json")
	c := struct {
		GroupId string `json:"group_id"`
		BootstrapServer string `json:"bootstrap_server"`
		Topic []string `json:"topic"`
		PromPort string `json:"prom_port"`
	}{}
	_ = json.Unmarshal(config, &c)
	*bootStrapServer = c.BootstrapServer
	*consumerGroup = c.GroupId
	*topic = strings.Join(c.Topic, ",")
	PromPort = c.PromPort
}

// init lag gauge vector
func getLagGaugeVec() *prometheus.GaugeVec {
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "partition_lag",
		Help: "partition lag",
	}, []string{"group_id", "topic", "partition"})
	// must register collector before expose/push
	prometheus.MustRegister(vec)
	return vec
}

// new Kafka fetcher
func NewKafkaFetcher() *Fetcher {
	var f Fetcher
	if !flag.Parsed() {
		flag.Parse()
	}
	conn, err := NewKafkaClient()
	if err != nil {
		log.Fatalf("can not connecting kafka server: %v", err)
	}
	f.conn = conn
	f.Done = make(chan error, 1)
	if *consumerGroup != "" {
		f.ConsumerGroup = *consumerGroup
	}
	if *topic != "" {
		f.Topics = strings.Split(*topic, ",")
	}
	if *partition != "" {
		partitionFlag := strings.Split(*partition, ",")
		for _, p := range partitionFlag {
			pi, _ := strconv.Atoi(p)
			f.Partitions = append(f.Partitions, int32(pi))
		}
	}
	f.ctx = context.Background()

	return &f
}

// new kafka server connection
func NewKafkaClient() (conn net.Conn, err error) {
	if *bootStrapServer == "" {
		return nil, errors.New("kafka host must not be empty")
	}
	if !regexp.MustCompile(`^[^:]*:\d+$`).Match([]byte(*bootStrapServer)) {
		*bootStrapServer = *bootStrapServer + `:9092`
	}
	// Kafka server closed connection
	// if c, ok := connections.Load(*bootStrapServer); ok {
	//	 return c.(net.Conn), nil
	// }
	conn, err = net.Dial("tcp", *bootStrapServer)
	if err != nil {
		return nil, err
	}
	//connections.Store(*bootStrapServer, conn)
	return
}

// get consumer offset partition num by metadata request
func GetConsumerOffsetPartitionNum() error {
	f := NewKafkaFetcher()
	f.Topics = f.Topics[:0]
	f.Topics = append(f.Topics, ConsumerOffsetTopicName)
	f.NewMetadataRequest()
	if len(f.Done) == 1 {
		return <- f.Done
	}
	for _, p := range f.metadata.Topics[ConsumerOffsetTopicName].Partitions {
		consumerOffsetPartitionLeader[p.Index] = p.LeaderId
	}
	atomic.StoreInt32(&consumerOffsetPartitionNum, int32(len(f.metadata.Topics[ConsumerOffsetTopicName].Partitions)))
	//fetchers.Store("metadata", f)
	return nil
}

// find coordinator broker
// Rule: Math.abs(groupID.hashCode()) % numPartitions(__consumer_offset)
func FindCoordinator(groupId string) (nodeId int32) {
	return consumerOffsetPartitionLeader[hashCode(groupId) % atomic.LoadInt32(&consumerOffsetPartitionNum)]
}

// calculate string hashcode
func hashCode(s string) (hash int32) {
	if len(s) > 0 && hash == 0 {
		for _, b := range s {
			hash = hash * 31 + b
		}
	}
	return
}

// connect to all brokers
func (f *Fetcher) ConnectToAllBrokers() (err error) {
	metadataFetcher := f.getRequestedMetadataFetcher()
	if len(f.Done) > 0 {
		return <- f.Done
	}
	for _, broker := range metadataFetcher.metadata.Brokers {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", broker.Host, broker.Port))
		if err != nil {
			return fmt.Errorf("connect to broker: %s failed, error: %v", broker.Host, err)
		}
		brokersConn[broker.NodeId] = &conn
	}
	return
}

// K properties
func (f *Fetcher) ConstructKProperties() {
	metadataFetcher := f.getRequestedMetadataFetcher()
	K.ControllerId = metadataFetcher.metadata.ControllerId
	for _, b := range metadataFetcher.metadata.Brokers {
		K.Brokers[b.NodeId] = &Broker{
			NodeId: b.NodeId,
			Host: b.Host,
			Port: b.Port,
		}
	}
	group := &ConsumerGroup{
		GroupId: f.ConsumerGroup,
		Topics: make(map[string]*Topic),
		CoordinatorId: FindCoordinator(f.ConsumerGroup),
	}
	// topics
	for _, topicName := range f.Topics {
		t := &Topic{
			Name: topicName,
			Partitions: make(map[int32]*Partition),
		}
		for index, p := range metadataFetcher.metadata.Topics[topicName].Partitions {
			partition := &Partition{
				Index: index,
				LeaderId: p.LeaderId,
			}
			t.Partitions[index] = partition
		}
		group.Topics[topicName] = t
	}
	K.Groups[f.ConsumerGroup] = group
}

// return a requested fetcher of Metadata
func (f *Fetcher) getRequestedMetadataFetcher() (metadataFetcher *Fetcher) {
	if _f, ok := fetchers.Load("metadata");ok {
		metadataFetcher = _f.(*Fetcher)
	} else {
		metadataFetcher = NewKafkaFetcher()
		metadataFetcher.NewMetadataRequest()
		if len(metadataFetcher.Done) > 0 {
			err := <- metadataFetcher.Done
			f.Done <- err
			return nil
		}
		fetchers.Store("metadata", metadataFetcher)
	}
	return
}

// new Metadata request
func (f *Fetcher) NewMetadataRequest() {
	if len(f.Topics) == 0 {
		err := errors.New("topic must not be empty for metadata request")
		f.Done <- err
		return
	}
	f.p = NewMetadataParser()
	f.p.preRequest()
	// [TopicNames]
	if len(f.Topics) == 0 {
		f.p.writeInt32ToBinary(-1)
	} else {
		f.p.writeInt32ToBinary(int32(len(f.Topics)))
		for _, topic := range f.Topics {
			f.p.writeStringToBinary(topic)
		}
	}
	f.SendRequest(-1)
	f.metadata = NewMetadata()
	f.metadata.p = f.p
	err := f.metadata.Parse()
	if err != nil {
		f.Done <- err
		return
	}
}

// new listOffset request
func (f *Fetcher) NewListOffsetRequest(replicaId int32, topicName string, partitionIndex int32) {
	err := f.ConnectToAllBrokers()
	if err != nil {
		f.Done <- err
		return
	}

	f.p = NewListOffsetParser()
	f.p.preRequest()

	// replica_id [topics:name,[partitions:partition_index,timestamp]]
	// find leader id of each partition replica
	f.p.writeInt32ToBinary(replicaId)
	f.p.writeInt32ToBinary(1)
	f.p.writeStringToBinary(topicName)
	f.p.writeInt32ToBinary(1)
	f.p.writeInt32ToBinary(partitionIndex)
	//timestamp
	f.p.writeInt64ToBinary(-1)

	f.SendRequest(replicaId)
	// Parse ListOffset response
	f.listOffset = NewListOffset()
	f.listOffset.p = f.p
	err = f.listOffset.Parse()
	if err != nil {
		f.Done <- err
		return
	}
}

// List each partition's offset
func (f *Fetcher) ListTopicOffset() (err error) {
	for groupName, group := range K.Groups {
		for topicName, topic := range group.Topics {
			for index, partition := range topic.Partitions {
				if partition.LeaderId == 0 {
					return fmt.Errorf("wrong leader id: [%s - %s - %d]", groupName, topicName, index)
				}
				f.NewListOffsetRequest(partition.LeaderId, topicName, index)
				if len(f.Done) > 0 {
					return <- f.Done
				}
				partition.MaxNumOffset = f.listOffset.Topics[topicName].Partitions[index].Offset
				lag := math.Abs(float64(partition.MaxNumOffset - partition.CommittedOffset))
				LagVec.With(prometheus.Labels{
					"group_id": groupName,
					"topic": topicName,
					"partition": fmt.Sprintf("%d", index),
				}).Set(lag)
			}
		}
	}
	return
}

// New DescribeLogDirs request
func (f *Fetcher) NewDescribeLogDirsRequest() {
	if len(f.Topics) == 0 {
		err := errors.New("topic must not be empty for metadata request")
		f.Done <- err
		return
	}

	f.p = NewDescribeLogDirsParser()
	f.p.preRequest()

	// construct metadata request
	metadataFetcher := f.getRequestedMetadataFetcher()

	// Topics [Name, [PartitionIndex]]
	f.p.writeInt32ToBinary(int32(len(f.Topics)))
	for _, topic := range f.Topics {
		f.p.writeStringToBinary(topic)
		f.p.writeInt32ToBinary(int32(len(metadataFetcher.metadata.Topics[topic].Partitions)))
		// write partitions index
		for index := range metadataFetcher.metadata.Topics[topic].Partitions {
			f.p.writeInt32ToBinary(index)
		}
	}
	f.SendRequest(-1)
	f.describeLogDirs = NewDescribeLogDirs()
	f.describeLogDirs.p = f.p
	err := f.describeLogDirs.Parse()
	if err != nil {
		f.Done <- err
		return
	}
}

// New offset fetch request
func (f *Fetcher) NewOffsetFetchRequest() {
	if f.ConsumerGroup == "" {
		err := errors.New("consumer group must not be empty for offset fetch request")
		f.Done <- err
		return
	}
	if len(f.Topics) == 0 {
		err := errors.New("topic must not be empty for metadata request")
		f.Done <- err
		return
	}

	err := f.ConnectToAllBrokers()
	if err != nil {
		f.Done <- err
		return
	}

	// construct metadata request
	metadataFetcher := f.getRequestedMetadataFetcher()

	f.p = NewOffsetFetchParser()
	f.p.preRequest()

	// ConsumerGroup [TopicName [Partition]]
	f.p.writeStringToBinary(f.ConsumerGroup)
	f.p.writeInt32ToBinary(int32(len(f.Topics)))
	// write topics
	for _, topic := range f.Topics {
		f.p.writeStringToBinary(topic)
		f.p.writeInt32ToBinary(int32(len(metadataFetcher.metadata.Topics[topic].Partitions)))
		// write partitions index
		for index := range metadataFetcher.metadata.Topics[topic].Partitions {
			f.p.writeInt32ToBinary(index)
		}
	}
	f.SendRequest(K.Groups[f.ConsumerGroup].CoordinatorId)
	// Parse offsetFetch response
	f.offsetFetch = NewOffsetFetch()
	f.offsetFetch.p = f.p
	err = f.offsetFetch.Parse()
	if err != nil {
		f.Done <- err
		return
	}
	// K properties
	for name, _topic := range f.offsetFetch.Topics {
		for index, _p := range _topic.Partitions {
			K.Groups[f.ConsumerGroup].Topics[name].Partitions[index].CommittedOffset = _p.CommittedOffset
		}
	}
}

// Send request to kafka server
func (f *Fetcher) SendRequest(nodeId int32) {
	var conn net.Conn
	defer func() {
		if v := recover();v != nil {
		}
	}()
	// No connection cache. Kafka server closed connection.
	defer f.conn.Close()
	for _, bc := range brokersConn {
		defer (*bc).Close()
	}
	if nodeId == -1 {
		conn = f.conn
	} else {
		conn = *brokersConn[nodeId]
	}
	// global count
	atomic.CompareAndSwapInt32(&rc, atomic.LoadInt32(&rc), atomic.LoadInt32(&rc) / 1024)

	ctx, cancel := context.WithTimeout(f.ctx, time.Millisecond * 1000)
	defer cancel()
	p := f.p.ReqBuffer.buf[:f.p.ReqBuffer.len]
	f.p.ReqBuffer = NewParserBuffer(BufferSize)
	f.p.writePacketToBinary(p)
	_, err := conn.Write(f.p.ReqBuffer.buf[:f.p.ReqBuffer.len+4])
	if err != nil {
		f.Done <- err
		return
	}
	for {
		select {
		case <- ctx.Done():
			return
		default:
			// TODO: blocking operation
			n, err := conn.Read(f.p.ResBuffer.buf)
			if err != nil {
				if err != io.EOF {
					f.Done <- err
				}
				return
			}
			if n <= 0 {
				f.Done <- errors.New("empty response")
				return
			}
			return
		}
	}
}

// print buffer
func (f *Fetcher) PrintlnResBuffer() {
	fmt.Println(f.p.ResBuffer.buf)
}

// parse and print form K struct
func (f *Fetcher) PrintlnK() {
	fmt.Println("Brokers: ")
	fmt.Printf("\tController: %d\n", K.ControllerId)
	for _, broker := range K.Brokers {
		fmt.Printf("\tnode_id: %d\thost: %s\tport: %d\t\n", broker.NodeId, broker.Host, broker.Port)
	}
	fmt.Printf("Group: [%s]\n", f.ConsumerGroup)
	fmt.Printf("\tCoordinator: [%d]\n", K.Groups[f.ConsumerGroup].CoordinatorId)
	for name, t := range K.Groups[f.ConsumerGroup].Topics {
		fmt.Printf("\tTopic: [%s]\n", name)
		fmt.Printf("\t\tpartition\n")
		for index, p := range t.Partitions {
			fmt.Printf("\t\tindex: %d\tleader_id: %d\tcommitted_offset: %d\tmax_num_offset: %d\t\n", index, p.LeaderId, p.CommittedOffset, p.MaxNumOffset)
		}
	}
}
