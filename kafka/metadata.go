package kafka

import (
	"fmt"
)

// Broker
type MetadataBroker struct {
	NodeId int32  // node id
	Host   string // host of broker
	Port   int32  // host port
	Rack   string // The rack of the broker, or null if it has not been assigned to a rack.
}

// Topic
type MetadataTopic struct {
	ErrorCode  int16  // topic error code
	Name       string // topic name
	IsInternal byte   // is internal topic
	Partitions map[int32]*MetadataPartition // all partitions
}

// Partition
type MetadataPartition struct {
	ErrorCode    int16  // partition error code
	Index        int32
	LeaderId     int32
	ReplicaNodes []int32 //replica nodes id
 	IsrNodes     []int32 // in sync nodes id
}

// Metadata response date
type Metadata struct {
	p            *Parser
	BrokerNum    int32 // brokers num
	Brokers      []*MetadataBroker // all brokers
	ControllerId int32 // controller broker id
	Topics       map[string]*MetadataTopic // all topics
	*CommonResponse              // common response
}

// new Metadata
func NewMetadata() *Metadata {
	return &Metadata{
		Topics: make(map[string]*MetadataTopic),
	}
}

// Parse metadata request
func (m *Metadata) Parse() (err error) {
	// common response
	m.CommonResponse = &CommonResponse{
		m.p.readInt32FromBinaryResponse(),
		m.p.readInt32FromBinaryResponse(),
	}
	// brokers
	m.parseBrokers()
	//controller id
	m.ControllerId = m.p.readInt32FromBinaryResponse()
	// topics, partitions
	err = m.parseTopics()
	if err != nil {
		return err
	}
	return
}

// Parse brokers
func (m *Metadata) parseBrokers() {
	// brokers num
	m.BrokerNum = m.p.readInt32FromBinaryResponse()
	// single broker
	for i := 0;i < int(m.BrokerNum);i++ {
		m.Brokers = append(m.Brokers, &MetadataBroker{
			m.p.readInt32FromBinaryResponse(),
			m.p.readStringFromBinaryResponse(),
			m.p.readInt32FromBinaryResponse(),
			m.p.readStringFromBinaryResponse(),
		})
	}
}

// Parse topics
func (m *Metadata) parseTopics() error {
	tn := int(m.p.readInt32FromBinaryResponse())
	for i := 0;i < tn;i++ {
		var t MetadataTopic
		t.ErrorCode = m.p.readInt16FromBinaryResponse()
		if t.ErrorCode != 0 {
			return fmt.Errorf("parse Metadata response topic error, code: %d", t.ErrorCode)
		}
		t.Name = m.p.readStringFromBinaryResponse()
		t.IsInternal = m.p.readBoolFromBinaryResponse()
		pn := int(m.p.readInt32FromBinaryResponse())
		// partitions
		t.Partitions = make(map[int32]*MetadataPartition)
		for e := 0;e < pn;e++ {
			var p MetadataPartition
			p.ErrorCode = m.p.readInt16FromBinaryResponse()
			if p.ErrorCode != 0 {
				return fmt.Errorf("parse Metadata response partition error, code: %d", p.ErrorCode)
			}
			p.Index = m.p.readInt32FromBinaryResponse()
			p.LeaderId = m.p.readInt32FromBinaryResponse()
			rnn := int(m.p.readInt32FromBinaryResponse())
			// replica nodes
			for z := 0;z < rnn;z++ {
				p.ReplicaNodes = append(p.ReplicaNodes, m.p.readInt32FromBinaryResponse())
			}
			// in-sync nodes
			inn := int(m.p.readInt32FromBinaryResponse())
			for j := 0;j < inn;j++ {
				p.IsrNodes = append(p.IsrNodes, m.p.readInt32FromBinaryResponse())
			}
			t.Partitions[p.Index] = &p
		}
		m.Topics[t.Name] = &t
	}
	return nil
}