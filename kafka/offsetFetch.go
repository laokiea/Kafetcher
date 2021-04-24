package kafka

import (
	"fmt"
)

// offset partition format
type OffsetPartition struct {
	Index           int32 // partition index
	CommittedOffset int64 //committed offset of each partition within consumer group
	Metadata        string // partition metadata
	ErrorCode       int16  // error code
}

// Topic
type OffsetTopic struct {
	Name       string // topic name
	Partitions map[int32]*OffsetPartition // all partitions
}

// offset format
type OffsetFetch struct {
	p         *Parser
	ErrorCode int16  // error code
	Topics    map[string]*OffsetTopic // all topics
	*CommonResponse              // common response
}

// new OffsetFetch
func NewOffsetFetch() *OffsetFetch {
	return &OffsetFetch{
		Topics: make(map[string]*OffsetTopic),
	}
}

//Parse response
func (offset *OffsetFetch) Parse() (err error) {
	// common response
	offset.CommonResponse = &CommonResponse{
		offset.p.readInt32FromBinaryResponse(),
		offset.p.readInt32FromBinaryResponse(),
	}

	// topics
	topicsNum := offset.p.readInt32FromBinaryResponse()
	for i := 0;i < int(topicsNum);i++ {
		var topic OffsetTopic
		topic.Partitions = make(map[int32]*OffsetPartition)
		topic.Name = offset.p.readStringFromBinaryResponse()
		partitionNum := offset.p.readInt32FromBinaryResponse()
		for j := 0;j < int(partitionNum);j++ {
			var partition OffsetPartition
			partition.Index = offset.p.readInt32FromBinaryResponse()
			partition.CommittedOffset = offset.p.readInt64FromBinaryResponse()
			partition.Metadata = offset.p.readStringFromBinaryResponse()
			partition.ErrorCode = offset.p.readInt16FromBinaryResponse()
			if partition.ErrorCode != 0 {
				return fmt.Errorf("parse offsetFetch response topic error, code: %d", partition.ErrorCode)
			}
			topic.Partitions[partition.Index] = &partition
		}
		offset.Topics[topic.Name] = &topic
	}
	//error code
	offset.ErrorCode = offset.p.readInt16FromBinaryResponse()
	if offset.ErrorCode != 0 {
		return fmt.Errorf("parse offsetFetch response error, code: %d", offset.ErrorCode)
	}

	return nil
}

