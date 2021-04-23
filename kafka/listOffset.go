package kafka

import (
	"fmt"
)

// list offset partition format
type ListOffsetPartition struct {
	Index           int32 // partition index
	Timestamp       int64 // The timestamp associated with the returned offset.
	Offset          int64 // max number of partition's offset
	ErrorCode       int16  // error code
}

// list offset topic format
type ListOffsetTopic struct {
	Name       string
	Partitions map[int32]*ListOffsetPartition
}

// list offset response format
type ListOffset struct {
	p      *Parser
	Topics map[string]*ListOffsetTopic
	*CommonResponse
}

// New list offset
func NewListOffset() *ListOffset {
	return &ListOffset{
		Topics: make(map[string]*ListOffsetTopic),
	}
}

// Parse listOffset response
func (list *ListOffset) Parse() (err error) {
	// common response
	list.CommonResponse = &CommonResponse{
		list.p.readInt32FromBinaryResponse(),
		list.p.readInt32FromBinaryResponse(),
	}
	// topics
	topicsNum := list.p.readInt32FromBinaryResponse()
	for i := 0;i < int(topicsNum);i++ {
		var topic ListOffsetTopic
		topic.Partitions = make(map[int32]*ListOffsetPartition)
		topic.Name = list.p.readStringFromBinaryResponse()
		partitionNum := list.p.readInt32FromBinaryResponse()
		for j := 0;j < int(partitionNum);j++ {
			var partition ListOffsetPartition
			partition.Index = list.p.readInt32FromBinaryResponse()
			partition.ErrorCode = list.p.readInt16FromBinaryResponse()
			if partition.ErrorCode != 0 {
				return fmt.Errorf("parse listoffset response topic error, code: %d", partition.ErrorCode)
			}
			partition.Timestamp = list.p.readInt64FromBinaryResponse()
			partition.Offset = list.p.readInt64FromBinaryResponse()
			topic.Partitions[partition.Index] = &partition
		}
		list.Topics[topic.Name] = &topic
	}
	return nil
}