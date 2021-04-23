package kafka

import "fmt"

// DescribeLogDirs topic partition format
type DescribeLogDirsPartition struct {
	Index       int32 // The partition index.
	Size        int64 // The size of the log segments in this partition in bytes.
	OffsetLag   int64 // The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition) or current replica's LEO (if it is the future log for the partition)
	IsFutureKey byte  // True if this log is created by AlterReplicaLogDirsRequest and will replace the current log of the replica in the future.
}

//DescribeLogDirs topics format
type DescribeLogDirsTopic struct {
	Name       string // The topic name.
	Partitions map[int32]*DescribeLogDirsPartition
}

// result format
type DescribeLogDirsResult struct {
	ErrorCode int16 //The error code, or 0 if there was no error.
	LogDir    string //	The absolute log directory path.
	Topics    map[string]*DescribeLogDirsTopic // Each topic.
}

// DescribeLogDirs response format
type DescribeLogDirs struct {
	p              *Parser
	ThrottleTimeMs int32 //The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	Results        map[string]*DescribeLogDirsResult //	The log directories.
	*CommonResponse // common response
}

// New describeLogDirs offset
func NewDescribeLogDirs() *DescribeLogDirs {
	return &DescribeLogDirs{
		Results: make(map[string]*DescribeLogDirsResult),
	}
}


// Parse
func (d *DescribeLogDirs) Parse() (err error) {
	// common response
	d.CommonResponse = &CommonResponse{
		d.p.readInt32FromBinaryResponse(),
		d.p.readInt32FromBinaryResponse(),
	}

	d.ThrottleTimeMs = d.p.readInt32FromBinaryResponse()
	resultNum := d.p.readInt32FromBinaryResponse()
	for i := 0;i < int(resultNum);i++ {
		var result DescribeLogDirsResult
		result.Topics = make(map[string]*DescribeLogDirsTopic)
		result.ErrorCode = d.p.readInt16FromBinaryResponse()
		if result.ErrorCode != 0 {
			return fmt.Errorf("parse describeLogDirs response error, code: %d", result.ErrorCode)
		}
		result.LogDir = d.p.readStringFromBinaryResponse()
		topicNnm := d.p.readInt32FromBinaryResponse()
		for j := 0;j < int(topicNnm);j++ {
			var topic DescribeLogDirsTopic
			topic.Partitions = make(map[int32]*DescribeLogDirsPartition)
			topic.Name = d.p.readStringFromBinaryResponse()
			partitionNum := d.p.readInt32FromBinaryResponse()
			for l := 0;l < int(partitionNum);l++ {
				var partition DescribeLogDirsPartition
				partition.Index = d.p.readInt32FromBinaryResponse()
				partition.Size = d.p.readInt64FromBinaryResponse()
				partition.OffsetLag = d.p.readInt64FromBinaryResponse()
				partition.IsFutureKey = d.p.readBoolFromBinaryResponse()
				topic.Partitions[partition.Index] = &partition
			}
			result.Topics[topic.Name] = &topic
		}
		d.Results[result.LogDir] = &result
	}
	return nil
}
