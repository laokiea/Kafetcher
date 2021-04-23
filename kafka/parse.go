package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"
)

// Response interface
type Response interface {
	Parse() // parse response
}

type Parser struct {
	ApiKey        uint16		 // The API key of this request.
	ApiVersion    uint16         // Version for request
	CorrelationId int32          // The client ID string.
	ClientId      string         // random string
	ReqBuffer     *Buffer        // request buffer
	ResBuffer     *Buffer        // response buffer
	ReqBytes      *bytes.Buffer  // bytes.Buffer for request buffer
	ResBytes      *bytes.Buffer  // bytes.Buffer for response buffer
}

// Parser buffer
type Buffer struct {
	buf []byte  // byte slice
	len int     // write/read bytes length
}

// Include common response fields
type CommonResponse struct {
	Size          int32 // response packet size
	CorrelationId int32 // The client ID string.
}

const (
	MetadataApiVersion = 0x01
	OffsetFetchApiVersion = 0x02
	ListOffsetApiVersion = 0x01
	DescribeLogDirsApiVersion = 0x01
)

// New kafka fetcher
func NewFetchParser(ApiKey uint16, ApiVersion uint16) *Parser {
	return &Parser{
		ApiKey: ApiKey,
		ApiVersion: ApiVersion,
		CorrelationId: atomic.LoadInt32(&rc),
		ClientId: NewClientId(9),
		ReqBuffer: NewParserBuffer(BufferSize),
		ResBuffer: NewParserBuffer(BufferSize),
	}
}

// New parser buffer
func NewParserBuffer(size int) *Buffer {
	return &Buffer{
		buf: make([]byte, size),
		len: 0,
	}
}

// OffsetFetch parser
func NewOffsetFetchParser() *Parser {
	// All versions has same fields for OffsetFetch request
	return NewFetchParser(0x09, OffsetFetchApiVersion)
}

// Metadata parser
func NewMetadataParser() *Parser {
	return NewFetchParser(0x03, MetadataApiVersion)
}

// ListOffset parser
func NewListOffsetParser() *Parser {
	return NewFetchParser(0x02, ListOffsetApiVersion)
}

//DescribeLogDirs parser
func NewDescribeLogDirsParser() *Parser {
	return NewFetchParser(0x23, DescribeLogDirsApiVersion)
}

// Generates a random string consists of hex char
func NewClientId(length int) (s string) {
	if length < 0 {
		return
	}
	rand.Seed(time.Now().Unix())
	var b strings.Builder
	for i := 0;i < length;i++ {
		b.WriteString(fmt.Sprintf("%02x", rand.Intn(0xFF)))
	}
	s = b.String()
	return
}

// pre request
func (p *Parser) preRequest() {
	if p.ReqBuffer.len != 0 {
		p.ReqBuffer = NewParserBuffer(BufferSize)
	}
	p.writeCommonFields()
}

// Common fields includes ApiKey, ApiVersion, ClientId etc
func (p *Parser) writeCommonFields() {
	// ApiKey
	p.writeUint16ToBinary(p.ApiKey)
	// ApiVersion
	p.writeUint16ToBinary(p.ApiVersion)
	// Correlation id
	p.writeInt32ToBinary(p.CorrelationId)
	// Client id
	p.writeStringToBinary(p.ClientId)
}

// Convert uint16 v to bytes and write into request buffer
func (p *Parser) writeUint16ToBinary(v uint16) {
	l := p.ReqBuffer.len
	wl := 2
	for i := wl - 1; i >= 0;i-- {
		p.ReqBuffer.buf[l+i] = uint8(v>>(8*(wl-1-i)))
	}
	p.ReqBuffer.len += wl
}

// Convert int32 v to bytes and write into request buffer
func (p *Parser) writeInt32ToBinary(v int32) {
	l := p.ReqBuffer.len
	wl := 4
	for i := wl - 1; i >= 0;i-- {
		p.ReqBuffer.buf[l+i] = uint8(v>>(8*(wl-1-i)))
	}
	p.ReqBuffer.len += wl
}

// Convert int64 v to bytes and write into request buffer
func (p *Parser) writeInt64ToBinary(v int64) {
	l := p.ReqBuffer.len
	wl := 8
	for i := wl - 1; i >= 0;i-- {
		p.ReqBuffer.buf[l+i] = uint8(v>>(8*(wl-1-i)))
	}
	p.ReqBuffer.len += wl
}

// Convert int16 v to bytes and write into request buffer
func (p *Parser) writeInt16ToBinary(v int16) {
	l := p.ReqBuffer.len
	wl := 2
	for i := wl - 1; i >= 0;i-- {
		p.ReqBuffer.buf[l+i] = uint8(v>>(8*(wl-1-i)))
	}
	p.ReqBuffer.len += wl
}

// Convert string v to bytes and write into request buffer
func (p *Parser) writeStringToBinary(v string) {
	if len(v) != 0 {
		p.writeInt16ToBinary(int16(len(v)))
		l := p.ReqBuffer.len
		s := []byte(v)
		for i, b := range s {
			p.ReqBuffer.buf[l+i] = b
		}
		p.ReqBuffer.len += len(s)
	} else {
		p.writeInt16ToBinary(int16(-1))
	}
}

// Write []bytes v into request buffer
func (p *Parser) writePacketToBinary(v []byte) {
	if len(v) != 0 {
		p.writeInt32ToBinary(int32(len(v)))
		l := p.ReqBuffer.len
		for i, b := range v {
			p.ReqBuffer.buf[l+i] = b
		}
		p.ReqBuffer.len += len(v)
	} else {
		p.writeInt16ToBinary(int16(-1))
	}
}

// Read 8 bytes from p.response buffer and convert to int64
func (p *Parser) readInt64FromBinaryResponse() (v int64) {
	var b = make([]byte, 8)
	for i := 0;i < 8;i++ {
		b[i] = p.ResBuffer.buf[p.ResBuffer.len+i]
	}

	p.ResBuffer.len += 8
	buf := bytes.NewBuffer(b)
	_ = binary.Read(buf, binary.BigEndian, &v)

	return
}

// Read 4 bytes from p.response buffer and convert to int32
func (p *Parser) readInt32FromBinaryResponse() (v int32) {
	var b = make([]byte, 4)
	for i := 0;i < 4;i++ {
		b[i] = p.ResBuffer.buf[p.ResBuffer.len+i]
	}

	p.ResBuffer.len += 4
	buf := bytes.NewBuffer(b)
	_ = binary.Read(buf, binary.BigEndian, &v)

	return
}

// Read 2 bytes from p.response buffer and convert to int16
func (p *Parser) readInt16FromBinaryResponse() (v int16) {
	var b = make([]byte, 2)
	for i := 0;i < 2;i++ {
		b[i] = p.ResBuffer.buf[p.ResBuffer.len+i]
	}

	p.ResBuffer.len += 2
	buf := bytes.NewBuffer(b)
	_ = binary.Read(buf, binary.BigEndian, &v)

	return
}

// Read string type from p.response buffer
func (p *Parser) readStringFromBinaryResponse() string {
	// read string length
	l := p.readInt16FromBinaryResponse()
	if l == -1 {
		return ""
	}

	var b = make([]byte, l)
	for i := 0;i < int(l);i++ {
		b[i] = p.ResBuffer.buf[p.ResBuffer.len+i]
	}
	p.ResBuffer.len += int(l)

	return string(b)
}

// read a boolean value(1 byte) from response
func (p *Parser) readBoolFromBinaryResponse() (b byte) {
	b = p.ResBuffer.buf[p.ResBuffer.len]
	p.ResBuffer.len++
	return
}