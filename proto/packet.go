package proto

import (
	"encoding/binary"
	"errors"
)

const (
	HeaderSize = 11
	MaxPayload = 1200
)

type Flags uint8

const (
	FlagSYN Flags = 1 << iota
	FlagACK
	FlagFIN
	FlagDATA
)

type Packet struct {
	Seq_N   uint32
	Ack     uint32
	Flags   Flags
	Payload []byte
}

var (
	ErrTooShort    = errors.New("packet is shorter")
	ErrBadLen      = errors.New("length field does not match")
	ErrPayloadSize = errors.New("payload too large")
)

func (p *Packet) Encode() ([]byte, error) {
	if len(p.Payload) > MaxPayload {
		return nil, ErrPayloadSize
	}

	buf := make([]byte, HeaderSize+len(p.Payload))

	binary.BigEndian.PutUint32(buf[0:4], p.Seq_N)
	binary.BigEndian.PutUint32(buf[4:8], p.Ack)
	buf[8] = byte(p.Flags)
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(p.Payload)))
	copy(buf[HeaderSize:], p.Payload)
	return buf, nil
}

func Decode(b []byte) (*Packet, error) {
	if len(b) < HeaderSize {
		return nil, ErrTooShort
	}
	n := int(binary.BigEndian.Uint16(b[9:11]))
	if len(b)-HeaderSize != n {
		return nil, ErrBadLen
	}
	p := &Packet{
		Seq_N: binary.BigEndian.Uint32(b[0:4]),
		Ack:   binary.BigEndian.Uint32(b[4:8]),
		Flags: Flags(b[8]),
	}
	if n > 0 {
		p.Payload = make([]byte, n)
		copy(p.Payload, b[HeaderSize:])
	}
	return p, nil
}

func (f Flags) Has(flag Flags) bool {
    return f&flag != 0
}
