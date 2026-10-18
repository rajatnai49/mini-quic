package link

import (
	"errors"
	"miniquic/proto"
	"time"
)

var (
	ErrTimeout = errors.New("recv timeout")
	ErrClosed = errors.New("link closed")
)

type Link interface {
	Send(p *proto.Packet) error
	Recv(timeout time.Duration) (*proto.Packet, error)
	Close() error
}
