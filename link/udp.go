package link

import (
	"errors"
	"miniquic/proto"
	"net"
	"time"
)

type UDPLink struct {
	conn      *net.UDPConn
	remote    *net.UDPAddr
	connected bool
	buf       []byte
}

var (
	ErrPeerNotSet  = errors.New("peer address not set")
	ErrInvalidPeer = errors.New("invalid UDP peer address")
)

func DialUDP(addr string) (*UDPLink, error) {
	ra, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	c, err := net.DialUDP("udp", nil, ra)
	if err != nil {
		return nil, err
	}
	return &UDPLink{
		conn:      c,
		connected: true,
		buf:       make([]byte, proto.HeaderSize+proto.MaxPayload+64),
	}, nil
}

func ListenUDP(addr string) (*UDPLink, error) {
	la, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	c, err := net.ListenUDP("udp", la)
	if err != nil {
		return nil, err
	}
	return &UDPLink{
		conn: c,
		buf:  make([]byte, proto.HeaderSize+proto.MaxPayload+64),
	}, nil
}

func (u *UDPLink) LocalAddr() net.Addr {
	return u.conn.LocalAddr()
}

func (u *UDPLink) SetPeer(addr net.Addr) error {
	remote, ok := addr.(*net.UDPAddr)

	if !ok || remote == nil {
		return ErrInvalidPeer
	}

	peer := *remote
	u.remote = &peer

	return nil
}

func (u *UDPLink) Send(p *proto.Packet) error {
	b, err := p.Encode()
	if err != nil {
		return err
	}

	if u.connected {
		_, err = u.conn.Write(b)
		return err
	}

	if u.remote == nil {
		return ErrPeerNotSet
	}

	_, err = u.conn.WriteToUDP(b, u.remote)
	return err
}

func (u *UDPLink) Recv(timeout time.Duration) (*proto.Packet, net.Addr, error) {
	if err := u.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, nil, err
	}
	for {
		n, addr, err := u.conn.ReadFromUDP(u.buf)

		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return nil, nil, ErrTimeout
			}
			return nil, addr, err
		}

		p, err := proto.Decode(u.buf[:n])
		if err != nil {
			continue
		}

		return p, addr, nil
	}
}

func (u *UDPLink) Close() error {
	return u.conn.Close()
}
