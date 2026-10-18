package link

import (
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
		remote:    ra,
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
		return nil
	}

	_, err = u.conn.WriteToUDP(b, u.remote)
	return err
}

func (u *UDPLink) Recv(timeout time.Duration) (*proto.Packet, error) {
	u.conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		n, addr, err := u.conn.ReadFromUDP(u.buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return nil, ErrTimeout
			}
			return nil, err
		}

		p, err := proto.Decode(u.buf[:n])
		if err != nil {
			continue
		}
		if u.remote == nil {
			u.remote = addr
		}
		return p, nil
	}
}

func (u *UDPLink) Close() error {
	return u.conn.Close()
}
