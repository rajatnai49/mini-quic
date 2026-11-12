package transport

import (
	"errors"
	"math/rand/v2"
	"miniquic/helpers"
	"miniquic/link"
	"miniquic/proto"
	"time"
)

type State uint8

const (
	StateClosed State = iota
	StateListen
	StateSynSent
	StateSynReceived
	StateSynEstablished
)

const (
	attempt_timeout   = 500 * time.Millisecond
	max_attempt       = 10
	half_open_timeout = 30 * time.Second
)

func client_handshake(l link.Link) error {
	current_state := StateClosed
	client_isn := rand.Uint32N(10000)
	var recv_server_isn uint32

	for attempt := 1; attempt < max_attempt; attempt++ {
		if err := l.Send(&proto.Packet{Flags: proto.FlagSYN, Seq: client_isn}); err != nil {
			return err
		}
		current_state = StateSynSent
		dead_line := time.Now().Add(attempt_timeout)

		for {
			remaining := time.Until(dead_line)
			if remaining <= 0 {
				break
			}

			p, _, err := l.Recv(remaining)
			if err == link.ErrTimeout {
				break
			}
			if err != nil {
				return err
			}
			if p.Flags == proto.FlagACK|proto.FlagSYN && p.Ack == client_isn+1 {
				recv_server_isn = p.Seq
				current_state = StateSynEstablished
				break
			}
		}

		if current_state == StateSynEstablished {
			if err := l.Send(&proto.Packet{Flags: proto.FlagACK, Seq: client_isn + 1, Ack: uint32(recv_server_isn) + 1}); err != nil {
				return err
			}
			return nil
		}
	}

	return errors.New("client handshake error after max attempts")
}

func server_handshake(l link.Link) error {
	current_state := StateListen
	var recv_client_isn uint32

	for {
		p, addr, err := l.Recv(30 * time.Second)

		if err == link.ErrTimeout {
			continue
		}
		if err != nil {
			return err
		}

		if p.Flags == proto.FlagSYN {
			if err := l.SetPeer(addr); err != nil {
				return err
			}

			server_isn := rand.Uint32N(10000)
			recv_client_isn = p.Seq
			if err := l.Send(&proto.Packet{Flags: proto.FlagSYN | proto.FlagACK, Seq: server_isn, Ack: recv_client_isn + 1}); err != nil {
				return err
			}
			current_state = StateSynReceived

			half_open_deadline := time.Now().Add(half_open_timeout)

			for {
				remaining := time.Until(half_open_deadline)
				if remaining <= 0 {
					break
				}

				p, ack_addr, err := l.Recv(remaining)
				if err == link.ErrTimeout {
					break
				}
				if err != nil {
					return err
				}
				if !helpers.AddrEqual(addr, ack_addr) {
					continue
				}

				if p.Flags == proto.FlagSYN && p.Seq == recv_client_isn {
					if err := l.Send(&proto.Packet{Flags: proto.FlagSYN | proto.FlagACK, Seq: server_isn, Ack: recv_client_isn + 1}); err != nil {
						return err
					}
					continue
				} else if p.Flags == proto.FlagACK {
					if p.Ack == server_isn+1 && p.Seq == recv_client_isn+1 {
						current_state = StateSynEstablished
						break
					}
				}
			}

			if current_state == StateSynEstablished {
				break
			}
			current_state = StateListen
		}
	}
	return nil
}
