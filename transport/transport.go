package transport

import (
	"errors"
	"io"
	"time"

	"miniquic/link"
	"miniquic/proto"
)

const (
	timeout  = 500 * time.Millisecond
	maxTries = 10
)

func Send(l link.Link, r io.Reader) error {
	for i := 0; i < maxTries; i++ {
		if err := l.Send(&proto.Packet{Flags: proto.FlagSYN}); err != nil {
			return err
		}

		p, err := l.Recv(timeout)
		if err == link.ErrTimeout {
			continue
		}
		if err != nil {
			return err
		}

		if p.Flags.Has(proto.FlagSYN) &&
			p.Flags.Has(proto.FlagACK) {
			break
		}

		if i == maxTries-1 {
			return errors.New("handshake failed")
		}
	}

	_ = l.Send(&proto.Packet{
		Flags: proto.FlagACK,
		Ack:   1,
	})

	buf := make([]byte, proto.MaxPayload)
	var seq uint32

	for {
		n, readErr := io.ReadFull(r, buf)
		if readErr != nil &&
			readErr != io.EOF &&
			readErr != io.ErrUnexpectedEOF {
			return readErr
		}

		if n > 0 {
			pkt := &proto.Packet{
				Seq_N:   seq,
				Flags:   proto.FlagDATA,
				Payload: append([]byte(nil), buf[:n]...),
			}

			acked := false

			for i := 0; i < maxTries; i++ {
				if err := l.Send(pkt); err != nil {
					return err
				}
				for {
					ack, err := l.Recv(timeout)
					if err == link.ErrTimeout {
						break
					}
					if err != nil {
						return err
					}

					if ack.Flags.Has(proto.FlagACK) &&
						ack.Ack == seq+1 {
						acked = true
						break
					}
				}

				if acked {
					break
				}
			}
			if !acked {
				return errors.New("data transfer failed")
			}

			seq++
		}

		if readErr == io.EOF ||
			readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	fin := &proto.Packet{
		Seq_N: seq,
		Flags: proto.FlagFIN,
	}

	for i := 0; i < maxTries; i++ {
		if err := l.Send(fin); err != nil {
			return err
		}

		p, err := l.Recv(timeout)
		if err == link.ErrTimeout {
			continue
		}
		if err != nil {
			return err
		}

		if p.Flags.Has(proto.FlagFIN) &&
			p.Flags.Has(proto.FlagACK) {
			return nil
		}
	}

	return errors.New("connection close failed")
}

func Receive(l link.Link, w io.Writer) error {
	var synack = &proto.Packet{
		Flags: proto.FlagSYN | proto.FlagACK,
		Ack:   1,
	}

	for {
		p, err := l.Recv(30 * time.Second)
		if err != nil {
			return err
		}
		if p.Flags.Has(proto.FlagSYN) {
			if err := l.Send(synack); err != nil {
				return err
			}
			break
		}
	}

	var expected uint32

	for {
		p, err := l.Recv(30 * time.Second)
		if err != nil {
			return err
		}

		switch {
		case p.Flags.Has(proto.FlagSYN):
			if err := l.Send(synack); err != nil {
				return err
			}

		case p.Flags.Has(proto.FlagDATA):
			if p.Seq_N == expected {
				if _, err := w.Write(p.Payload); err != nil {
					return err
				}
				expected++
			}

			if err := l.Send(&proto.Packet{
				Flags: proto.FlagACK,
				Ack:   expected,
			}); err != nil {
				return err
			}

		case p.Flags.Has(proto.FlagFIN):
			if p.Seq_N != expected {
				continue
			}

			return l.Send(&proto.Packet{
				Seq_N: p.Seq_N,
				Ack:   expected + 1,
				Flags: proto.FlagFIN | proto.FlagACK,
			})
		}
	}
}
