package helpers

import "net"

func AddrEqual(a, b net.Addr) bool {
	aa, ok := a.(*net.UDPAddr)
	if !ok {
		return false
	}

	ba, ok := b.(*net.UDPAddr)
	if !ok {
		return false
	}

	return aa.IP.Equal(ba.IP) && aa.Port == ba.Port && aa.Zone == ba.Zone
}
