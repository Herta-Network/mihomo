package trojan

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/metacubex/mihomo/transport/socks5"
)

func TestPacketLargePayloadAndTruncation(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	left.SetDeadline(time.Now().Add(3 * time.Second))
	right.SetDeadline(time.Now().Add(3 * time.Second))
	target := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5353}
	done := make(chan error, 1)
	go func() {
		_, err := WritePacket(right, socks5.ParseAddrToSocksAddr(target), bytes.Repeat([]byte{7}, 60000))
		if err == nil {
			_, err = WritePacket(right, socks5.ParseAddrToSocksAddr(target), nil)
		}
		if err == nil {
			_, err = WritePacket(right, socks5.ParseAddrToSocksAddr(target), []byte("next"))
		}
		done <- err
	}()
	conn := NewPacketConn(left)
	var short [1]byte
	n, addr, err := conn.ReadFrom(short[:])
	if err != nil || n != 1 || short[0] != 7 || addr.String() != target.String() {
		t.Fatalf("n=%d addr=%v err=%v", n, addr, err)
	}
	data, release, addr, err := conn.WaitReadFrom()
	if release != nil {
		release()
	}
	if err != nil || len(data) != 0 || addr.String() != target.String() {
		t.Fatal("truncated bytes became another datagram", err)
	}
	data, release, _, err = conn.WaitReadFrom()
	if release != nil {
		defer release()
	}
	if err != nil || string(data) != "next" {
		t.Fatal("packet boundary lost", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
