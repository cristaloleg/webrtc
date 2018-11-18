package network

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/pions/webrtc/internal/srtp"
	"github.com/pions/webrtc/pkg/rtp"
)

// TODO: Migrate to srtp.Conn

func (m *Manager) CreateContextSRTP(serverWriteKey, clientWriteKey []byte, profile string) error {
	var err error
	m.srtpInboundContextLock.Lock()
	m.srtpInboundContext, err = srtp.CreateContext(
		serverWriteKey[0:16],
		serverWriteKey[16:],
		profile)
	m.srtpInboundContextLock.Unlock()
	if err != nil {
		return errors.New("failed to build inbound SRTP context")
	}

	m.srtpOutboundContextLock.Lock()
	m.srtpOutboundContext, err = srtp.CreateContext(
		clientWriteKey[0:16],
		clientWriteKey[16:],
		profile)
	m.srtpOutboundContextLock.Unlock()
	if err != nil {
		return errors.New("failed to build outbound SRTP context")
	}

	return nil
}

func (m *Manager) handleSRTP(buffer []byte) {
	m.srtpInboundContextLock.Lock()
	defer m.srtpInboundContextLock.Unlock()
	if m.srtpInboundContext == nil {
		fmt.Printf("Got RTP packet but no SRTP Context to handle it \n")
		return
	}

	if len(buffer) > 4 {
		var rtcpPacketType uint8

		r := bytes.NewReader([]byte{buffer[1]})
		if err := binary.Read(r, binary.BigEndian, &rtcpPacketType); err != nil {
			fmt.Println("Failed to check packet for RTCP")
			return
		}

		if rtcpPacketType >= 192 && rtcpPacketType <= 223 {
			decrypted, err := m.srtpInboundContext.DecryptRTCP(buffer)
			if err != nil {
				fmt.Println(err)
				fmt.Println(decrypted)
				return
			}
			return
		}
	}

	packet := &rtp.Packet{}
	if err := packet.Unmarshal(buffer); err != nil {
		fmt.Println("Failed to unmarshal RTP packet")
		return
	}

	if ok := m.srtpInboundContext.DecryptRTP(packet); !ok {
		fmt.Println("Failed to decrypt packet")
		return
	}

	bufferTransport := m.bufferTransports[packet.SSRC]
	if bufferTransport == nil {
		bufferTransport = m.bufferTransportGenerator(packet.SSRC, packet.PayloadType)
		if bufferTransport == nil {
			return
		}
		m.bufferTransports[packet.SSRC] = bufferTransport
	}

	select {
	case bufferTransport <- packet:
	default:
	}

}
