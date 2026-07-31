package tcp

import (
	"io"
	"net"
)

const readBuffSize = 4 * 1024

type CommonOptions struct {
	net.KeepAliveConfig
	ReadBuffSize int
}

type ProcessBytes func(conn *net.TCPConn, data []byte, eof bool, err error) bool

type streamReader struct {
	opts         CommonOptions
	processBytes ProcessBytes
}

func (s *streamReader) ListenAndServe(addr net.TCPAddr) error {
	lis, err := net.ListenTCP("tcp", &addr)

	if err != nil {
		return err
	}

	return s.serve(lis)
}

func (s *streamReader) serve(lis *net.TCPListener) error {
	defer lis.Close()

	for {
		conn, err := lis.AcceptTCP()

		if err != nil {
			return err
		}

		conn.SetKeepAliveConfig(s.opts.KeepAliveConfig)
		conn.SetReadBuffer(s.opts.ReadBuffSize)

		go s.read(conn)
	}
}

func (s *streamReader) read(conn *net.TCPConn) {
	defer conn.Close()

	buff := make([]byte, s.opts.ReadBuffSize)

	for {
		bytesRead, err := conn.Read(buff)
		data := buff[:bytesRead]

		if err != nil {
			if err == io.EOF {
				s.processBytes(conn, data, true, nil)
			} else {
				s.processBytes(conn, data, false, err)
			}

			return
		}

		close := s.processBytes(conn, data, false, nil)

		if close {
			return
		}
	}
}

type IStreamReader interface {
	ListenAndServe(addr net.TCPAddr) error
}

func CreateStreamReader(opts CommonOptions, processBytes ProcessBytes) IStreamReader {
	if opts.ReadBuffSize == 0 {
		opts.ReadBuffSize = readBuffSize
	}
	
	s := &streamReader{opts: opts, processBytes: processBytes}

	return s
}
