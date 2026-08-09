package tcp

import (
	"io"
	"net"
)

const readBuffSize = 4 * 1024

type CommonOptions struct {
	// KeepAliveConfig is user for every accepted connection.
	net.KeepAliveConfig
	// ReadBuffSize set's size of application buffer passed to processBytes (reused for subsequent reads of same connection) and also underlaying OS receive buffer size (conn.SetReadBuffer). It defaults to 4096 when not passed explicitly or set to 0 - that should be aligned with common OS page size.
	ReadBuffSize int
}

// Function can be used for aribtrary data processing - e.g. to frame data stream into discrete, formatted messages. Data buffer size is determined by the CommonOptions.ReadBuffSize option and it reads from a connection until EOF or any error is encountered. Also boolean can be returned from the function to terminate read loop when required - e.g. based on the content of the stream. In case eof true, err is nil and vice versa - in case of non-EOF error, err will be non-nil and eof false.
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
	// ListenAndServe blocks by default, but can be trigged in it's own dedicated goroutine when required. However accepted connections are always read in their own goroutines as appropriate.
	ListenAndServe(addr net.TCPAddr) error
}

// Create stream reader to process bytes of incoming TCP connections. opts.ReadBuffSize set's size of application buffer passed to processBytes (reused for subsequent reads of same connection) and also underlaying OS receive buffer size (conn.SetReadBuffer). It defaults to 4096 when not passed explicitly or set to 0 - that should be aligned with common OS page size. Every connection is processed in it's own separate goroutine.
func CreateStreamReader(opts CommonOptions, processBytes ProcessBytes) IStreamReader {
	if opts.ReadBuffSize == 0 {
		opts.ReadBuffSize = readBuffSize
	}
	
	s := &streamReader{opts: opts, processBytes: processBytes}

	return s
}
