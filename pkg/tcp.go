package pkg

import (
	"encoding/binary"
	"io"
	"net"
)

const chunk_size = 1024 * 2

func ConnReadBytes(conn net.Conn) ([]byte, error) {
	var buf []byte
	read_complete := false
	tmp_buf := make([]byte, chunk_size)
	for !read_complete {
		clear(tmp_buf)
		n, err := conn.Read(tmp_buf)
		if err == io.EOF && (n > 0 || len(buf) > 0) {
			read_complete = true
		}

		if err == io.EOF {
			continue
		}

		if err != nil {
			return nil, err
		}

		buf = append(buf, tmp_buf[:n]...)
		if n < chunk_size {
			read_complete = true
		}
	}
	return buf, nil
}

func ConnWriteBytes(conn net.Conn, buf []byte) (int, error) {
	n := uint32(len(buf))
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, n)
	buf = append(header, buf...)

	return conn.Write(buf)
}
