package pkg

import (
	"encoding/binary"
	"io"
	"net"
)

const chunk_size = 1024 * 2

func ConnReadBytes(conn net.Conn) ([]byte, error) {
	var buf []byte
	chunk := make([]byte, chunk_size)
	var size uint32
	for {
		clear(chunk)
		n, err := conn.Read(chunk)
		if err == io.EOF {
			if size > 0 && len(buf) >= int(size) {
				break
			}
			continue
		}

		if err != nil {
			return nil, err
		}

		if size == 0 {
			size = binary.BigEndian.Uint32(chunk[:4])
			buf = append(buf, chunk[4:n]...)
		} else {
			buf = append(buf, chunk[:n]...)
		}
		if size > 0 && len(buf) >= int(size) {
			break
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
