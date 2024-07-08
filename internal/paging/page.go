package paging

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path"

	"github.com/google/uuid"
	"github.com/tobsdb/tobsdb/pkg"
)

const (
	MAX_PAGE_SIZE    = 4000 // 4KB
	PAGE_HEADER_SIZE = 48
)

type Page struct {
	id uuid.UUID

	Prev uuid.UUID
	Next uuid.UUID

	buf []byte
}

func NewPage(prev_page_id, next_page_id uuid.UUID) *Page {
	return &Page{uuid.New(), prev_page_id, next_page_id, []byte{}}
}

var ERR_INVALID_PAGE_HEADER = errors.New("invalid page headers")

func LoadPageUUID(base string, id uuid.UUID) (*Page, error) { return LoadPage(base, id.String()) }
func LoadPage(base string, id string) (*Page, error) {
	location := path.Join(base, id)
	data, err := os.ReadFile(location)
	if err != nil {
		return nil, err
	}

	page_id, err := uuid.FromBytes(data[0:16])
	if err != nil {
		pkg.FatalLog(ERR_INVALID_PAGE_HEADER, "page ID", err)
	}

	prev_page_id, err := uuid.FromBytes(data[16:32])
	if err != nil {
		pkg.FatalLog(ERR_INVALID_PAGE_HEADER, "previous page ID", err)
	}

	next_page_id, err := uuid.FromBytes(data[32:48])
	if err != nil {
		pkg.FatalLog(ERR_INVALID_PAGE_HEADER, "next page ID", err)
	}

	page_buf := data[PAGE_HEADER_SIZE:]

	if id != page_id.String() {
		pkg.FatalLog("LoadPage", "page id mismatch", id, page_id.String())
	}

	return &Page{page_id, prev_page_id, next_page_id, []byte(page_buf)}, nil
}

// The first 32 bytes are reserved for page links.
// 16 for the previous page id and 16 for the next page.
//
// The rest ({MAX_PAGE_SIZE}) is the page data.
func (page *Page) WriteToFile(base string) error {
	page_id, err := page.id.MarshalBinary()
	if err != nil {
		return err
	}
	prev_page_id, err := page.Prev.MarshalBinary()
	if err != nil {
		return err
	}
	next_page_id, err := page.Next.MarshalBinary()
	if err != nil {
		return err
	}

	if len(page_id) > 16 || len(prev_page_id) > 16 || len(next_page_id) > 16 {
		pkg.FatalLog("Page Link IDs too large", prev_page_id, next_page_id)
	}

	location := path.Join(base, page.id.String())

	buf := []byte{}
	buf = append(buf, page_id...)
	buf = append(buf, prev_page_id...)
	buf = append(buf, next_page_id...)
	buf = append(buf, page.buf...)

	return os.WriteFile(location, buf, 0644)
}

var (
	ERR_PAGE_OVERFLOW = errors.New("page overflow")
	ERR_MAX_DATA_SIZE = errors.New("maximum data size exceeded")
)

const block_header_size = 2

func (p *Page) Push(data []byte) error {
	buf_size := len(p.buf)
	data_size := len(data)

	// check data size is less than uint16::MAX
	if int(uint16(data_size)) < data_size {
		return ERR_MAX_DATA_SIZE
	}

	// +2 bytes to account for the header
	if data_size+block_header_size+buf_size > MAX_PAGE_SIZE {
		return ERR_PAGE_OVERFLOW
	}

	// prefix each data block with its size
	header := make([]byte, block_header_size)
	binary.BigEndian.PutUint16(header, uint16(data_size))
	p.buf = append(p.buf, header...)
	p.buf = append(p.buf, data...)

	return nil
}

func (p *Page) NewReader() *PageReader {
	return &PageReader{nil, p, nil}
}

type PageReader struct {
	r   *bufio.Reader
	p   *Page
	Buf []byte
}

func (r *PageReader) ReadNext() bool {
	if r.r == nil {
		r.r = bufio.NewReader(bytes.NewReader(r.p.buf[:]))
	}

	// decode the data block size from the header
	header := make([]byte, block_header_size)
	_, err := io.ReadFull(r.r, header)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			pkg.FatalLog("ReadNext", err)
		}
		return false
	}
	size := binary.BigEndian.Uint16(header)

	buf := make([]byte, size)
	_, err = io.ReadFull(r.r, buf)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			pkg.FatalLog("ReadNext", err)
		}
		return false
	}
	r.Buf = buf
	return true
}
