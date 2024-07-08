package paging_test

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/google/uuid"
	"github.com/tobsdb/tobsdb/internal/builder"
	"github.com/tobsdb/tobsdb/internal/paging"
	"gotest.tools/assert"
)

func TestPageRead(t *testing.T) {
	builder.GobRegisterTypes()
	p := paging.NewPage(uuid.Nil, uuid.Nil)

	for i := 0; i < 50; i++ {
		m := map[string]int{"test": i}
		buf := bytes.Buffer{}
		err := gob.NewEncoder(&buf).Encode(m)
		assert.NilError(t, err)
		p.Push(buf.Bytes())
	}

	r := p.NewReader()
	i := 0
	for r.ReadNext() {
		var m map[string]int
		err := gob.NewDecoder(bytes.NewReader(r.Buf)).Decode(&m)
		assert.NilError(t, err)
		assert.Equal(t, m["test"], i)
		i++
	}
}
