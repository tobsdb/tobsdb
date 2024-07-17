package builder_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	. "github.com/tobsdb/tobsdb/internal/builder"
	"gotest.tools/assert"
)

const TEST_SIZE = 10

func newTestTDBTableRows() *TDBTableRows {
	r := NewTDBTableRows(&Table{}, TDBTableIndexes{})
	for i := 0; i < TEST_SIZE; i++ {
		r.Insert(i, TDBTableRow{SYS_PRIMARY_KEY: i})
	}
	return r
}

func TestTDBTableRows(t *testing.T) {
	t.Run("Insert", func(t *testing.T) {
		wg := sync.WaitGroup{}

		r := NewTDBTableRows(&Table{}, TDBTableIndexes{})

		for i := 0; i < 10; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				if r.Has(i) {
					assert.Assert(t, false)
				}
				ok := r.Insert(i, TDBTableRow{SYS_PRIMARY_KEY: i})
				assert.Assert(t, ok, fmt.Sprintf("failed to insert %d", i))
			}()
		}

		wg.Wait()

		// ensure duplicates fail
		for i := 0; i < 10; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				ok := r.Insert(i, TDBTableRow{SYS_PRIMARY_KEY: i})
				assert.Assert(t, !ok)
			}()
		}

		wg.Wait()
	})

	t.Run("Get", func(t *testing.T) {
		r := newTestTDBTableRows()
		for i := 0; i < TEST_SIZE; i++ {
			assert.Assert(t, r.Has(i))
			row, _ := r.Get(i)
			assert.DeepEqual(t, row, TDBTableRow{SYS_PRIMARY_KEY: i})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		r := newTestTDBTableRows()
		i := rand.Intn(TEST_SIZE)
		assert.Assert(t, r.Has(i))
		ok := r.Delete(i)
		assert.Assert(t, ok)
	})

	t.Run("Replace", func(t *testing.T) {
		r := newTestTDBTableRows()
		i := rand.Intn(TEST_SIZE)
		assert.Assert(t, r.Has(i))
		new_row := TDBTableRow{SYS_PRIMARY_KEY: i + TEST_SIZE}
		r.Replace(i, new_row)

		row, _ := r.Get(i)
		assert.DeepEqual(t, row, new_row)
	})
}
