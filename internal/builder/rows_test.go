package builder_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	. "github.com/tobsdb/tobsdb/internal/builder"
	"gotest.tools/assert"
)

func newTestTDBTableRows(t *testing.T, size int, inMem bool) *TDBTableRows {
	r := NewTDBTableRows(&Table{
		Name: "test_rows", Schema: &Schema{
			Name: "test_rows",
			Tdb: &TobsDB{
				WriteSettings: &TDBWriteSettings{InMem: inMem, WritePath: "db.tdb"},
			},
		},
	}, TDBTableIndexes{}, TDBTablePrimaryIndexes{})
	for i := 0; i < size; i++ {
		ok:= r.Insert(i, TDBTableRow{SYS_PRIMARY_KEY: i})
		assert.Assert(t, ok, fmt.Sprintf("failed to insert %d", i))
	}
	return r
}

func TestTDBTableRows(t *testing.T) {
	t.Run("Insert", func(t *testing.T) {
		wg := sync.WaitGroup{}

		r := NewTDBTableRows(&Table{}, TDBTableIndexes{}, TDBTablePrimaryIndexes{})

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

	const TEST_SIZE = 10

	t.Run("Get", func(t *testing.T) {
		r := newTestTDBTableRows(t, TEST_SIZE, true)
		for i := 0; i < TEST_SIZE; i++ {
			assert.Assert(t, r.Has(i))
			row, _ := r.Get(i)
			assert.DeepEqual(t, row, TDBTableRow{SYS_PRIMARY_KEY: i})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		r := newTestTDBTableRows(t, TEST_SIZE, true)
		i := rand.Intn(TEST_SIZE)
		assert.Assert(t, r.Has(i))
		ok := r.Delete(i)
		assert.Assert(t, ok)
	})

	t.Run("Replace", func(t *testing.T) {
		r := newTestTDBTableRows(t, TEST_SIZE, true)
		i := rand.Intn(TEST_SIZE)
		assert.Assert(t, r.Has(i))
		new_row := TDBTableRow{SYS_PRIMARY_KEY: i + TEST_SIZE}
		r.Replace(i, new_row)

		row, _ := r.Get(i)
		assert.DeepEqual(t, row, new_row)
	})
}

func TestTDBTableRowsRecords(t *testing.T) {
	r := newTestTDBTableRows(t, 500, true)
	ch := r.Records()
	i := 0
	for rec := range ch {
		assert.DeepEqual(t, rec.Val, TDBTableRow{SYS_PRIMARY_KEY: i})
		i++
	}
	assert.Equal(t, i, 500)
}
