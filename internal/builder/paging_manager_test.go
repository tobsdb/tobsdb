package builder_test

import (
	"testing"

	"github.com/tobsdb/tobsdb/internal/builder"
	"gotest.tools/assert"
)

func TestPagingManager(t *testing.T) {
	builder.GobRegisterTypes()
	pm := builder.NewPagingManager(&builder.Table{
		Schema: &builder.Schema{Tdb: &builder.TobsDB{
			WriteSettings: &builder.TDBWriteSettings{},
		}},
	})

	assert.NilError(t, pm.Insert(1, builder.TDBTableRow{"a": 1, "b": 2}))
	assert.NilError(t, pm.Insert(2, builder.TDBTableRow{"c": 3, "d": 4}))

	m, err := pm.ParsePage()
	assert.NilError(t, err)
	assert.Equal(t, m.Len(), 2)
	assert.DeepEqual(t, m.Idx[1], builder.TDBTableRow{"a": 1, "b": 2})
	assert.DeepEqual(t, m.Idx[2], builder.TDBTableRow{"c": 3, "d": 4})
}
