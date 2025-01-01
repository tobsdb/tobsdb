package builder

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/tobsdb/tobsdb/internal/auth"
	"github.com/tobsdb/tobsdb/internal/parser"
	"github.com/tobsdb/tobsdb/internal/props"
	"github.com/tobsdb/tobsdb/internal/types"
	"github.com/tobsdb/tobsdb/pkg"
)

type SchemaAccess struct {
	UserId string
	Role   auth.TdbUserRole
}

func userAccess(u *auth.TdbUser) func(a SchemaAccess) bool {
	return func(a SchemaAccess) bool { return a.UserId == u.Id }
}

// Maps table name to its saved data
type TDBData = pkg.Map[string, *TDBTableRows]

func NewTDBData(data TDBData, tables pkg.Map[string, *Table]) TDBData {
	if data == nil {
		data = make(TDBData)
	}

	for _, t := range tables {
		if data.Has(t.Name) {
			continue
		}
		indexes := make(TDBTableIndexes)
		for _, f := range t.Fields.Idx {
			if f.IndexLevel() < IndexLevelUnique {
				continue
			}

			indexes.Set(f.Name, &TDBTableIndexMap{
				locker: sync.RWMutex{},
				Map:    make(map[string]int),
			})
		}
		data.Set(t.Name, NewTDBTableRows(t, indexes, TDBTablePageRefs{}))
	}

	return data
}

func TDBDataApplySnapshot(d, snapshot TDBData) {
	for k, v := range snapshot {
		d.Get(k).ApplySnapshot(v)
	}
}

type Schema struct {
	Tables *pkg.InsertSortMap[string, *Table]
	// table_name -> row_id -> field_name -> value
	Data TDBData `json:"-"`
	Name string

	locker sync.RWMutex

	WriteTicker *time.Ticker `json:"-"`
	LastChange  time.Time    `json:"-"`

	users []SchemaAccess

	Tdb *TobsDB `json:"-"`

	parent *Schema
}

func (s *Schema) NewSnapshot() *Schema {
	snapshot := &Schema{
		Tables:     pkg.NewInsertSortMap[string, *Table](),
		Data:       NewTDBData(s.Data, s.Tables.Idx),
		Name:       s.Name,
		users:      make([]SchemaAccess, len(s.users)),
		LastChange: s.LastChange,
		parent:     s,
	}
	copy(snapshot.users, s.users)

	for _, name := range s.Tables.Sorted {
		snapshot.SnapshotTable(name)
	}
    return snapshot
}

func (s *Schema) ApplySnapshot(snapshot *Schema) error {
    if snapshot.LastChange.After(s.LastChange) {
        for name, t := range s.Tables.Idx {
            t.ApplySnapshot(snapshot.Tables.Get(name))
        }
        TDBDataApplySnapshot(s.Data, snapshot.Data)
        s.LastChange = snapshot.LastChange
    }
    return nil
}

func (s *Schema) SnapshotTable(name string) {
	if !s.parent.Tables.Has(name) {
		panic(fmt.Errorf("table %s does not exist", name))
	}
	if s.Tables.Has(name) {
		return
	}
	parent := s.parent.Tables.Get(name)
	t := parent.NewSnapshot()
	t.Schema = s
	s.Tables.Push(t.Name, t)
	row := s.Data.Get(t.Name)
	indexes := pkg.Map[string, *TDBTableIndexMap]{}
	for k := range row.Indexes {
		indexes.Set(k, &TDBTableIndexMap{Map: pkg.Map[string, int]{}})
	}
	s.Data.Set(t.Name, NewTDBTableRows(t, indexes, pkg.Map[int, string]{}))
}

func (s *Schema) InMem() bool {
	if s.Tdb == nil {
		return true
	}
	return s.Tdb.WriteSettings.InMem
}

func (s *Schema) AddUser(u *auth.TdbUser, r auth.TdbUserRole) error {
	if slices.ContainsFunc(s.users, userAccess(u)) {
		return fmt.Errorf("User %s already has access", u.Id)
	}
	s.users = append(s.users, SchemaAccess{u.Id, r})
	return nil
}

func (s *Schema) RemoveUser(u *auth.TdbUser) error {
	idx := slices.IndexFunc(s.users, userAccess(u))
	if idx == -1 {
		return fmt.Errorf("User %s does not have access", u.Id)
	}
	s.users = slices.Delete(s.users, idx, idx+1)
	return nil
}

// returns user role on schema or -1 if user has no access
func (s *Schema) CheckUserAccess(u *auth.TdbUser) auth.TdbUserRole {
	if u.IsRoot {
		return auth.TdbUserRoleAdmin
	}
	idx := slices.IndexFunc(s.users, userAccess(u))
	if idx == -1 {
		return auth.TdbUserRole(-1)
	}
	return s.users[idx].Role
}

func (s *Schema) UserHasClearance(u *auth.TdbUser, r auth.TdbUserRole) bool {
	a := s.CheckUserAccess(u)
	if a == -1 {
		return false
	}
	return a.HasClearance(r)
}

func (s *Schema) GetLocker() *sync.RWMutex { return &s.locker }

func (s *Schema) UpdateLastChange() { s.LastChange = time.Now() }

const SYS_PRIMARY_KEY = "__tdb_id__"

func NewSchemaFromString(input string, data TDBData, build_only bool) (*Schema, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("No schema provided")
	}

	schema, err := ParseSchema(input)
	if err != nil {
		return nil, err
	}

	if build_only {
		return schema, nil
	}

	if data == nil {
		schema.Data = make(TDBData)
	} else {
		schema.Data = data
	}

	for _, t := range schema.Tables.Idx {
		if !schema.Data.Has(t.Name) {
			indexes := make(TDBTableIndexes)
			for _, f := range t.Fields.Idx {
				if f.IndexLevel() < IndexLevelUnique {
					continue
				}

				indexes.Set(f.Name, &TDBTableIndexMap{
					locker: sync.RWMutex{},
					Map:    make(map[string]int),
				})
			}

			schema.Data.Set(t.Name, NewTDBTableRows(t, indexes, TDBTablePageRefs{}))
			continue
		}

		rows := t.Rows()
		rows.Map.SetComparisonFunc(func(a, b TDBTableRow) bool {
			return GetPrimaryKey(a) < GetPrimaryKey(b)
		})
	}
	return schema, nil
}

func NewSchemaFromURL(input *url.URL, build_only bool) (*Schema, error) {
	params, err := url.ParseQuery(input.RawQuery)
	if err != nil {
		return nil, err
	}
	schema_data := params.Get("schema")
	return NewSchemaFromString(schema_data, nil, build_only)
}

// ValidateSchemaRelations() allows relations to be defined with non-unique fields.
//
// This logic means that relations defined with unqiue fields are 1-to-1 relations,
// while relations defined with non-unique fields are 1-to-many.
//
// vector -> non-vector type relations are one-to-many;
// non-vector -> vector type relations are many-to-one;
// vector -> vector type relations are many-to-many;
//
// it is assumed that a vector field that is a relation is a vector of individual relations
// and not a relation as a vector itself
func ValidateSchemaRelations(schema *Schema) error {
	for _, table := range schema.Tables.Idx {
		for _, field := range table.Fields.Idx {
			if !field.Properties.Has(props.FieldPropRelation) {
				continue
			}
			rel_table_name, rel_field_name := parser.ParseRelationProp(field.Properties.Get(props.FieldPropRelation).(string))

			invalidRelationError := ThrowInvalidRelationError(table.Name, field.Name, rel_table_name, rel_field_name)

			if !schema.Tables.Has(rel_table_name) {
				return invalidRelationError(fmt.Sprintf("%s is not a valid table", rel_table_name))
			}

			if rel_table_name == table.Name && rel_field_name == field.Name {
				return invalidRelationError("invalid self-relation")
			}

			rel_table := schema.Tables.Get(rel_table_name)

			if !rel_table.Fields.Has(rel_field_name) {
				return invalidRelationError(
					fmt.Sprintf("%s is not a valid field on table %s", rel_field_name, rel_table_name),
				)
			}

			rel_field := rel_table.Fields.Get(rel_field_name)

			if rel_field.BuiltinType != field.BuiltinType {
				// check vector <-> non-vector relations
				if field.BuiltinType == types.FieldTypeVector {
					vector_type, v_level := parser.ParseVectorProp(field.Properties.Get(props.FieldPropVector).(string))
					if v_level > 1 {
						return invalidRelationError("nested vector fields cannot be relations")
					}
					if rel_field.BuiltinType != vector_type {
						return invalidRelationError("field types must match")
					}
				} else if rel_field.BuiltinType == types.FieldTypeVector {
					vector_type, _ := parser.ParseVectorProp(rel_field.Properties.Get(props.FieldPropVector).(string))
					if field.BuiltinType != vector_type {
						return invalidRelationError("field types must match")
					}
				} else {
					return invalidRelationError("field types must match")
				}
			}

			// check vector types & levels are the same
			if field.BuiltinType == types.FieldTypeVector && rel_field.BuiltinType == types.FieldTypeVector {
				field_v_type, field_v_level := parser.ParseVectorProp(field.Properties.Get(props.FieldPropVector).(string))
				rel_field_v_type, rel_field_v_level := parser.ParseVectorProp(rel_field.Properties.Get(props.FieldPropVector).(string))

				if field_v_type != rel_field_v_type || field_v_level != rel_field_v_level {
					return invalidRelationError("field types must match")
				}
			}
		}
	}

	return nil
}

func ThrowInvalidRelationError(table_name, field_name, rel_table_name, rel_field_name string) func(string) error {
	return func(reason string) error {
		return fmt.Errorf(
			"Invalid relation between %s.%s and %s.%s; %s",
			table_name, field_name, rel_table_name, rel_field_name, reason,
		)
	}
}

func (s *Schema) MetaData() ([]byte, error) { return json.Marshal(s) }

func (s *Schema) WriteToFile() error {
	s.locker.Lock()
	defer s.locker.Unlock()

	meta_data, err := s.MetaData()
	if err != nil {
		return err
	}

	base := s.Base()
	if _, err := os.Stat(base); os.IsNotExist(err) {
		os.Mkdir(base, 0o755)
	}

	if err := os.WriteFile(path.Join(base, "meta.tdb"), meta_data, 0o644); err != nil {
		return err
	}

	for _, t := range s.Tables.Idx {
		err := t.WriteToFile()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Schema) Base() string {
	if s.Tdb == nil {
		pkg.DebugLog("TDB is nil in schema", s.Name)
		return s.Name
	}
	base := path.Join(s.Tdb.WriteSettings.WritePath, s.Name)
	if _, err := os.Stat(base); os.IsNotExist(err) {
		os.Mkdir(base, 0o755)
	}
	return base
}

func NewSchemaFromPath(base, name string) (*Schema, error) {
	base = path.Join(base, name)
	meta_data, err := os.ReadFile(path.Join(base, "meta.tdb"))
	if err != nil {
		return nil, err
	}

	var s Schema
	err = json.Unmarshal(meta_data, &s)
	if err != nil {
		return nil, err
	}

	s.Data = TDBData{}
	for _, t := range s.Tables.Idx {
		t.Schema = &s
		for _, f := range t.Fields.Idx {
			f.Table = t
		}
		indexes, err := BuildTableIndexesFromPath(base, t.Name)
		if err != nil {
			return nil, err
		}
		rows := NewTDBTableRows(t, indexes.Indexes, indexes.PrimaryIndexes)
		s.Data.Set(t.Name, rows)
	}
	return &s, nil
}
