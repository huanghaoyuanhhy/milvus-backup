package main

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
)

func TestKvPairsToMap(t *testing.T) {
	t.Run("Nil", func(t *testing.T) {
		assert.Nil(t, kvPairsToMap(nil))
	})

	t.Run("Empty", func(t *testing.T) {
		assert.Nil(t, kvPairsToMap([]*commonpb.KeyValuePair{}))
	})

	t.Run("MultipleEntries", func(t *testing.T) {
		pairs := []*commonpb.KeyValuePair{
			{Key: "dim", Value: "128"},
			{Key: "metric_type", Value: "L2"},
		}
		m := kvPairsToMap(pairs)
		assert.Equal(t, "128", m["dim"])
		assert.Equal(t, "L2", m["metric_type"])
		assert.Len(t, m, 2)
	})
}

func TestBuildFieldDump(t *testing.T) {
	t.Run("BasicField", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			FieldID:      100,
			Name:         "pk",
			DataType:     schemapb.DataType_Int64,
			IsPrimaryKey: true,
			AutoID:       true,
			State:        schemapb.FieldState_FieldCreated,
		}

		fd := buildFieldDump(f)
		assert.Equal(t, int64(100), fd.FieldID)
		assert.Equal(t, "pk", fd.Name)
		assert.Equal(t, "Int64", fd.DataType)
		assert.True(t, fd.IsPrimaryKey)
		assert.True(t, fd.AutoID)
		assert.Equal(t, "FieldCreated", fd.State)
		assert.Empty(t, fd.ElementType)
	})

	t.Run("VectorFieldWithTypeParams", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			FieldID:  101,
			Name:     "vec",
			DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: "dim", Value: "256"},
			},
		}

		fd := buildFieldDump(f)
		assert.Equal(t, "FloatVector", fd.DataType)
		assert.Equal(t, "256", fd.TypeParams["dim"])
		assert.Empty(t, fd.ElementType)
	})

	t.Run("ArrayFieldWithElementType", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			FieldID:     102,
			Name:        "tags",
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_VarChar,
		}

		fd := buildFieldDump(f)
		assert.Equal(t, "Array", fd.DataType)
		assert.Equal(t, "VarChar", fd.ElementType)
	})

	t.Run("PartitionKeyField", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			FieldID:        103,
			Name:           "tenant",
			DataType:       schemapb.DataType_VarChar,
			IsPartitionKey: true,
		}

		fd := buildFieldDump(f)
		assert.True(t, fd.IsPartitionKey)
		assert.False(t, fd.IsClusteringKey)
	})

	t.Run("NullableField", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			FieldID:  104,
			Name:     "optional",
			DataType: schemapb.DataType_VarChar,
			Nullable: true,
		}

		fd := buildFieldDump(f)
		assert.True(t, fd.Nullable)
	})

	t.Run("FunctionOutputField", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			FieldID:          105,
			Name:             "sparse_out",
			DataType:         schemapb.DataType_SparseFloatVector,
			IsFunctionOutput: true,
		}

		fd := buildFieldDump(f)
		assert.True(t, fd.IsFunctionOutput)
	})
}

func TestMergeFields(t *testing.T) {
	t.Run("UpdateStateForExistingField", func(t *testing.T) {
		d := &CollectionDump{
			Fields: []FieldDump{
				{FieldID: 1, Name: "pk", State: ""},
				{FieldID: 2, Name: "vec", State: ""},
			},
		}
		sepFields := []*schemapb.FieldSchema{
			{FieldID: 1, State: schemapb.FieldState_FieldCreated},
			{FieldID: 2, State: schemapb.FieldState_FieldCreated},
		}

		mergeFields(d, sepFields)
		assert.Len(t, d.Fields, 2)
		assert.Equal(t, "FieldCreated", d.Fields[0].State)
		assert.Equal(t, "FieldCreated", d.Fields[1].State)
	})

	t.Run("AddNewField", func(t *testing.T) {
		d := &CollectionDump{
			Fields: []FieldDump{
				{FieldID: 1, Name: "pk"},
			},
		}
		sepFields := []*schemapb.FieldSchema{
			{FieldID: 2, Name: "vec", DataType: schemapb.DataType_FloatVector, State: schemapb.FieldState_FieldCreated},
		}

		mergeFields(d, sepFields)
		assert.Len(t, d.Fields, 2)
		assert.Equal(t, "vec", d.Fields[1].Name)
		assert.Equal(t, "FloatVector", d.Fields[1].DataType)
	})

	t.Run("EmptySeparateFields", func(t *testing.T) {
		d := &CollectionDump{
			Fields: []FieldDump{{FieldID: 1, Name: "pk"}},
		}

		mergeFields(d, nil)
		assert.Len(t, d.Fields, 1)
	})

	t.Run("EmptyInlineFields", func(t *testing.T) {
		d := &CollectionDump{}
		sepFields := []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64},
		}

		mergeFields(d, sepFields)
		assert.Len(t, d.Fields, 1)
		assert.Equal(t, "pk", d.Fields[0].Name)
	})
}

func TestReaderPrefix(t *testing.T) {
	r := &reader{rootPath: "by-dev"}
	assert.Equal(t, "by-dev/root-coord/database/db-info", r.prefix("root-coord", "database", "db-info"))
	assert.Equal(t, "by-dev/meta/field-index/100", r.prefix("meta", "field-index", "100"))
}
