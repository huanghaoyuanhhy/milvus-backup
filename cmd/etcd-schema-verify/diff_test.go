package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiffCollections_Aligned(t *testing.T) {
	src := &CollectionDump{
		ID: 100, DBID: 1, Name: "coll", State: "CollectionCreated",
		ShardsNum: 2, ConsistencyLevel: "Strong",
		Fields: []FieldDump{
			{FieldID: 1, Name: "pk", DataType: "Int64", IsPrimaryKey: true},
			{FieldID: 2, Name: "vec", DataType: "FloatVector", TypeParams: map[string]string{"dim": "128"}},
		},
		Partitions: []PartitionDump{
			{PartitionID: 10, PartitionName: "_default", State: "PartitionCreated"},
		},
	}
	dst := &CollectionDump{
		ID: 100, DBID: 1, Name: "coll", State: "CollectionCreated",
		ShardsNum: 2, ConsistencyLevel: "Strong",
		Fields: []FieldDump{
			{FieldID: 1, Name: "pk", DataType: "Int64", IsPrimaryKey: true},
			{FieldID: 2, Name: "vec", DataType: "FloatVector", TypeParams: map[string]string{"dim": "128"}},
		},
		Partitions: []PartitionDump{
			{PartitionID: 10, PartitionName: "_default", State: "PartitionCreated"},
		},
	}

	diffs := diffCollections(src, dst)
	assert.Empty(t, diffs)
}

func TestDiffCollections_CollectionLevelDiff(t *testing.T) {
	src := &CollectionDump{
		ID: 100, DBID: 1, Name: "coll", State: "CollectionCreated",
		ShardsNum: 2, ConsistencyLevel: "Strong",
		Properties: map[string]string{"ttl": "3600"},
	}
	dst := &CollectionDump{
		ID: 101, DBID: 1, Name: "coll", State: "CollectionCreated",
		ShardsNum: 4, ConsistencyLevel: "Bounded",
		Properties: map[string]string{"ttl": "7200"},
	}

	diffs := diffCollections(src, dst)

	diffMap := toDiffMap(diffs)
	assert.Equal(t, "100", diffMap["id"].Src)
	assert.Equal(t, "101", diffMap["id"].Dst)
	assert.Equal(t, "2", diffMap["shards_num"].Src)
	assert.Equal(t, "4", diffMap["shards_num"].Dst)
	assert.Equal(t, "Strong", diffMap["consistency_level"].Src)
	assert.Equal(t, "Bounded", diffMap["consistency_level"].Dst)
	assert.Equal(t, "3600", diffMap["properties.ttl"].Src)
	assert.Equal(t, "7200", diffMap["properties.ttl"].Dst)
}

func TestDiffFields_MatchByID(t *testing.T) {
	t.Run("FieldMissingSrc", func(t *testing.T) {
		src := []FieldDump{}
		dst := []FieldDump{{FieldID: 1, Name: "pk", DataType: "Int64"}}

		var diffs []Diff
		diffFields(&diffs, src, dst)
		assert.Len(t, diffs, 1)
		assert.Equal(t, "<missing>", diffs[0].Src)
		assert.Equal(t, "pk", diffs[0].Dst)
		assert.Contains(t, diffs[0].Path, "id=1")
	})

	t.Run("FieldMissingDst", func(t *testing.T) {
		src := []FieldDump{{FieldID: 1, Name: "pk", DataType: "Int64"}}
		dst := []FieldDump{}

		var diffs []Diff
		diffFields(&diffs, src, dst)
		assert.Len(t, diffs, 1)
		assert.Equal(t, "pk", diffs[0].Src)
		assert.Equal(t, "<missing>", diffs[0].Dst)
	})

	t.Run("FieldDataTypeDiff", func(t *testing.T) {
		src := []FieldDump{{FieldID: 2, Name: "vec", DataType: "FloatVector"}}
		dst := []FieldDump{{FieldID: 2, Name: "vec", DataType: "Float16Vector"}}

		var diffs []Diff
		diffFields(&diffs, src, dst)
		assert.Len(t, diffs, 1)
		assert.Contains(t, diffs[0].Path, "data_type")
		assert.Equal(t, "FloatVector", diffs[0].Src)
		assert.Equal(t, "Float16Vector", diffs[0].Dst)
	})

	t.Run("FieldBoolDiff", func(t *testing.T) {
		src := []FieldDump{{FieldID: 1, Name: "pk", IsPrimaryKey: true, Nullable: false}}
		dst := []FieldDump{{FieldID: 1, Name: "pk", IsPrimaryKey: false, Nullable: true}}

		var diffs []Diff
		diffFields(&diffs, src, dst)
		dm := toDiffMap(diffs)
		assert.Contains(t, dm, "fields[id=1,name=pk].is_primary_key")
		assert.Contains(t, dm, "fields[id=1,name=pk].nullable")
	})

	t.Run("FieldTypeParamsDiff", func(t *testing.T) {
		src := []FieldDump{{FieldID: 2, Name: "vec", TypeParams: map[string]string{"dim": "128"}}}
		dst := []FieldDump{{FieldID: 2, Name: "vec", TypeParams: map[string]string{"dim": "256"}}}

		var diffs []Diff
		diffFields(&diffs, src, dst)
		assert.Len(t, diffs, 1)
		assert.Contains(t, diffs[0].Path, "type_params.dim")
	})
}

func TestDiffPartitions(t *testing.T) {
	t.Run("Aligned", func(t *testing.T) {
		p := []PartitionDump{{PartitionID: 1, PartitionName: "_default", State: "PartitionCreated"}}
		var diffs []Diff
		diffPartitions(&diffs, p, p)
		assert.Empty(t, diffs)
	})

	t.Run("MissingPartition", func(t *testing.T) {
		src := []PartitionDump{
			{PartitionID: 1, PartitionName: "_default", State: "PartitionCreated"},
			{PartitionID: 2, PartitionName: "part_a", State: "PartitionCreated"},
		}
		dst := []PartitionDump{
			{PartitionID: 1, PartitionName: "_default", State: "PartitionCreated"},
		}

		var diffs []Diff
		diffPartitions(&diffs, src, dst)
		assert.Len(t, diffs, 1)
		assert.Contains(t, diffs[0].Path, "id=2")
		assert.Equal(t, "part_a", diffs[0].Src)
		assert.Equal(t, "<missing>", diffs[0].Dst)
	})

	t.Run("StateDiff", func(t *testing.T) {
		src := []PartitionDump{{PartitionID: 1, PartitionName: "_default", State: "PartitionCreated"}}
		dst := []PartitionDump{{PartitionID: 1, PartitionName: "_default", State: "PartitionDropping"}}

		var diffs []Diff
		diffPartitions(&diffs, src, dst)
		assert.Len(t, diffs, 1)
		assert.Contains(t, diffs[0].Path, "state")
	})
}

func TestDiffIndexes(t *testing.T) {
	t.Run("Aligned", func(t *testing.T) {
		idx := []IndexDump{{
			IndexID: 1, IndexName: "idx_vec", FieldID: 2,
			IndexParams: map[string]string{"index_type": "IVF_FLAT", "nlist": "128"},
			State:       "Finished",
		}}
		var diffs []Diff
		diffIndexes(&diffs, idx, idx)
		assert.Empty(t, diffs)
	})

	t.Run("IndexParamsDiff", func(t *testing.T) {
		src := []IndexDump{{
			IndexID: 1, IndexName: "idx_vec", FieldID: 2,
			IndexParams: map[string]string{"index_type": "IVF_FLAT", "nlist": "128"},
		}}
		dst := []IndexDump{{
			IndexID: 1, IndexName: "idx_vec", FieldID: 2,
			IndexParams: map[string]string{"index_type": "IVF_FLAT", "nlist": "256"},
		}}

		var diffs []Diff
		diffIndexes(&diffs, src, dst)
		assert.Len(t, diffs, 1)
		assert.Contains(t, diffs[0].Path, "index_params.nlist")
	})

	t.Run("MissingIndex", func(t *testing.T) {
		src := []IndexDump{{IndexID: 1, IndexName: "idx_vec"}}
		dst := []IndexDump{}

		var diffs []Diff
		diffIndexes(&diffs, src, dst)
		assert.Len(t, diffs, 1)
		assert.Equal(t, "<missing>", diffs[0].Dst)
	})

	t.Run("FieldIDDiff", func(t *testing.T) {
		src := []IndexDump{{IndexID: 1, IndexName: "idx", FieldID: 2}}
		dst := []IndexDump{{IndexID: 1, IndexName: "idx", FieldID: 3}}

		var diffs []Diff
		diffIndexes(&diffs, src, dst)
		dm := toDiffMap(diffs)
		assert.Contains(t, dm, "indexes[id=1,name=idx].field_id")
	})
}

func TestDiffFunctions(t *testing.T) {
	t.Run("Aligned", func(t *testing.T) {
		f := []FunctionDump{{
			ID: 1, Name: "bm25", Type: "BM25",
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"sparse"},
			InputFieldIDs:    []int64{3},
			OutputFieldIDs:   []int64{4},
		}}
		var diffs []Diff
		diffFunctions(&diffs, f, f)
		assert.Empty(t, diffs)
	})

	t.Run("TypeDiff", func(t *testing.T) {
		src := []FunctionDump{{ID: 1, Name: "fn", Type: "BM25"}}
		dst := []FunctionDump{{ID: 1, Name: "fn", Type: "TextEmbedding"}}

		var diffs []Diff
		diffFunctions(&diffs, src, dst)
		dm := toDiffMap(diffs)
		assert.Contains(t, dm, "functions[id=1,name=fn].type")
	})

	t.Run("FieldIDsDiff", func(t *testing.T) {
		src := []FunctionDump{{ID: 1, Name: "fn", InputFieldIDs: []int64{3, 4}}}
		dst := []FunctionDump{{ID: 1, Name: "fn", InputFieldIDs: []int64{3, 5}}}

		var diffs []Diff
		diffFunctions(&diffs, src, dst)
		assert.Len(t, diffs, 1)
		assert.Contains(t, diffs[0].Path, "input_field_ids")
	})
}

func TestDiffMap(t *testing.T) {
	t.Run("BothNil", func(t *testing.T) {
		var diffs []Diff
		diffMap(&diffs, "props", nil, nil)
		assert.Empty(t, diffs)
	})

	t.Run("SrcOnly", func(t *testing.T) {
		var diffs []Diff
		diffMap(&diffs, "props", map[string]string{"a": "1"}, nil)
		assert.Len(t, diffs, 1)
		assert.Equal(t, "1", diffs[0].Src)
		assert.Equal(t, "<missing>", diffs[0].Dst)
	})

	t.Run("DstOnly", func(t *testing.T) {
		var diffs []Diff
		diffMap(&diffs, "props", nil, map[string]string{"a": "1"})
		assert.Len(t, diffs, 1)
		assert.Equal(t, "<missing>", diffs[0].Src)
		assert.Equal(t, "1", diffs[0].Dst)
	})

	t.Run("ValueDiff", func(t *testing.T) {
		var diffs []Diff
		diffMap(&diffs, "p", map[string]string{"k": "v1"}, map[string]string{"k": "v2"})
		assert.Len(t, diffs, 1)
		assert.Equal(t, "p.k", diffs[0].Path)
	})

	t.Run("Equal", func(t *testing.T) {
		var diffs []Diff
		diffMap(&diffs, "p", map[string]string{"k": "v"}, map[string]string{"k": "v"})
		assert.Empty(t, diffs)
	})
}

func TestDiffStrSlice(t *testing.T) {
	t.Run("Equal", func(t *testing.T) {
		var diffs []Diff
		diffStrSlice(&diffs, "f", []string{"a", "b"}, []string{"a", "b"})
		assert.Empty(t, diffs)
	})

	t.Run("Different", func(t *testing.T) {
		var diffs []Diff
		diffStrSlice(&diffs, "f", []string{"a"}, []string{"a", "b"})
		assert.Len(t, diffs, 1)
	})

	t.Run("BothNil", func(t *testing.T) {
		var diffs []Diff
		diffStrSlice(&diffs, "f", nil, nil)
		assert.Empty(t, diffs)
	})
}

func TestDiffInt64Slice(t *testing.T) {
	t.Run("Equal", func(t *testing.T) {
		var diffs []Diff
		diffInt64Slice(&diffs, "f", []int64{1, 2}, []int64{1, 2})
		assert.Empty(t, diffs)
	})

	t.Run("DifferentLength", func(t *testing.T) {
		var diffs []Diff
		diffInt64Slice(&diffs, "f", []int64{1}, []int64{1, 2})
		assert.Len(t, diffs, 1)
	})

	t.Run("DifferentValues", func(t *testing.T) {
		var diffs []Diff
		diffInt64Slice(&diffs, "f", []int64{1, 2}, []int64{1, 3})
		assert.Len(t, diffs, 1)
	})

	t.Run("BothNil", func(t *testing.T) {
		var diffs []Diff
		diffInt64Slice(&diffs, "f", nil, nil)
		assert.Empty(t, diffs)
	})
}

func TestCollectIDs(t *testing.T) {
	a := map[int64]string{3: "c", 1: "a"}
	b := map[int64]string{2: "b", 1: "a"}
	ids := collectIDs(a, b)
	assert.Equal(t, []int64{1, 2, 3}, ids)
}

func TestCollectIDs_Empty(t *testing.T) {
	ids := collectIDs(map[int64]string{}, map[int64]string{})
	assert.Empty(t, ids)
}

// helper to convert diffs slice to map for easier assertions
func toDiffMap(diffs []Diff) map[string]Diff {
	m := make(map[string]Diff, len(diffs))
	for _, d := range diffs {
		m[d.Path] = d
	}
	return m
}
