package main

import (
	"fmt"
	"sort"
	"strings"
)

func diffCollections(src, dst *CollectionDump) []Diff {
	var diffs []Diff

	// collection-level fields
	cmpInt64(&diffs, "id", src.ID, dst.ID)
	cmpInt64(&diffs, "db_id", src.DBID, dst.DBID)
	cmpStr(&diffs, "name", src.Name, dst.Name)
	cmpStr(&diffs, "description", src.Description, dst.Description)
	cmpStr(&diffs, "state", src.State, dst.State)
	cmpInt32(&diffs, "shards_num", src.ShardsNum, dst.ShardsNum)
	cmpStr(&diffs, "consistency_level", src.ConsistencyLevel, dst.ConsistencyLevel)
	diffMap(&diffs, "properties", src.Properties, dst.Properties)

	// fields: match by field ID (CDC preserves IDs)
	diffFields(&diffs, src.Fields, dst.Fields)

	// partitions: match by partition ID
	diffPartitions(&diffs, src.Partitions, dst.Partitions)

	// indexes: match by index ID
	diffIndexes(&diffs, src.Indexes, dst.Indexes)

	// functions: match by function ID
	diffFunctions(&diffs, src.Functions, dst.Functions)

	return diffs
}

func diffFields(diffs *[]Diff, src, dst []FieldDump) {
	srcMap := make(map[int64]FieldDump, len(src))
	for _, f := range src {
		srcMap[f.FieldID] = f
	}
	dstMap := make(map[int64]FieldDump, len(dst))
	for _, f := range dst {
		dstMap[f.FieldID] = f
	}

	allIDs := collectIDs(srcMap, dstMap)
	for _, id := range allIDs {
		sf, inSrc := srcMap[id]
		df, inDst := dstMap[id]

		label := fmt.Sprintf("fields[id=%d]", id)
		if inSrc {
			label = fmt.Sprintf("fields[id=%d,name=%s]", id, sf.Name)
		} else if inDst {
			label = fmt.Sprintf("fields[id=%d,name=%s]", id, df.Name)
		}

		if !inSrc {
			*diffs = append(*diffs, Diff{Path: label, Src: "<missing>", Dst: df.Name})
			continue
		}
		if !inDst {
			*diffs = append(*diffs, Diff{Path: label, Src: sf.Name, Dst: "<missing>"})
			continue
		}

		cmpStr(diffs, label+".name", sf.Name, df.Name)
		cmpStr(diffs, label+".description", sf.Description, df.Description)
		cmpStr(diffs, label+".data_type", sf.DataType, df.DataType)
		cmpStr(diffs, label+".element_type", sf.ElementType, df.ElementType)
		diffMap(diffs, label+".type_params", sf.TypeParams, df.TypeParams)
		diffMap(diffs, label+".index_params", sf.IndexParams, df.IndexParams)
		cmpBool(diffs, label+".is_primary_key", sf.IsPrimaryKey, df.IsPrimaryKey)
		cmpBool(diffs, label+".auto_id", sf.AutoID, df.AutoID)
		cmpBool(diffs, label+".is_partition_key", sf.IsPartitionKey, df.IsPartitionKey)
		cmpBool(diffs, label+".is_clustering_key", sf.IsClusteringKey, df.IsClusteringKey)
		cmpBool(diffs, label+".is_function_output", sf.IsFunctionOutput, df.IsFunctionOutput)
		cmpBool(diffs, label+".nullable", sf.Nullable, df.Nullable)
		cmpStr(diffs, label+".default_value", sf.DefaultValue, df.DefaultValue)
		cmpStr(diffs, label+".state", sf.State, df.State)
	}
}

func diffPartitions(diffs *[]Diff, src, dst []PartitionDump) {
	srcMap := make(map[int64]PartitionDump, len(src))
	for _, p := range src {
		srcMap[p.PartitionID] = p
	}
	dstMap := make(map[int64]PartitionDump, len(dst))
	for _, p := range dst {
		dstMap[p.PartitionID] = p
	}

	allIDs := collectIDs(srcMap, dstMap)
	for _, id := range allIDs {
		sp, inSrc := srcMap[id]
		dp, inDst := dstMap[id]

		label := fmt.Sprintf("partitions[id=%d]", id)
		if inSrc {
			label = fmt.Sprintf("partitions[id=%d,name=%s]", id, sp.PartitionName)
		} else if inDst {
			label = fmt.Sprintf("partitions[id=%d,name=%s]", id, dp.PartitionName)
		}

		if !inSrc {
			*diffs = append(*diffs, Diff{Path: label, Src: "<missing>", Dst: dp.PartitionName})
			continue
		}
		if !inDst {
			*diffs = append(*diffs, Diff{Path: label, Src: sp.PartitionName, Dst: "<missing>"})
			continue
		}

		cmpStr(diffs, label+".partition_name", sp.PartitionName, dp.PartitionName)
		cmpStr(diffs, label+".state", sp.State, dp.State)
	}
}

func diffIndexes(diffs *[]Diff, src, dst []IndexDump) {
	srcMap := make(map[int64]IndexDump, len(src))
	for _, idx := range src {
		srcMap[idx.IndexID] = idx
	}
	dstMap := make(map[int64]IndexDump, len(dst))
	for _, idx := range dst {
		dstMap[idx.IndexID] = idx
	}

	allIDs := collectIDs(srcMap, dstMap)
	for _, id := range allIDs {
		si, inSrc := srcMap[id]
		di, inDst := dstMap[id]

		label := fmt.Sprintf("indexes[id=%d]", id)
		if inSrc {
			label = fmt.Sprintf("indexes[id=%d,name=%s]", id, si.IndexName)
		} else if inDst {
			label = fmt.Sprintf("indexes[id=%d,name=%s]", id, di.IndexName)
		}

		if !inSrc {
			*diffs = append(*diffs, Diff{Path: label, Src: "<missing>", Dst: di.IndexName})
			continue
		}
		if !inDst {
			*diffs = append(*diffs, Diff{Path: label, Src: si.IndexName, Dst: "<missing>"})
			continue
		}

		cmpStr(diffs, label+".index_name", si.IndexName, di.IndexName)
		cmpInt64(diffs, label+".field_id", si.FieldID, di.FieldID)
		diffMap(diffs, label+".type_params", si.TypeParams, di.TypeParams)
		diffMap(diffs, label+".index_params", si.IndexParams, di.IndexParams)
		diffMap(diffs, label+".user_index_params", si.UserIndexParams, di.UserIndexParams)
		cmpBool(diffs, label+".is_auto_index", si.IsAutoIndex, di.IsAutoIndex)
		cmpStr(diffs, label+".state", si.State, di.State)
		cmpBool(diffs, label+".deleted", si.Deleted, di.Deleted)
	}
}

func diffFunctions(diffs *[]Diff, src, dst []FunctionDump) {
	srcMap := make(map[int64]FunctionDump, len(src))
	for _, f := range src {
		srcMap[f.ID] = f
	}
	dstMap := make(map[int64]FunctionDump, len(dst))
	for _, f := range dst {
		dstMap[f.ID] = f
	}

	allIDs := collectIDs(srcMap, dstMap)
	for _, id := range allIDs {
		sf, inSrc := srcMap[id]
		df, inDst := dstMap[id]

		label := fmt.Sprintf("functions[id=%d]", id)
		if inSrc {
			label = fmt.Sprintf("functions[id=%d,name=%s]", id, sf.Name)
		} else if inDst {
			label = fmt.Sprintf("functions[id=%d,name=%s]", id, df.Name)
		}

		if !inSrc {
			*diffs = append(*diffs, Diff{Path: label, Src: "<missing>", Dst: df.Name})
			continue
		}
		if !inDst {
			*diffs = append(*diffs, Diff{Path: label, Src: sf.Name, Dst: "<missing>"})
			continue
		}

		cmpStr(diffs, label+".name", sf.Name, df.Name)
		cmpStr(diffs, label+".description", sf.Description, df.Description)
		cmpStr(diffs, label+".type", sf.Type, df.Type)
		diffInt64Slice(diffs, label+".input_field_ids", sf.InputFieldIDs, df.InputFieldIDs)
		diffStrSlice(diffs, label+".input_field_names", sf.InputFieldNames, df.InputFieldNames)
		diffInt64Slice(diffs, label+".output_field_ids", sf.OutputFieldIDs, df.OutputFieldIDs)
		diffStrSlice(diffs, label+".output_field_names", sf.OutputFieldNames, df.OutputFieldNames)
		diffMap(diffs, label+".params", sf.Params, df.Params)
	}
}

// --- comparison helpers ---

func cmpStr(diffs *[]Diff, path, src, dst string) {
	if src != dst {
		*diffs = append(*diffs, Diff{Path: path, Src: src, Dst: dst})
	}
}

func cmpInt64(diffs *[]Diff, path string, src, dst int64) {
	if src != dst {
		*diffs = append(*diffs, Diff{Path: path, Src: fmt.Sprintf("%d", src), Dst: fmt.Sprintf("%d", dst)})
	}
}

func cmpInt32(diffs *[]Diff, path string, src, dst int32) {
	if src != dst {
		*diffs = append(*diffs, Diff{Path: path, Src: fmt.Sprintf("%d", src), Dst: fmt.Sprintf("%d", dst)})
	}
}

func cmpBool(diffs *[]Diff, path string, src, dst bool) {
	if src != dst {
		*diffs = append(*diffs, Diff{Path: path, Src: fmt.Sprintf("%v", src), Dst: fmt.Sprintf("%v", dst)})
	}
}

func diffMap(diffs *[]Diff, prefix string, src, dst map[string]string) {
	allKeys := make(map[string]struct{})
	for k := range src {
		allKeys[k] = struct{}{}
	}
	for k := range dst {
		allKeys[k] = struct{}{}
	}

	sorted := make([]string, 0, len(allKeys))
	for k := range allKeys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	for _, k := range sorted {
		sv := src[k]
		dv := dst[k]
		path := prefix + "." + k

		_, inSrc := src[k]
		_, inDst := dst[k]

		switch {
		case !inSrc:
			*diffs = append(*diffs, Diff{Path: path, Src: "<missing>", Dst: dv})
		case !inDst:
			*diffs = append(*diffs, Diff{Path: path, Src: sv, Dst: "<missing>"})
		case sv != dv:
			*diffs = append(*diffs, Diff{Path: path, Src: sv, Dst: dv})
		}
	}
}

func diffStrSlice(diffs *[]Diff, path string, src, dst []string) {
	s := strings.Join(src, ",")
	d := strings.Join(dst, ",")
	if s != d {
		*diffs = append(*diffs, Diff{Path: path, Src: fmt.Sprintf("[%s]", s), Dst: fmt.Sprintf("[%s]", d)})
	}
}

func diffInt64Slice(diffs *[]Diff, path string, src, dst []int64) {
	if len(src) != len(dst) {
		*diffs = append(*diffs, Diff{Path: path, Src: fmt.Sprintf("%v", src), Dst: fmt.Sprintf("%v", dst)})
		return
	}
	for i := range src {
		if src[i] != dst[i] {
			*diffs = append(*diffs, Diff{Path: path, Src: fmt.Sprintf("%v", src), Dst: fmt.Sprintf("%v", dst)})
			return
		}
	}
}

// collectIDs gathers all keys from two maps and returns them sorted.
func collectIDs[V any](a, b map[int64]V) []int64 {
	seen := make(map[int64]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}

	ids := make([]int64, 0, len(seen))
	for k := range seen {
		ids = append(ids, k)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
