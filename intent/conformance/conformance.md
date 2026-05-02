# Conformance Report: Dagster to IDL

**Project:** dagster-idl  
**Corpus:** Dagster asset graphs (quickstart_etl + assets_dynamic_partitions + multi_asset_example)  
**IDL Schema Version:** 0.1.4  
**Extraction Date:** 2025-01-01

## Overview

This corpus validates the IDL's ability to represent Dagster asset-based dataflow patterns, focusing specifically on:
- **DQ1 (Dataflow Variance):** How the IDL handles varying cardinalities and type transformations in pipelines
- **DQ5 (Fan Strategy):** How the IDL represents fan-out/fan-in patterns (broadcast, partition, routed)

## Corpus Statistics

- **Total Assets:** 8 single assets + 1 multi_asset (2 outputs)
- **Total Nodes:** 11 (8 computation nodes + 1 multi_asset + 2 output nodes)
- **Total Edges:** 9
- **Patterns Covered:**
  - Linear chain (topstory_ids → topstories → most_frequent_words)
  - Partitioned pipeline (releases_metadata → release_zips → release_files → release_files_metadata)
  - Fan-in: 2→1 (releases_metadata + release_files_metadata → releases_summary)
  - Fan-out: 1→2 (write_multiple_artifacts → first_table + second_table)

## Graph Topology

### Quickstart ETL Chain (Linear)
```
topstory_ids → topstories → most_frequent_words
```

**Cardinality:** 1→1→1  
**Types:** None → MaterializeResult → MaterializeResult  
**Strategy:** broadcast at each step

### Dynamic Partitions Pipeline
```
releases_metadata (partitioned) → release_zips (partitioned) → release_files (partitioned) → release_files_metadata (partitioned)
                     ↓                                                                            ↓
                     +--------------------------------------------→ releases_summary (unpartitioned)
```

**Cardinality:**  
- releases_metadata: many (partitioned)
- release_zips: many (partitioned)
- release_files: many (partitioned)
- release_files_metadata: many (partitioned)
- releases_summary: one (aggregates all partitions)

**Fan-in:** releases_metadata (many) + release_files_metadata (many) → releases_summary (one)  
**Strategy:**  
- Partition edges: partition (each partition flows independently)
- Fan-in edges: broadcast (all partitions aggregate into one)

### Multi-Asset Fan-Out
```
write_multiple_artifacts → first_table
                        → second_table
```

**Cardinality:** 1→2 (single computation produces 2 outputs)  
**Types:** tuple[wandb.Table, wandb.Table] → Table + Table  
**Strategy:** partition (split output tuple into separate assets)

## IDL Representation Quality

### Strengths
1. **Node representation:** Computation nodes clearly distinguish asset vs. multi_asset types
2. **Partition metadata:** `is_partitioned` and `partitions_def` captured at node level
3. **Cardinality tracking:** `input_cardinality` and `output_cardinality` on nodes
4. **Fan strategy:** `fan_strategy` attribute on edges captures broadcast/partition distinction
5. **Type annotations:** Return types preserved in node properties

### Gaps Identified
1. **Type variance within chain:** IDL doesn't track type transformations edge-by-edge (DataFrame → CSV file → Dict)
2. **Partition semantics:** Can't distinguish *how* partitions are defined (dynamic vs. static vs. time-based)
3. **Routed fan strategy:** No examples in this corpus, but IDL supports it theoretically
4. **Aggregation semantics:** Fan-in edges don't specify *how* aggregation happens (groupby, join, union)

## Conformance to IDL v0.1.4

**Status:** ✅ **CONFORMS** with minor semantic gaps

The extracted graph validates against IDL schema v0.1.4:
- All nodes have required fields (id, label, node_type)
- All edges have required fields (source, target, edge_type)
- Fan strategy is represented as edge property
- Cardinality is represented as node property

## Validation Summary

| Pattern | Represented? | Notes |
|---------|--------------|-------|
| Linear chain | ✅ Yes | Simple 1→1 edges with broadcast |
| Partitioned pipeline | ✅ Yes | Cardinality + partition strategy on edges |
| Fan-out (1→N) | ✅ Yes | Multi-asset with partition edges to outputs |
| Fan-in (N→1) | ✅ Yes | Multiple broadcast edges converging to one node |
| Type variance | ⚠️ Partial | Types in node properties, not edges |
| Dynamic partitioning | ⚠️ Partial | Captured but not deeply modeled |

## Recommendations

1. **DQ1 Resolution:** Move to **resolved** with caveat: IDL can represent cardinality variance via node properties, but edge-level type transformations need separate modeling if deep lineage is required.

2. **DQ5 Resolution:** Move to **resolved**: Fan strategy (broadcast/partition/routed) is expressible as edge property. This corpus validates broadcast and partition; routed would need conditional routing logic.

3. **Schema Stability:** No schema changes required for basic Dagster representation. Future extensions could add:
   - `aggregation_type` on fan-in edges (groupby, join, union, concat)
   - `partition_semantics` on nodes (dynamic, static, time, custom)
   - `type_transformation` on edges (if deep lineage tracking is desired)
