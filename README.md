# Dagster to IDL Extraction

**Purpose:** Validate IDL design questions DQ1 (dataflow variance) and DQ5 (fan strategy) using real Dagster asset graphs.

**Wave:** W15  
**Agent:** Parker  
**Status:** ✅ Complete

---

## Corpus

This extraction analyzes three Dagster example projects:

1. **quickstart_etl**: Linear chain (3 assets)
   - topstory_ids → topstories → most_frequent_words
   - Pattern: 1→1 sequential dataflow

2. **assets_dynamic_partitions**: Partitioned pipeline (5 assets)
   - releases_metadata → release_zips → release_files → release_files_metadata
   - releases_metadata + release_files_metadata → releases_summary (fan-in)
   - Pattern: many→many (partitioned) + many→1 (aggregation)

3. **multi_asset_example**: Fan-out (1→2)
   - write_multiple_artifacts → {first_table, second_table}
   - Pattern: 1→N (single computation, multiple outputs)

**Total:** 8 single assets + 1 multi_asset → 11 nodes, 9 edges

---

## Results

### DQ1: Dataflow Variance

**Question:** Can IDL represent cardinality and type variance in pipelines?

**Answer:** ✅ **RESOLVED**

- IDL **can represent** cardinality variance via node properties:
  - `input_cardinality: "one" | "many"`
  - `output_cardinality: "one" | "many"`
  - `is_partitioned: boolean`
- Validated patterns: 1→1, many→many, many→1, 1→N
- Type annotations captured as `return_type` on nodes
- **Limitation:** Type transformations are node-level, not edge-level

**Verdict:** No schema changes required. Current v0.1.4 is sufficient.

---

### DQ5: Fan Strategy

**Question:** How to represent fan-out: broadcast vs. partition vs. routed?

**Answer:** ✅ **RESOLVED**

- IDL extension `fan_strategy: enum("broadcast", "partition", "routed")` as **edge property** is sufficient
- Validated patterns:
  - ✅ **broadcast:** Linear chains, aggregations (many→1)
  - ✅ **partition:** Partitioned pipelines (many→many), multi-asset splits (1→N)
  - ⚠️ **routed:** Not found in corpus (Dagster doesn't use conditional routing)

**Classification Logic:**
- partition-to-partition (same key) → **partition**
- many→one (aggregation) → **broadcast**
- one→many (duplication) → **broadcast**
- multi_asset → outputs → **partition** (split)

**Verdict:** No schema changes required. Current v0.1.4 is sufficient.

---

## Files

```
dagster-idl/
├── README.md                          # This file
├── extract_dagster.py                 # Python AST extractor
├── intent/
│   ├── sources/                       # Original Dagster code
│   │   ├── quickstart_etl_assets.py
│   │   ├── dynamic_partitions_assets.py
│   │   └── multi_asset_example.py
│   ├── extracted/
│   │   └── dagster-graph.json        # Generated IDL graph
│   └── conformance/
│       ├── conformance.md             # Overall conformance report
│       ├── dq1-validation.md          # DQ1 detailed validation
│       └── dq5-validation.md          # DQ5 detailed validation
└── dagster-repo/                      # Cloned Dagster examples (gitignored)
```

---

## Running the Extractor

```bash
python3 extract_dagster.py
```

Output: `intent/extracted/dagster-graph.json`

---

## Key Findings

1. **Cardinality variance is representable** via node properties (input/output cardinality)
2. **Fan strategy is representable** via edge properties (broadcast/partition/routed)
3. **No schema changes required** — IDL v0.1.4 is sufficient
4. **Optional future extensions:**
   - `edge.routing_condition` for routed strategies
   - `edge.partition_key` for partition alignment validation
   - `edge.type_transformation` for deep lineage tracking

---

## Decision

See `.squad/decisions/inbox/parker-dagster-dq1-dq5.md` for formal decision record.

**Summary:** Both DQ1 and DQ5 are **RESOLVED**. IDL schema v0.1.4 requires no changes.
