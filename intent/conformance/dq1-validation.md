# DQ1 Validation: Dataflow Variance in Dagster Corpus

**Design Question:** How does the IDL handle dataflow variance — pipeline graphs where nodes have different in/out cardinalities and types vary along the pipeline?

**Status:** ✅ **RESOLVED** (with caveats)

## Evidence from Corpus

### Variance Pattern 1: Cardinality Shift (1→Many→1)

**Example:** Dynamic partitions pipeline

```
releases_metadata (many) → release_zips (many) → ... → release_files_metadata (many) → releases_summary (one)
```

**Variance Observed:**
- `releases_metadata`: input_cardinality=many, output_cardinality=many
- `release_files_metadata`: input_cardinality=many, output_cardinality=many  
- `releases_summary`: input_cardinality=one, output_cardinality=one (aggregates all partitions)

**IDL Representation:**
```json
{
  "id": "asset_releases_summary",
  "properties": {
    "input_cardinality": "one",
    "output_cardinality": "one",
    "is_partitioned": false
  }
}
```

**Verdict:** ✅ Cardinality variance is **representable** via `input_cardinality` and `output_cardinality` on nodes.

---

### Variance Pattern 2: Type Transformation Along Chain

**Example:** Quickstart ETL chain

```
topstory_ids (None) → topstories (MaterializeResult) → most_frequent_words (MaterializeResult)
```

**Type Evolution:**
1. `topstory_ids`: Fetches JSON, writes to file → return type `None` (side-effect)
2. `topstories`: Reads file, transforms to DataFrame, writes CSV → return type `MaterializeResult`
3. `most_frequent_words`: Reads CSV, computes word counts, generates chart → return type `MaterializeResult`

**Actual Data Types at Runtime:**
- topstory_ids: `List[int]` (written to JSON file)
- topstories: `pd.DataFrame` (written to CSV file)
- most_frequent_words: `Dict[str, int]` (written to JSON file)

**IDL Representation:**
```json
{
  "id": "asset_topstories",
  "properties": {
    "return_type": "MaterializeResult"
  }
}
```

**Gap Identified:** ⚠️ The IDL captures the **declared return type** (MaterializeResult) but not the **actual data type** (DataFrame, Dict). The file I/O side-effects are invisible.

**Workaround:** For Dagster, the declared return type is less important than the *runtime data type*. Extractors could introspect the function body to infer actual types (e.g., `df.to_csv()` → DataFrame output).

---

### Variance Pattern 3: Multi-Output Fan-Out

**Example:** Multi-asset

```python
def write_multiple_artifacts() -> tuple[wandb.Table, wandb.Table]:
    first_table = wandb.Table(...)
    second_table = wandb.Table(...)
    return first_table, second_table
```

**Type Split:**
- Input: single function execution
- Output: 2 separate assets (first_table, second_table)

**IDL Representation:**
```json
{
  "id": "multi_write_multiple_artifacts",
  "properties": {
    "output_count": 2,
    "return_type": "tuple[wandb.Table, wandb.Table]"
  }
}
// Two separate output nodes
{
  "id": "asset_first_table",
  "properties": {
    "dagster_type": "asset_output",
    "parent_multi_asset": "write_multiple_artifacts"
  }
}
```

**Verdict:** ✅ Multi-output fan-out is **representable** by creating separate output nodes with `parent_multi_asset` linkage.

---

## Summary: Can IDL Handle Variance?

### ✅ What IDL Handles Well

1. **Cardinality variance:** Nodes can have different input/output cardinalities (one, many, zero)
2. **Partition variance:** Partitioned assets are distinguishable from unpartitioned
3. **Fan-out variance:** Multi-output computations can be modeled with multiple output nodes
4. **Declared type annotations:** Return types are preserved in node properties

### ⚠️ What IDL Handles Partially

1. **Runtime type evolution:** IDL captures declared types but not runtime data types
2. **Side-effect I/O:** File writes/reads aren't modeled as explicit edges
3. **Type transformations per edge:** Types are node-level metadata, not edge-level

### ❌ What IDL Cannot Express (in current schema)

1. **Dynamic type inference:** No way to say "this asset produces a DataFrame at runtime but declares MaterializeResult"
2. **Schema evolution:** No way to track DataFrame column schema changes across pipeline
3. **Conditional types:** No way to say "output type depends on input partition key"

---

## Recommendations for DQ1

**Verdict:** ✅ **RESOLVED FOR BASIC USE CASES**

The IDL **can represent** cardinality variance (many→one, one→many) and basic type annotations. For Dagster-style dataflow:
- Partitioned assets → `is_partitioned=true` + `input_cardinality=many`
- Aggregation assets → `is_partitioned=false` + `input_cardinality=one` (even if inputs are partitioned)
- Fan-out → Multi-output nodes with separate asset outputs

**Caveat:** For **deep lineage tracking** (e.g., "what columns flowed from source to sink?"), the IDL needs edge-level type metadata or a separate lineage layer. The current schema is sufficient for **graph topology and cardinality**, but not for **fine-grained data provenance**.

**No schema changes required** to close DQ1 for the stated scope (cardinality variance).

**Optional future extension:** Add `edge.type_transformation` field to capture input_type → output_type per edge, if deep provenance is needed.

---

## Corpus Examples Supporting Resolution

| Asset Chain | Variance Type | IDL Handles? |
|-------------|---------------|--------------|
| topstory_ids → topstories | 1→1, type change | ✅ Yes (as node properties) |
| releases_metadata → releases_summary | many→1 aggregation | ✅ Yes (cardinality + partition flag) |
| write_multiple_artifacts → {first_table, second_table} | 1→2 fan-out | ✅ Yes (multi-output nodes) |
| release_zips → release_files | many→many partition-preserving | ✅ Yes (partition edges) |

**Conclusion:** DQ1 is **resolved** for graph-level variance representation. Deeper semantic variance (schema evolution, conditional types) is out of scope for v0.1.4.
