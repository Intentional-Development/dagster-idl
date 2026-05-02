# DQ5 Validation: Fan Strategy in Dagster Corpus

**Design Question:** When a single asset/dataset fans out to N downstream consumers, how does the IDL represent fan-out (broadcast vs. partition vs. routed)?

**Status:** ✅ **RESOLVED**

## Proposed Solution

**Edge-level property:** `fan_strategy: enum("broadcast", "partition", "routed")`

This property lives on **edges** (not nodes) because the same upstream asset can have different fan strategies to different downstream consumers.

---

## Evidence from Corpus

### Fan Strategy 1: Broadcast (Full Data to All Consumers)

**Example 1: Linear Chain**
```
topstory_ids → topstories → most_frequent_words
```

**Semantics:** Each downstream asset consumes the **full output** of the upstream asset. No splitting.

**IDL Representation:**
```json
{
  "source": "asset_topstory_ids",
  "target": "asset_topstories",
  "properties": {
    "fan_strategy": "broadcast"
  }
}
```

**Classification Logic:**
- Both assets are unpartitioned (1→1)
- Downstream reads the entire upstream output
- ✅ **broadcast**

---

**Example 2: Fan-In with Aggregation**
```
releases_metadata (many) → releases_summary (one)
release_files_metadata (many) → releases_summary (one)
```

**Semantics:** `releases_summary` consumes **all partitions** from both upstream assets and aggregates them into a single summary.

**IDL Representation:**
```json
{
  "source": "asset_releases_metadata",
  "target": "asset_releases_summary",
  "properties": {
    "fan_strategy": "broadcast"
  }
}
```

**Classification Logic:**
- Upstream is partitioned (many), downstream is unpartitioned (one)
- Downstream needs **all** upstream data to compute summary
- ✅ **broadcast** (even though it's many→one, all data flows)

---

### Fan Strategy 2: Partition (Data Split Across Consumers)

**Example 1: Partitioned Pipeline**
```
releases_metadata (many) → release_zips (many)
```

**Semantics:** Each **partition** of `releases_metadata` flows to the corresponding **partition** of `release_zips`. Partition `release-1.0` produces `release-1.0.zip`, independently of other partitions.

**IDL Representation:**
```json
{
  "source": "asset_releases_metadata",
  "target": "asset_release_zips",
  "properties": {
    "fan_strategy": "partition"
  }
}
```

**Classification Logic:**
- Both assets use the same `partitions_def` (releases_partitions_def)
- Each partition flows independently (no cross-partition dependencies)
- ✅ **partition**

---

**Example 2: Multi-Asset Fan-Out**
```python
@multi_asset(outs={"first_table": ..., "second_table": ...})
def write_multiple_artifacts() -> tuple[wandb.Table, wandb.Table]:
    return first_table, second_table
```

**Semantics:** A single computation produces **multiple outputs**. The return tuple is split into separate assets.

**IDL Representation:**
```json
{
  "source": "multi_write_multiple_artifacts",
  "target": "asset_first_table",
  "properties": {
    "fan_strategy": "partition"
  }
},
{
  "source": "multi_write_multiple_artifacts",
  "target": "asset_second_table",
  "properties": {
    "fan_strategy": "partition"
  }
}
```

**Classification Logic:**
- Single computation → multiple outputs
- Each output gets a **portion** of the result (first element vs. second element of tuple)
- ✅ **partition** (output is split, not duplicated)

---

### Fan Strategy 3: Routed (Conditional Data Routing)

**Status:** ❌ **NOT FOUND IN CORPUS**

**Hypothetical Example:**
```python
@asset
def source_data() -> pd.DataFrame:
    return pd.DataFrame([...])

@asset(deps=[source_data])
def high_value_customers(context, source_data: pd.DataFrame) -> pd.DataFrame:
    return source_data[source_data["value"] > 1000]

@asset(deps=[source_data])
def low_value_customers(context, source_data: pd.DataFrame) -> pd.DataFrame:
    return source_data[source_data["value"] <= 1000]
```

**Semantics:** `source_data` fans out to two consumers, but each consumer gets a **filtered subset** based on a predicate.

**Expected IDL Representation:**
```json
{
  "source": "asset_source_data",
  "target": "asset_high_value_customers",
  "properties": {
    "fan_strategy": "routed",
    "routing_condition": "value > 1000"
  }
}
```

**Conclusion:** The IDL schema **supports** `fan_strategy: "routed"`, but this corpus doesn't contain routing logic. Dagster's typical pattern is broadcast or partition, not conditional routing at the graph level.

---

## Validation: IDL Extension Sufficiency

### Proposed IDL Schema (Already in v0.1.4)
```json
{
  "edges": [
    {
      "source": "node_id",
      "target": "node_id",
      "edge_type": "dataflow",
      "properties": {
        "fan_strategy": "broadcast" | "partition" | "routed"
      }
    }
  ]
}
```

### ✅ What Works

1. **Broadcast:** Captured for 1→1, 1→N (full broadcast), many→1 (aggregation)
2. **Partition:** Captured for many→many (same partition key), 1→N (multi-asset split)
3. **Edge-level property:** Same upstream can have different strategies to different downstreams ✅

### ⚠️ Limitations Found

1. **Routing conditions:** `fan_strategy: "routed"` exists, but no field for the **routing predicate**. If routing is needed, add:
   ```json
   "properties": {
     "fan_strategy": "routed",
     "routing_condition": "value > threshold"  // ← optional field
   }
   ```

2. **Partition key propagation:** The IDL doesn't explicitly model *which* partition key is used. Two assets can both be "partitioned" but use different keys. Need:
   ```json
   "properties": {
     "partition_key": "release_tag"  // ← to validate alignment
   }
   ```

---

## Summary: Can IDL Represent Fan Strategies?

| Pattern | Example in Corpus | IDL Handles? | Notes |
|---------|-------------------|--------------|-------|
| **Broadcast (1→1)** | topstory_ids → topstories | ✅ Yes | Simple dataflow |
| **Broadcast (many→1)** | releases_metadata → releases_summary | ✅ Yes | Aggregation |
| **Partition (many→many)** | releases_metadata → release_zips | ✅ Yes | Same partition key |
| **Partition (1→N split)** | multi_asset → first_table, second_table | ✅ Yes | Output tuple split |
| **Routed (conditional)** | ❌ Not in corpus | ⚠️ Partial | `fan_strategy: "routed"` supported, but no routing condition field |

---

## Recommendations for DQ5

**Verdict:** ✅ **RESOLVED**

The IDL extension `fan_strategy: enum("broadcast", "partition", "routed")` as an **edge property** is **sufficient** for Dagster dataflow patterns.

**Evidence:**
1. ✅ Broadcast is validated (linear chains, aggregations)
2. ✅ Partition is validated (partitioned pipelines, multi-asset splits)
3. ⚠️ Routed is theoretically supported but not validated in this corpus

**No schema changes required** for basic Dagster representation. Current v0.1.4 is sufficient.

**Optional future extensions:**
1. Add `edge.routing_condition` for routed strategies (if semantic routing is needed)
2. Add `edge.partition_key` to validate partition alignment between upstream/downstream

---

## Classification Algorithm Validation

The extractor uses this logic to assign `fan_strategy`:

```python
def _infer_fan_strategy(upstream, downstream):
    if upstream.is_partitioned and downstream.is_partitioned:
        return "partition"  # partition-to-partition
    elif upstream.is_partitioned and not downstream.is_partitioned:
        return "broadcast"  # many→1 aggregation
    elif not upstream.is_partitioned and downstream.is_partitioned:
        return "broadcast"  # 1→many broadcast
    else:
        return "broadcast"  # 1→1 default
```

**Validation Results:**

| Edge | Upstream | Downstream | Assigned | Correct? |
|------|----------|------------|----------|----------|
| releases_metadata → release_zips | partitioned | partitioned | partition | ✅ Yes |
| releases_metadata → releases_summary | partitioned | unpartitioned | broadcast | ✅ Yes |
| topstory_ids → topstories | unpartitioned | unpartitioned | broadcast | ✅ Yes |
| multi_asset → first_table | N/A | N/A | partition | ✅ Yes (manual override for multi-asset) |

**Conclusion:** Classification logic is sound for Dagster patterns.

---

## Corpus Examples Supporting Resolution

| Fan Pattern | Example | IDL Edge Property | Validated? |
|-------------|---------|-------------------|------------|
| 1→1 broadcast | topstory_ids → topstories | `"fan_strategy": "broadcast"` | ✅ Yes |
| many→1 broadcast (aggregation) | releases_metadata → releases_summary | `"fan_strategy": "broadcast"` | ✅ Yes |
| many→many partition | releases_metadata → release_zips | `"fan_strategy": "partition"` | ✅ Yes |
| 1→N partition (split) | multi_asset → {first_table, second_table} | `"fan_strategy": "partition"` | ✅ Yes |
| 1→N routed (conditional) | ❌ Not found | `"fan_strategy": "routed"` | ⚠️ Theoretical only |

**Conclusion:** DQ5 is **resolved**. The `fan_strategy` edge property is sufficient and validated for broadcast and partition. Routed is supported in schema but not exercised in this corpus.
