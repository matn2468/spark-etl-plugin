# 04 — Join strategies: identify, explain, change

## Contents
1. How to see which join Spark used
2. The five physical joins
3. Why Spark chose it (decision order)
4. AQE: initial plan vs final plan
5. How to change the strategy (and when not to)
6. Worked case: the mapping table that grew

---

## 1. How to see which join Spark used

- **Spark UI → SQL tab → query graph**: look for a box named `BroadcastHashJoin`,
  `SortMergeJoin`, `ShuffledHashJoin`, `BroadcastNestedLoopJoin`, `CartesianProduct`.
- **Code**: `df.explain("formatted")` (Spark 3) or `df.explain(True)` (Spark 2). In Scala the same.
- **SQL**: `EXPLAIN FORMATTED SELECT ...`.

Example (formatted plan excerpt):
```
AdaptiveSparkPlan isFinalPlan=true
+- BroadcastHashJoin [plan_code#12], [plan_code#40], Inner, BuildRight, false
   :- Filter isnotnull(plan_code#12)
   :  +- FileScan parquet silver.transactions [..] PartitionFilters: [isnotnull(dt), (dt = 2026-11-10)]
   +- BroadcastExchange HashedRelationBroadcastMode(...)
      +- FileScan parquet ref.plan_mapping [plan_code, revenue_account]
```
`BuildRight` = the right side was broadcast. `Exchange hashpartitioning(...)` on **both** sides
plus `Sort` = sort-merge join.

## 2. The five physical joins

| Join | Plan signature | Shuffle? | When it is right | Danger |
|---|---|---|---|---|
| **BroadcastHashJoin (BHJ)** | `BroadcastExchange` on the small side, `BuildLeft/Right` | No (big side stays in place) | One side small (default ≤ 10 MB estimated) | Broadcast too big → driver OOM, `Futures timed out`, slow broadcast |
| **SortMergeJoin (SMJ)** | `Exchange hashpartitioning` + `Sort` on both sides | Yes, both sides | Two large tables (default for equi-joins) | Shuffle cost, spill, **skew** on hot keys |
| **ShuffledHashJoin (SHJ)** | `Exchange` on both, no `Sort`, `ShuffledHashJoin` | Yes | One side medium, fits per-partition hash map | Per-partition OOM if build side large |
| **BroadcastNestedLoopJoin (BNLJ)** | `BroadcastNestedLoopJoin` | No | Non-equi join with a tiny side | O(n×m): very slow if not tiny |
| **CartesianProduct** | `CartesianProduct` | Yes | Almost never intentional | Row explosion; usually a missing/incorrect join condition |

Range / non-equi conditions (`a.ts BETWEEN b.start AND b.end`) cannot use hash joins → BNLJ or
Cartesian. Fix by adding an equality key (e.g. date bucket) to the condition.

## 3. Why Spark chose it (decision order, equi-joins)

1. **Hints** win: `broadcast(df)` / `/*+ BROADCAST(t) */`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`.
2. Side small enough → **BHJ** if estimated size ≤ `spark.sql.autoBroadcastJoinThreshold`
   (default 10 MB; `-1` disables). Estimate comes from file size/statistics — **after** column
   pruning and filters only if Spark can see them. For outer joins only the non-preserved side can
   be broadcast (left join → right side).
3. `spark.sql.join.preferSortMergeJoin=true` (default) → **SMJ**; otherwise SHJ when applicable.
4. No equality condition → BNLJ (if a side can be broadcast) or Cartesian.

Why it **changed** between runs without code change: input grew past the threshold, stats
changed, a filter stopped being pushed down, a config changed, or AQE decided differently.

## 4. AQE: initial plan vs final plan (Spark 3.x; default on from 3.2)

- `spark.sql.adaptive.enabled` re-plans at each shuffle boundary using **real** sizes.
- It can turn SMJ → BHJ at runtime when the post-filter side is small
  (`spark.sql.adaptive.autoBroadcastJoinThreshold`), coalesce small partitions
  (`coalescePartitions`), and split skewed partitions (`skewJoin.enabled`).
- The UI shows `isFinalPlan=false` while running and `true` when done; `explain()` before
  running shows only the **initial** plan. Always diagnose on the **final** plan, and say which one
  you are reading. If the user pasted an `explain()` output, warn that the final plan may differ.
- Skew handling appears as `SortMergeJoin(skew=true)` / `AQEShuffleRead skewed`.

## 5. How to change the strategy (and when not to)

Prefer making the data smaller over making the threshold bigger:
1. **Project and filter before the join**: select only key + needed columns; filter the dimension
   to relevant/current rows; optionally semi-join it against the distinct keys present in the fact
   partition. A smaller side broadcasts naturally.
2. **Targeted hint** `fact.join(F.broadcast(dim), "key")` instead of a global threshold change
   (a global change affects *every* join in the session).
3. **Raise the threshold** only as a mitigation, after checking driver memory
   (the broadcast is collected on the driver and copied to every executor) and
   `spark.sql.broadcastTimeout` (default 300 s).
4. **Large × large**: keep SMJ but fix partitions (AQE, `shuffle.partitions`), handle skew
   (AQE skew join, salting, isolating hot keys, filtering null keys), or **bucket** both tables on
   the join key if the join repeats daily.
5. **Ensure key uniqueness** on the lookup side — duplicate keys multiply rows (correctness *and*
   performance bug).
6. Match **data types** of keys (string vs bigint forces casts and can block pushdown).

## 6. Worked case: the mapping table that grew

Symptom: daily job 30 min → 2 h+, no deploy, some executor OOM/spill.
Evidence to request: SQL-tab plans of good and bad runs; size of the mapping table then vs now.
Typical finding: good run `BroadcastHashJoin ... BuildRight`; bad run `SortMergeJoin` with
`Exchange hashpartitioning` on the **fact** side, huge shuffle write, long-tail tasks. Mapping table
grew (new markets/history) past 10 MB.
Mitigation: targeted `broadcast()` hint or higher threshold (check driver memory).
Durable fix: select only needed columns, filter to relevant/current mappings (SCD2 `is_current`
or effective dates), enforce key uniqueness, add a size/row-count check on the mapping table and
a data contract with the upstream owner.
Questions to answer explicitly: *Did the job need the new mapping rows?* *Could filtering or
restructuring the table avoid raising the threshold?*
