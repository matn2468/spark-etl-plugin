# Optimization — <ETL name>

## 1. What the ETL does (confirm)
Inputs → outputs, grain, key business rules.

## 2. Optimized code
```python
# full rewritten ETL
```

## 3. Change log
| # | Change | Why | Expected impact (runtime / shuffle / files / cost / readability) |
|---|---|---|---|

## 4. Equivalence notes
| Change | Output identical? | Notes (intentional differences, e.g. duplicates removed) |
|---|---|---|

## 5. Risks & assumptions to validate

## 6. Validation plan
1. Run old and new on the same partition (non-prod).
2. Compare: row counts, key uniqueness, sums of key measures, `old.exceptAll(new)` and
   `new.exceptAll(old)` both empty (or differences explained).
3. Compare Spark UI: duration, shuffle read/write, spill, number of jobs, files written.
