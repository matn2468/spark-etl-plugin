# ETL Diagnosis — <dag_id>.<task_id> — <logical date>

**Status:** Failed | Slow | Wrong data  ·  **Layer:** Airflow | Engine | Spark | Bronze | Silver | Gold
**Engine / IDs:** <EMR j-.../s-... | Serverless app/run | Glue jr_... | Spark application_...>

## 1. What happened (plain language)
<2–3 sentences anyone on the team understands.>

## 2. Evidence
| Source | Evidence (quoted line / metric / plan node) |
|---|---|
| Airflow task log | |
| Engine log | |
| Spark UI | |

## 3. Root cause
<Most likely cause, with confidence (confirmed / likely / hypothesis) and why.>

## 4. Run comparison (if applicable)
<Table from 06-run-comparison.md, only rows that matter.>

## 5. Fix
- **Immediate mitigation:** <to unblock today's run>
- **Durable fix:** <code/data/config change>
- **Prevention:** <DQ check, alert, contract, test>

## 6. What I still need (if anything)
<From data-request.md — only if confidence is not "confirmed".>

## 7. What to learn from this
<One or two lines: the pattern to recognize next time.>
