# What I need to continue

To <diagnose / review / build> this, please bring me the items below **in this order**.
Item 1 is enough to start; send the rest if I ask.

| # | What | Why I need it | How to get it |
|---|---|---|---|
| 1 | <e.g. Full log of the first failed try of task X> | <identify operator + resource ID> | <Airflow UI → DAG → Grid → red square → Logs → try 1 → Download. MWAA: CloudWatch group `airflow-<env>-Task`> |
| 2 | <e.g. EMR step stderr> | <real exception> | <EMR console → cluster j-… → Steps → step → View logs → stderr; or S3 `<log-uri>/j-…/steps/s-…/stderr.gz`> |
| 3 | | | |

**Tips:** paste text rather than screenshots when possible; remove secrets, account IDs or
personal data before sharing; for long logs, paste the last ~150 lines plus every line containing
`ERROR`, `Exception`, `Caused by`, or an ID (`j-`, `s-`, `jr_`, `application_`).
