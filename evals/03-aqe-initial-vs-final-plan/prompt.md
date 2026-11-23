---
max_turns: 10
allowed_tools: [Read, Glob, Grep, Skill]
---

I ran df.explain() before the job and it shows SortMergeJoin between transactions and plan_mapping, but the Spark UI SQL tab after the run shows BroadcastHashJoin. Which one did Spark actually use and why?
