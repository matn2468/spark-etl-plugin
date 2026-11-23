---
max_turns: 10
allowed_tools: [Read, Glob, Grep, Skill]
---

Our daily enrichment job went from 30 minutes to 2.5 hours with no deploy. Good run plan: BroadcastHashJoin BuildRight on plan_mapping. Bad run plan: SortMergeJoin with Exchange hashpartitioning on both sides. The plan_mapping table went from 6 MB to 45 MB because another team added historical markets. A colleague wants to set autoBroadcastJoinThreshold to 100MB. What do you recommend?
