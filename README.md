# GRIT — Granular Rollup Incremental Table  
**A “hybrid materialized view” pattern for Snowflake + MWAA**  
*Pronounced like “grit”: it makes your analytics tough and fast.*

> **GRIT = Granular Rollup Incremental Table** — a nightly precomputed aggregate **table** (the “materialization”) plus thin **views** on top. It delivers blazing‑fast queries for year/month/week/day without Snowflake MVs, keeps costs predictable, and avoids double‑counting.

---

## Why GRIT (vs. “just a view”)?

**Views don’t store data**—they recompute at query time. GRIT stores the results *once per day* (via MWAA/Airflow), so daytime queries read a **small, prunable table** instead of scanning your giant fact tables.

**Benefits**
- **Performance & cost predictability:** Pay compute once nightly; cheap daytime scans.
- **Multi‑grain flexibility:** Year / Month / Week / Day in one table with a clear `grain` column.
- **Concurrency:** Dozens of BI users = tiny scans, not multiple re‑aggregations.
- **Operational control:** Deterministic refresh windows, backfills, snapshots, rollbacks.
- **Governance:** Clean views, labels, and guardrails to stop double-counting.


---

## 📖 Origin Story: Why GRIT Exists


## 🧩 Cross-Grain Composition (CGC)

GRIT implements a powerful modeling principle we call **Cross-Grain Composition (CGC)**.

> **Cross-Grain Composition** is the practice of storing multiple levels of time aggregation (day, week, month, year) in a single, canonical table — then composing queries *across those grains* as needed.

This pattern allows you to:
- Blend granular and summary insights with **minimal re-compute**
- Enable analysts to drill down or roll up **without switching tables**
- Use a consistent schema for **dashboards, alerts, and audits**
- Avoid the common trap of duplicating logic and data across multiple grain-specific tables

Think of it as **semantic zooming for data** — one surface, many resolutions.

The GRIT table is the physical layer that makes Cross-Grain Composition possible.


The GRIT pattern was independently developed by **Troy Johnson at Chewy**, a seasoned data engineering manager who recognized a major gap in traditional analytics architecture.

Like many engineers, Troy had built and used countless pre-aggregated views and summary tables over nearly a decade. But one day—before his coffee kicked in—he had a realization:

> “Why do we keep building *one grain per table* when most queries need multiple?”

That lightbulb moment led to the core design of GRIT: a single table with multiple grains (day/week/month/year), explicitly labeled with a `grain` column and canonical `period_start`, refreshed nightly with Airflow.

Since then, GRIT has become a best-practice pattern used for blazing-fast analytics, operational resilience, and flexible rollups.

---


## High-level architecture

```
            +--------------------+
            |   Source Facts     |   (e.g., OPS_ORDERS)
            +---------+----------+
                      |
                      | nightly ETL (MWAA/Airflow → Snowflake)
                      v
            +--------------------+
            |    GRIT Table      |   (precomputed multi-grain aggregates)
            |  agg_metrics       |
            +---------+----------+
                      |
                      | grain-pinned views (safe surfaces)
                      v
        +-------------+--------------+
        |  V_AGG_BY_DAY / WEEK / ... |  (analyst- & BI-friendly)
        +-----------------------------+
```

---

## Core design decisions

1. **Single table, multiple grains:** `grain` column ∈ {`day`,`week`,`month`,`year`} with **canonical `period_start`** (date).  
2. **Weeks are dates, not numbers:** store the **week’s start date** (`DATE_TRUNC('week', <date>)`), not “Week 23.” Labels can be derived in views.  
3. **Calendar choice is explicit:** ISO/UTC by default; 4‑4‑5 uses a `DIM_CALENDAR`.  
4. **Incremental refresh with a straggler window:** nightly MERGE plus a trailing N‑day recompute to correct late arrivals.  
5. **Grain‑pinned views:** keep ad‑hoc users safe from double counting.  
6. **Optional atomic swap:** build in staging, validate, then `SWAP` for zero downtime.  

---

## Schema (Snowflake)

> Replace dimensions/metrics with your real ones (e.g., `carrier_id`, `lane_id`, `ship_node_id`, `service_level`).

```sql
create table if not exists AGG_METRICS (
  grain            string,          -- 'day'|'week'|'month'|'year'
  period_start     date,            -- canonical start of period (Mon for ISO week; 1st for month; Jan-1 for year)

  -- dimensions
  carrier_id       number,
  ship_node_id     number,

  -- metrics
  shipments        number,
  weight_lbs       number,
  cost_usd         number,

  -- housekeeping
  last_updated_at  timestamp_ntz default current_timestamp(),

  constraint pk_agg_metrics unique (grain, period_start, carrier_id, ship_node_id)
);

-- Optional pruning hint for large tables:
alter table AGG_METRICS cluster by (grain, period_start);
```

**Labels in the view (not the table):** add `period_label` in the view via `TO_CHAR` so storage stays canonical.

---

## Nightly build SQL (incremental MERGE with straggler window)

Parameterize in Airflow as Variables.

```sql
-- Parameters (Airflow renders these as literals)
set N_DAYS := 400;         -- how far back to scan base facts
set STRAGGLER_DAYS := 28;  -- trailing window to correct late arrivals

with base as (
  select
    cast(o.ship_date as date) as d,
    o.carrier_id,
    o.ship_node_id,
    1 as shipments,
    o.weight_lbs,
    o.cost_usd
  from OPS_ORDERS o
  where o.ship_date >= dateadd(day, -$N_DAYS, current_date)
),
by_day as (
  select
    'day' as grain,
    d as period_start,
    carrier_id, ship_node_id,
    count(*)        as shipments,
    sum(weight_lbs) as weight_lbs,
    sum(cost_usd)   as cost_usd
  from base
  group by 1,2,3,4
),
by_week as (
  select
    'week' as grain,
    date_trunc('week', d) as period_start, -- ISO Monday start; use DIM_CALENDAR for fiscal
    carrier_id, ship_node_id,
    count(*)        as shipments,
    sum(weight_lbs) as weight_lbs,
    sum(cost_usd)   as cost_usd
  from base
  group by 1,2,3,4
),
by_month as (
  select
    'month' as grain,
    date_trunc('month', d) as period_start,
    carrier_id, ship_node_id,
    count(*)        as shipments,
    sum(weight_lbs) as weight_lbs,
    sum(cost_usd)   as cost_usd
  from base
  group by 1,2,3,4
),
by_year as (
  select
    'year' as grain,
    date_trunc('year', d) as period_start,
    carrier_id, ship_node_id,
    count(*)        as shipments,
    sum(weight_lbs) as weight_lbs,
    sum(cost_usd)   as cost_usd
  from base
  group by 1,2,3,4
),
all_grains as (
  select * from by_day
  union all select * from by_week
  union all select * from by_month
  union all select * from by_year
)
merge into AGG_METRICS t
using all_grains s
  on  t.grain = s.grain
  and t.period_start = s.period_start
  and t.carrier_id = s.carrier_id
  and t.ship_node_id = s.ship_node_id
when matched then update set
  shipments       = s.shipments,
  weight_lbs      = s.weight_lbs,
  cost_usd        = s.cost_usd,
  last_updated_at = current_timestamp()
when not matched then insert (
  grain, period_start, carrier_id, ship_node_id, shipments, weight_lbs, cost_usd
) values (
  s.grain, s.period_start, s.carrier_id, s.ship_node_id, s.shipments, s.weight_lbs, s.cost_usd
);

-- Optional: clean/recompute trailing window to fix late data precisely
delete from AGG_METRICS
where period_start >= dateadd(day, -$STRAGGLER_DAYS, date_trunc('day', current_date))
  and grain in ('day','week','month'); -- year seldom needs it
```

### Atomic refresh variant (CTAS + SWAP)

This pattern avoids partial refresh visibility and gives instant rollback via clones.

```sql
create or replace transient table AGG_METRICS_STAGING as
with base as (...)
-- (same CTEs as above)
select * from all_grains;

-- (optional) lightweight validations on STAGING here
-- e.g., compare total shipments vs. yesterday within ±X%

-- Zero-downtime swap:
alter table if exists AGG_METRICS swap with AGG_METRICS_STAGING;

-- Optional: keep a 7-day clone history for rollback
-- create table AGG_METRICS_2025_09_08 clone AGG_METRICS;  -- timestamped snapshot
```

---

## Views (friendly, safe, and consistent)

```sql
create or replace view V_AGG_METRICS as
select
  grain,
  period_start,
  case
    when grain = 'week'  then to_char(period_start, 'YYYY-"W"IW')
    when grain = 'month' then to_char(period_start, 'YYYY-MM')
    when grain = 'year'  then to_char(period_start, 'YYYY')
    else to_char(period_start, 'YYYY-MM-DD')
  end as period_label,
  carrier_id,
  ship_node_id,
  shipments,
  weight_lbs,
  cost_usd,
  last_updated_at
from AGG_METRICS;

create or replace view V_AGG_BY_DAY   as select * from V_AGG_METRICS where grain='day';
create or replace view V_AGG_BY_WEEK  as select * from V_AGG_METRICS where grain='week';
create or replace view V_AGG_BY_MONTH as select * from V_AGG_METRICS where grain='month';
create or replace view V_AGG_BY_YEAR  as select * from V_AGG_METRICS where grain='year';
```

> **Tip:** If you use 4‑4‑5 or custom fiscal periods, join to `DIM_CALENDAR` during build to set `period_start` properly and change `period_label` to a fiscal label.

---

## Example analyst queries

```sql
-- Last 13 weeks by node
select period_start, period_label, ship_node_id, shipments
from V_AGG_BY_WEEK
where period_start >= dateadd(week, -13, date_trunc('week', current_date))
order by period_start;

-- Last 12 months by carrier
select period_start, period_label, carrier_id, shipments, cost_usd
from V_AGG_BY_MONTH
where period_start >= dateadd(month, -12, date_trunc('month', current_date))
order by period_start;

-- YTD snapshot (one row per dim combo)
select *
from V_AGG_BY_YEAR
where period_start = date_trunc('year', current_date);
```

---

## MWAA / Airflow DAG (SnowflakeOperator)

```python
# dags/grit_agg.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DEFAULT_ARGS = {
    "owner": "tnt-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="grit_agg",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 3 * * *",  # nightly 03:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["agg","grit","snowflake"],
) as dag:

    N_DAYS = Variable.get("GRIT_N_DAYS", default_var="400")
    STRAGGLER_DAYS = Variable.get("GRIT_STRAGGLER_DAYS", default_var="28")

    sql = f\"""
    set N_DAYS := {N_DAYS};
    set STRAGGLER_DAYS := {STRAGGLER_DAYS};
    -- Paste the MERGE SQL from the README here
    -- (base/by_day/by_week/by_month/by_year/all_grains + MERGE + optional delete)
    \"""

    build_agg = SnowflakeOperator(
        task_id="build_grit_agg",
        snowflake_conn_id="snowflake_default",
        sql=sql,
        autocommit=True,
    )

    build_agg
```

**QC add-ons (recommended):**
- Row deltas vs. yesterday (`|Δshipments| <= 20%`).
- Non-null checks on keys.
- “Hot window” counts (last 7/28 days) within historical bounds.
- Write results to an **audit table** and alert on failures (Email/Slack).

---

## Fiscal calendars (4‑4‑5) and custom weeks

If you’re on a retail 4‑4‑5:
- Build a `DIM_CALENDAR` with columns like `calendar_date`, `fiscal_week_start`, `fiscal_month_start`, `fiscal_year_start`, `fiscal_week_label`, `fiscal_month_label`, etc.
- During the nightly build, join `base` to `DIM_CALENDAR` to replace `DATE_TRUNC('week'|...)` with the fiscal equivalents.
- Change views to label with fiscal strings (`FY25 P05 W02`).

---

## Performance & storage guidance

- **Clustering:** `(grain, period_start)` is usually enough. Add a selective dim **only if** pruning is poor.
- **Day grain history:** Consider capping day‑grain to last **N** days (e.g., 180) if daily history isn’t queried.
- **Search Optimization Service:** Consider for pinpoint predicates on high-cardinality dims; it works only on tables.
- **Warehouse sizing:** The nightly build can run on a medium/large WH; daytime queries should succeed on small/XS.

---

## Backfills & rollbacks

- **Backfill:** Parameterize `N_DAYS` or add `backfill_start/backfill_end` to rerun historical windows.
- **Atomic swap:** Keep yesterday’s clone to **flip back** instantly if validations fail post‑deploy.
- **Time Travel:** Use to validate diffs or recover accidental deletes.

---

## Guardrails to avoid double counting

- Always filter by a **single grain** in end-user views (`V_AGG_BY_*`).  
- If analysts need multi-grain comparisons, do it in **separate CTEs** and join on keys—don’t union and sum blindly.  
- Educate users that `period_start` is canonical; `period_label` is display only.

---

## Common gotchas

- **Ambiguous weeks:** Never store “Week 23.” Use `period_start` date.
- **TZ drift:** Derive periods from UTC or a fixed business TZ consistently.
- **Late-arriving data:** Set `STRAGGLER_DAYS` ≥ your observed tail (often 14–35d in logistics).
- **Fiscal flips:** Document the calendar; don’t mix ISO and fiscal in the same table without clear columns.

---

## Suggested repo structure

```
grit/
├─ dags/
│  └─ grit_agg.py
├─ sql/
│  ├─ create_agg_metrics.sql
│  ├─ build_grit_merge.sql
│  └─ build_grit_ctas_swap.sql
├─ views/
│  └─ create_views.sql
├─ docs/
│  └─ README.md  <-- this file
└─ scripts/
   └─ validate_grit.sql  (QC checks; optional)
```

---

## TL;DR one-liner (for your team)

> **GRIT = Granular Rollup Incremental Table**: a nightly Snowflake build that stores year/month/week/day aggregates in one table with a canonical `period_start`. Views make it safe to query, costs stay predictable, and performance is 🔥.

---

### Appendix: quick 4‑4‑5 example (pattern)

```sql
-- DIM_CALENDAR has fiscal_* columns already computed.
with base as (
  select cast(o.ship_date as date) as d, o.carrier_id, o.ship_node_id, 1 as shipments, o.weight_lbs, o.cost_usd
  from OPS_ORDERS o
  where o.ship_date >= dateadd(day,-$N_DAYS,current_date)
),
fc as (
  select dc.calendar_date as d,
         dc.fiscal_week_start as week_start,
         dc.fiscal_month_start as month_start,
         dc.fiscal_year_start as year_start
  from DIM_CALENDAR dc
  where dc.calendar_date >= dateadd(day,-$N_DAYS,current_date)
),
base_fiscal as (
  select b.*, f.week_start, f.month_start, f.year_start
  from base b join fc f on b.d = f.d
)
-- group by base_fiscal.week_start / month_start / year_start instead of DATE_TRUNC(...)
```

---

**Authored for: Chewy TNT (Transportation) — Snowflake + MWAA stack**  
**Pattern owner:** Troy “GRIT” Johnson 😎

---

## ⏱️ Combining GRIT with Real-Time Data

GRIT is built nightly. But what if you need “*last 3 weeks up to now*”? The current day (today) isn’t in the GRIT table yet.

Here’s how to query it:

```sql
-- Part 1: Historical (precomputed) data
select *
from V_AGG_BY_WEEK
where period_start >= dateadd(week, -3, date_trunc('week', current_date))
  and period_start < date_trunc('week', current_date)  -- exclude this week (incomplete)

union all

-- Part 2: Real-time aggregation (today → now)
select
  'week' as grain,
  date_trunc('week', ship_date) as period_start,
  carrier_id,
  ship_node_id,
  count(*)        as shipments,
  sum(weight_lbs) as weight_lbs,
  sum(cost_usd)   as cost_usd,
  current_timestamp() as last_updated_at
from OPS_ORDERS
where ship_date >= date_trunc('week', current_date)
group by 1,2,3,4;
```

> This pattern keeps queries fast **and current**. Just ensure your base table (`OPS_ORDERS`) has enough freshness to cover “today.” You can wrap this logic in a view or use a macro for consistency.
