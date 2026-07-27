# PERFORMANCE AUDIT — Υποδομή BI (2026-07-27)

Μετρήσεις σε live production (bi.boxvisio.com). Αντικαθιστά/επικαιροποιεί το
`PERFORMANCE_AUDIT.md` (2026-03-08), του οποίου τα νούμερα έχουν παλιώσει δραματικά.

## 0. Βασικά μεγέθη

| Πόρος | Τιμή |
|---|---|
| vCPU / RAM | 12 / 22 GB |
| Δίσκος | 150 GB, 104 GB σε χρήση (**73%**), 41 GB ελεύθερα |
| Load average | 2.3 / 1.9 / 1.9 |
| CPU postgres | **144% μόνιμα** |
| CPU api | 42% (σε **1 μόνο process**) |
| Μέγεθος DB | pharmacy295 39 GB · danihltsitoura 36 GB · zisopoulosph 3.2 GB |

---

## 1. Η PostgreSQL τρέχει με εργοστασιακές default ρυθμίσεις  🔴 ΚΡΙΣΙΜΟ

Το `docker-compose.yml` περνάει μόνο security flags στην postgres — **καμία** ρύθμιση μνήμης/IO.

| Παράμετρος | Τώρα | Πρέπει |
|---|---|---|
| `shared_buffers` | **128 MB** | 6–8 GB |
| `work_mem` | **4 MB** | 64–128 MB |
| `maintenance_work_mem` | 64 MB | 2 GB |
| `effective_cache_size` | 4 GB | 16 GB |
| `random_page_cost` | **4** (τιμή για HDD) | 1.1 (SSD) |
| `effective_io_concurrency` | **1** | 200 |
| `max_parallel_workers_per_gather` | 2 | 4 |
| `max_wal_size` | 1 GB | 8 GB |
| `wal_compression` | off | on |
| `track_io_timing` | off | on |
| `pg_stat_statements` | **δεν είναι φορτωμένο** | φόρτωση |

**Απόδειξη επίπτωσης:**
- Cache hit ratio: **72.2%** (φυσιολογικό για BI: 99%+)
- Block reads από δίσκο στο pharmacy295: **127.704.435.938** (~1 PB)
- Temp files: **453.832 αρχεία / 5.806 GB spill σε δίσκο** — κάθε sort/hash/group-by ξεχειλίζει επειδή το `work_mem` είναι 4 MB
- Παράδειγμα: απλό monthly group-by στο `fact_sales` → `Buffers: shared hit=4446 read=384430`. Δηλαδή **3 GB διαβάστηκαν από δίσκο** για ένα query 13 γραμμών αποτελέσματος.

**Κέρδος:** το μεγαλύτερο μονόπλευρο win. Απαιτεί restart postgres (~30 δευτ. downtime).

---

## 2. Τα aggregates ξαναχτίζονται ΟΛΟΚΛΗΡΑ σε κάθε sync  🔴 ΚΡΙΣΙΜΟ

Στο [worker/tasks.py:4980](worker/tasks.py#L4980), όταν δεν δοθεί ρητό date range, το task κάνει
`SELECT MIN(doc_date), MAX(doc_date) FROM <fact_table>` και **ξαναχτίζει όλο το ιστορικό**.
Το μοτίβο κάθε refresh είναι `DELETE FROM agg_* WHERE date BETWEEN ...` → `INSERT INTO agg_* SELECT ...`
(δεν υπάρχει upsert). Το `incremental-sync-all-tenants` τρέχει **κάθε 1 λεπτό**.

**Απόδειξη — `pg_stat_user_tables` (pharmacy295):**

| Πίνακας | Ζωντανές γραμμές | n_tup_ins | n_tup_del | seq_tup_read |
|---|---|---|---|---|
| `agg_stock_aging` | 1.9 M | **2.109.932.112** | **5.320.343.881** | 10.5 δισ. |
| `agg_inventory_snapshot_daily` | 356 K | **1.845.749.357** | 1.624.415.308 | 2.9 δισ. |
| `fact_inventory` | 2.8 M | 10.9 M | 8.2 M | **492.271.005.460** (340.252 seq scans) |
| `agg_sales_daily` | 595 K | 64.882.075 | 64.826.699 | 192 M |

Ο `agg_stock_aging` έχει ξαναχτιστεί ~2.800 φορές. Αυτό είναι που κρατάει την postgres στο 144% CPU
μόνιμα και είναι η γενεσιουργός αιτία όλου του bloat της §3.

**Τι πρέπει:**
1. Περιορισμός παραθύρου: refresh μόνο στις ημερομηνίες που όντως άλλαξαν (ή τελευταίες N ημέρες), όχι MIN/MAX ιστορικού.
2. `INSERT ... ON CONFLICT DO UPDATE` αντί για DELETE+INSERT — μηδενίζει το dead-tuple churn.
3. Debounce: συγχώνευση refresh requests ανά tenant/entity (π.χ. 1 ανά 10 λεπτά) αντί για ένα ανά sync.

---

## 3. Ακραίο bloat σε πίνακες και indexes  🔴 ΚΡΙΣΙΜΟ

Άμεση συνέπεια της §2.

| Αντικείμενο | Πραγματικά δεδομένα | Τώρα στον δίσκο | Παράγοντας |
|---|---|---|---|
| `agg_stock_aging` — indexes | ~250 MB | **17 GB** | ~68× |
| ↳ `agg_stock_aging_pkey` | ~40 MB | **5.403 MB** | 135× (και **0 scans**) |
| `agg_inventory_snapshot_daily` — indexes | ~150 MB | **3.925 MB** | ~26× |
| `dim_items` — table | 193.211 γρ. × 423 B = **82 MB** | **3.058 MB** | **37×** |
| `dim_items` — indexes | — | 1.373 MB | — |
| `fact_sales` — table | ~1.8 GB | 3.038 MB | ~1.7× |

**Αιτία του `dim_items`:** **355.857.994 UPDATE**, εκ των οποίων μόνο **14.768 HOT** (0.004%).
Κάθε sync ξαναγράφει κάθε γραμμή item (μαζί με `updated_at = now()`), και επειδή ο πίνακας έχει
13 indexes, κάθε UPDATE γράφει και 13 index entries. Μηδενικό HOT ⇒ ατέρμονο bloat.

**Ανακτήσιμος χώρος στο pharmacy295: ~22 GB.**

**Τι πρέπει:**
- `REINDEX CONCURRENTLY` σε όλους τους agg_* + `dim_items` (χωρίς downtime).
- `VACUUM FULL` / `pg_repack` σε `dim_items`, `agg_stock_aging`, `agg_inventory_snapshot_daily`.
- `ALTER TABLE dim_items SET (fillfactor = 80)` για να επιτραπούν HOT updates.
- Στο upsert του `dim_items`: `WHERE ... IS DISTINCT FROM ...` ώστε να μην γράφονται αμετάβλητες γραμμές.

---

## 4. 302 αχρησιμοποίητα indexes = 16 GB  🟠 ΥΨΗΛΟ

Στο pharmacy295: **302 indexes με `idx_scan = 0`, συνολικά 16.029 MB**.

Τα μεγαλύτερα:

| Index | Μέγεθος | Scans |
|---|---|---|
| `agg_stock_aging_pkey` | 5.403 MB | 0 |
| `ix_agg_stock_aging_item_external_id` | 1.608 MB | 0 |
| `agg_inventory_snapshot_daily_pkey` | 1.594 MB | 0 |
| `ix_agg_stock_aging_branch_ext_id` | 1.575 MB | 0 |
| `ix_agg_stock_aging_bucket` | 1.561 MB | 0 |
| `ix_agg_stock_aging_days` | 1.558 MB | 0 |
| `ix_dim_items_updated_at` | 255 MB | 1 |
| `ix_dim_items_name` / `_sku` / `_barcode` / `_commercial_status` / `_manufacturer_code` / `_replenishment_status_1` / `_2` | ~610 MB σύνολο | 0–1 |

Δεν κοστίζουν μόνο χώρο — κάθε ένα από τα 355 M updates του `dim_items` και τα 2.1 δισ. inserts του
`agg_stock_aging` πληρώνει write amplification για όλα αυτά.

---

## 5. Το executive dashboard έχει κάνει regression σε 10–20 δευτερόλεπτα  🔴 ΚΡΙΣΙΜΟ

Από τα live `app.kpi_perf` logs σήμερα, `/v1/dashboard/executive-summary` σε cache MISS:

```
api_time_ms: 19754  (db 19598, 10 queries)
api_time_ms: 12406  (db 12268)
api_time_ms: 12160  (db 11977)
api_time_ms: 10290  (db 10133)
```

Το audit του Μαρτίου μετρούσε **69 ms** στην ίδια διαδρομή.

Δύο queries κυριαρχούν:
- `sales_company_month_doc_amounts` (monthly totals) — **6.5–7.3 s**
- warehouse breakdown πάνω στο `fact_sales` — **2.9–9.7 s**

**Αιτία** — [kpi_queries.py:7876](backend/app/services/kpi_queries.py#L7876): η `sales_monthly_company_totals`
χρησιμοποιεί το γρήγορο `agg_sales_monthly` **μόνο αν δεν υπάρχουν** turnover/behavior business rules:

```python
if not (_has_sales_turnover_series_rules() or _has_sales_behavior_rules()):
    ...  # γρήγορο μονοπάτι από agg_sales_monthly
# αλλιώς: full scan του fact_sales (3 GB) με 2-επίπεδο GROUP BY ανά document
```

Ο tenant έχει ενεργά rules ⇒ το γρήγορο μονοπάτι **δεν ενεργοποιείται ποτέ**.

Επιπλέον, το `fact_sales` κουβαλάει στήλη `source_payload_json` με **avg_width 886 bytes** — δηλαδή
~60% κάθε seq scan διαβάζει raw JSON payload που κανένα analytic query δεν χρειάζεται.

**Τι πρέπει:**
1. Υλοποίηση rule-aware monthly/warehouse aggregates στο agg layer, ώστε το dashboard να μην πέφτει ποτέ σε fact scan.
2. Μεταφορά του `source_payload_json` σε πλαϊνό πίνακα (ή `SET STORAGE EXTERNAL`) — μειώνει το `fact_sales` κατά ~60%.

Άλλα αργά endpoints από τα ίδια logs: `/v1/kpi/eshop/analysis` 2.818 ms, `/v1/kpi/pos/by-category`
684 ms, `/v1/kpi/sales/filter-options` 649–782 ms.

---

## 6. Το API τρέχει σε 1 μόνο process  🟠 ΥΨΗΛΟ

```
/opt/cloudon-bi/.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Χωρίς `--workers`. Σε μηχάνημα 12 πυρήνων, **όλη η HTTP κίνηση εξυπηρετείται από 1 πυρήνα**, ήδη στο 42%.
Ένα αργό query των 12 δευτ. μπλοκάρει event-loop χρόνο για όλους.

Επιπλέον υπάρχουν **14 στοιβαγμένα `BaseHTTPMiddleware`** ([main.py:70-83](backend/app/main.py#L70-L83)),
που εφαρμόζονται και στα `/static`. Κάθε `BaseHTTPMiddleware` προσθέτει ένα anyio task wrapper ανά request.

**Τι πρέπει:** `--workers 4` (μαζί με §9), και μετατροπή των βαρύτερων middleware σε καθαρό ASGI.

---

## 7. Στατικά αρχεία χωρίς caching  🟡 ΜΕΣΑΙΟ

Το `/static` σερβίρεται από FastAPI `StaticFiles` μέσω proxy. Απάντηση:

```
last-modified: Wed, 03 Jun 2026 06:11:10 GMT
etag: "ce8b7560ca4ef6dcdbd85617d6bbfa32"
(κανένα Cache-Control)
```

Χωρίς `Cache-Control` ο browser κάνει revalidation σε **κάθε** asset σε **κάθε** page load — ~30 επιπλέον
round-trips ανά σελίδα, όλα περνώντας από τα 14 middleware.

**Τι πρέπει:** `location /static/ { alias ...; expires 1y; add_header Cache-Control "public, immutable"; }`
στο nginx με versioned filenames.

---

## 8. Τα staging tables δεν καθαρίζονται ποτέ  🟠 ΥΨΗΛΟ

Ο tenant `danihltsitoura` είναι 36 GB, εκ των οποίων **~34 GB είναι `stg_*`**:

| Πίνακας | Μέγεθος | Γραμμές | Dead tuples |
|---|---|---|---|
| `stg_inventory_documents` | **14 GB** | 6.198.777 | 907.655 |
| `stg_cash_transactions` | **13 GB** | 11.264.617 | 1.148.830 |
| `stg_purchase_documents` | 5.614 MB | 3.944.059 | 280.682 |
| `stg_expense_documents` | 1.900 MB | 1.554.756 | 220.369 |
| `stg_sales_documents` | 1.278 MB | 594.623 | 31.578 |

Στο `stg_inventory_documents`: **100% των γραμμών είναι `transform_status = 'processed'`**,
παλαιότερες από 2026-05-26. Κρατάμε 14 GB raw JSON που έχει ήδη μετασχηματιστεί.

**Τι πρέπει:** retention job που διαγράφει processed staging > 7–14 ημερών (ή partition by month + DROP).

---

## 9. Επικίνδυνα μαθηματικά connection pool  🟡 ΜΕΣΑΙΟ

`DB_POOL_SIZE=10`, `DB_MAX_OVERFLOW=20` **ανά engine**, και δημιουργείται ξεχωριστό engine ανά tenant
συν το control ([tenant_manager.py:35](backend/app/db/tenant_manager.py#L35)).
Με 3 tenants: 4 × 30 = **120 connections** έναντι `max_connections = 100`.

Με `--workers 4` (§6) γίνεται 480. **Χρειάζεται pgbouncer** (transaction pooling) ή μικρότερα per-engine pools
πριν προστεθούν workers.

---

## 10. Παρατηρησιμότητα 🟡 ΜΕΣΑΙΟ

- `pg_stat_statements`: διαθέσιμο αλλά **μη φορτωμένο** — δεν υπάρχει καθόλου ranking αργών queries σε επίπεδο DB.
- `log_min_duration_statement = -1` — τίποτα δεν καταγράφεται.
- `track_io_timing = off` — τα `EXPLAIN (ANALYZE, BUFFERS)` δεν δείχνουν I/O χρόνο.
- Redis: 38 keys, 11.7 MB, hit ratio **53.5%**, `maxmemory` **απεριόριστο** με policy `noeviction`
  (κίνδυνος OOM αν μεγαλώσει το cache).

---

## Πλάνο εφαρμογής

### Φάση 1 — Άμεσα, χαμηλό ρίσκο (1 ημέρα) · αναμενόμενο κέρδος 3–5×
1. Tuning postgres.conf + restart (§1) — **το μεγαλύτερο single win**
2. `REINDEX CONCURRENTLY` σε agg_* + dim_items (§3) — χωρίς downtime, ~20 GB πίσω
3. Διαγραφή των 302 αχρησιμοποίητων indexes (§4) — 16 GB + λιγότερο write amplification
4. Purge processed staging στον danihltsitoura (§8) — ~34 GB πίσω
5. Cache-Control για /static στο nginx (§7)
6. Φόρτωση `pg_stat_statements` + `log_min_duration_statement = 1000` (§10)

Ο δίσκος πάει από 73% → ~35%.

### Φάση 2 — Δομικά, μεσαίο ρίσκο (2–4 ημέρες) · κέρδος 5–20× στα βαριά paths
7. Περιορισμός παραθύρου refresh + upsert αντί DELETE+INSERT + debounce (§2)
8. Rule-aware monthly/warehouse aggregates ώστε το dashboard να μην αγγίζει το `fact_sales` (§5)
9. `fillfactor` + no-op-skip στο upsert του `dim_items` (§3)

### Φάση 3 — Κλιμάκωση (1–2 ημέρες)
10. pgbouncer (§9), μετά `uvicorn --workers 4` (§6)
11. Μεταφορά `source_payload_json` εκτός `fact_sales` (§5)
12. Μείωση/μετατροπή των 14 middleware σε ASGI (§6)

### Αναμενόμενο τελικό αποτέλεσμα
| Μετρική | Τώρα | Στόχος |
|---|---|---|
| Executive dashboard (MISS) | 10–20 s | < 300 ms |
| Cache hit ratio postgres | 72% | > 99% |
| Temp spill | 5.8 TB | ~0 |
| CPU postgres (idle load) | 144% | < 30% |
| Χρήση δίσκου | 73% | ~35% |

---

# ΑΠΟΤΕΛΕΣΜΑΤΑ ΕΦΑΡΜΟΓΗΣ — 2026-07-27

Όλα τα παραπάνω εφαρμόστηκαν σε live production την ίδια ημέρα.

## Μετρημένα αποτελέσματα

| Μετρική | Πριν | Μετά | Βελτίωση |
|---|---|---|---|
| Βαρύ query dashboard (monthly rollup) | 6.512–7.291 ms | **1.032–1.053 ms** | **6,3×** |
| Cache hit ratio (pharmacy295) | 72,2% | **95,2%** | — |
| CPU postgres σε ηρεμία | **144%** | **7,9%** | **18×** |
| Temp file spill | 5.806 GB | ~0 | — |
| Χρήση δίσκου | 73% (104 GB) | **29% (41 GB)** | 63 GB πίσω |
| DB pharmacy295 | 39 GB | **8,7 GB** | 4,5× |
| DB danihltsitoura | 36 GB | **320 MB** | 115× |
| `agg_stock_aging` (indexes) | 17 GB | **140 MB** | 124× |
| `dim_items` (heap) | 3.058 MB | **86 MB** | 36× |
| API processes | 1 | **4** | 4× |

## Κρίσιμο εύρημα εκτός σχεδίου: εξαντλημένα int4 sequences

Κατά την ανάλυση των logs βρέθηκε **ενεργό production incident** που δεν φαινόταν στο UI:

```
SequenceGeneratorLimitExceededError: nextval: reached maximum value of
sequence "agg_stock_aging_id_seq" (2147483647)
```

Το `agg_stock_aging_id_seq` είχε φτάσει 100% του int4 και **το inventory aggregate
refresh αποτύγχανε εντελώς** — τα inventory KPI δεν ενημερώνονταν καθόλου. Το
`agg_inventory_snapshot_daily_id_seq` ήταν στο 88,3% και θα ακολουθούσε σε ημέρες.
Άμεση συνέπεια των 2,1 δισ. inserts της §2.

Διορθώθηκε: και τα 20 `agg_*` PK σε **bigint** και στους 3 tenants (το
`agg_inventory_snapshot_daily` χρειάστηκε drop/recreate του `agg_inventory_snapshot`
view). Επαληθεύτηκε: refresh ολοκληρώθηκε σε 123 s για όλο το ιστορικό
(2016-01-04 → 2026-07-27) και το sequence πέρασε ομαλά το παλιό όριο (2.150.216.272).

Καλύφθηκε και για νέους tenants: models σε `BigInteger` + migration
`20260727_0041_tenant_agg_bigint_pks.py`.

## Τι έγινε ακριβώς

**Υποδομή**
- postgres: `shared_buffers` 128 MB → **10 GB**, `work_mem` 4 MB → 64 MB,
  `maintenance_work_mem` → 1 GB, `random_page_cost` 4 → 1.1,
  `effective_io_concurrency` 1 → 200, `max_parallel_workers_per_gather` 2 → 4,
  `max_wal_size` → 8 GB, `wal_compression` on, autovacuum cost limit 200 → 2000
- `jit = off` — μετρήθηκε ~545 ms compile overhead σε query 1,2 s· σταθερά ~10% χειρότερο
- `pg_stat_statements` + `track_io_timing` + `log_min_duration_statement=1000`
- `max_connections` 100 → 300, `DB_POOL_SIZE` 10 → 5, `DB_MAX_OVERFLOW` 20 → 10
- `uvicorn --workers 4`
- nginx: `/static/` σερβίρεται απευθείας από δίσκο, versioned assets με
  `Cache-Control: public, max-age=31536000, immutable`

**Το μέγεθος του shared_buffers ήταν το κρίσιμο σημείο.** Στα 6 GB το cache hit ratio
έμενε στο 12% όσες φορές κι αν διαβαζόταν ο ίδιος πίνακας. Αιτία: η Postgres σαρώνει
πίνακες μεγαλύτερους από `shared_buffers/4` μέσω ενός ring buffer 256 KB και δεν τους
κρατάει ποτέ. Το heap του `fact_sales` (1.718 MB) ήταν μόλις 12% πάνω από το τότε
όριο των 1.536 MB. Στα 10 GB το όριο ανέβηκε στα 2,5 GB και το hit ratio πήγε 95%.

**Καθαρισμός δεδομένων**
- 168 αχρησιμοποίητα indexes >1 MB διαγράφηκαν (rollback DDL στο
  `artifacts/perf-2026-07/rollback_indexes_*.sql`)
- `REINDEX CONCURRENTLY` σε όλους τους bloated πίνακες
- 23,5 εκατ. processed staging rows διαγράφηκαν (34 GB)
- `VACUUM FULL` + `ANALYZE` σε fact/dim/agg tables

**Κώδικας**
- `worker/tasks.py`: debounce των aggregate refresh — ένα refresh ανά tenant+entity
  ανά 120 s με union των pending date ranges, αντί για ~2 refresh/λεπτό (2.884/ημέρα)
- `worker/tasks.py` + `celery_app.py`: νέο `purge_processed_staging_all_tenants`,
  νυχτερινό 03:30, retention 14 ημερών (τα `failed` rows κρατούνται πάντα)
- `ingestion/engine.py`: το upsert του `dim_items` παρακάμπτει τα no-op updates με
  `ON CONFLICT ... WHERE (…) IS DISTINCT FROM (…)`. Πριν: 355.857.994 updates σε
  193.211 items με μόλις 14.768 HOT (0,004%)
- `models/tenant.py`: `fillfactor` 85/90 σε `dim_items`/`dim_customers` μέσω
  post-create DDL, ώστε τα updates να μένουν HOT

## Εκκρεμεί — απαιτεί απόφαση προϊόντος

**Rule-aware aggregates για το dashboard (§5).** Το `/v1/dashboard/executive-summary`
πέφτει σε full scan του `fact_sales` αντί να διαβάσει το `agg_sales_monthly`. Η αιτία
δεν είναι αυτή που υπέθετε το αρχικό πόρισμα: το `agg_sales_monthly` **είναι** ήδη
rule-aware ως προς τα document rules. Το πρόβλημα είναι ότι ο pharmacy295 έχει
**behavior-code whitelist** `[102, 103, 131, 151, 152, 181]` στο `sales_kpi_config`,
που είναι διαφορετικό σύστημα κανόνων (`kpi_participation_rules`) και **δεν**
μοντελοποιείται στο aggregate. Το fallback στο fact είναι επομένως λειτουργικά ορθό.

Η σωστή λύση είναι να προστεθεί διάσταση `behavior_code` στα `agg_sales_daily` /
`agg_sales_monthly` ώστε το φιλτράρισμα να γίνεται στο aggregate. Δεν εφαρμόστηκε:
αλλάζει τον τρόπο υπολογισμού οικονομικών KPI και χρειάζεται επαλήθευση των
αριθμών με τον πελάτη πριν βγει σε παραγωγή. Εκτιμώμενο κέρδος: 1,03 s → <150 ms.

**`source_payload_json` εκτός `fact_sales` (§11).** Δεν εφαρμόστηκε: αντίθετα με την
αρχική υπόθεση, η στήλη **χρησιμοποιείται ενεργά** από τα business rules
(`source_transaction_type_id`, `document_series_name`, `document_type` στα
`kpi_queries.py` και `ui.py`). Η σωστή προσέγγιση είναι εξαγωγή αυτών των πεδίων σε
κανονικές στήλες και μετά `SET STORAGE EXTERNAL` στο JSON — όχι σκέτη αφαίρεση.

## Artifacts
Scripts και rollback DDL: `artifacts/perf-2026-07/`
