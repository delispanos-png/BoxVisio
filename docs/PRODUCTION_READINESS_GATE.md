# Production Readiness Gate

Το production gate είναι το ενιαίο check πριν από release ή μεγάλο tenant onboarding.
Στόχος του είναι να μειώνει τα "αν" στον κώδικα και στα δεδομένα με επαναλήψιμο αποτέλεσμα `PASS`, `WARN` ή `FAIL`.

## Εκτέλεση

```bash
cd /opt/cloudon-bi
docker compose exec -T api bash -lc 'cd /opt/cloudon-bi && /opt/cloudon-bi/.venv/bin/python scripts/production_readiness_check.py --tenant pharmacy295 --from-date 2025-01-01 --to-date 2026-05-20 --base-url http://127.0.0.1:8000 --slow-ms 3500'
```

Το script γράφει αναφορά σε:

```text
artifacts/production_readiness/
```

## Τι ελέγχει

- Active tenant connector.
- SQL connector / JavaScript bridge stream parity.
- Enabled, supported και mapped streams ανά tenant connector.
- Control και tenant migration version.
- Fact coverage ανά stream για το επιλεγμένο διάστημα.
- Aggregate coverage για βασικά dashboards.
- Dashboard/API smoke tests με πραγματικό tenant token.
- Cold-cache performance baseline.
- Data quality σε βασικές διαστάσεις και fact links.
- Priority pool και production chunk policy.

## Κανόνας release

- `PASS`: μπορεί να προχωρήσει.
- `WARN`: μπορεί να προχωρήσει μόνο αν τα warnings έχουν καταγραφεί και είναι αποδεκτά.
- `FAIL`: δεν προχωράει production release.

Για pharmacy295 στις 2026-05-20 το gate επέστρεψε `WARN`: δεν υπάρχουν αποτυχημένα dashboards, όμως υπάρχουν data-quality warnings και cold-cache καθυστέρηση στα sales/purchases summary για μεγάλο διάστημα.
