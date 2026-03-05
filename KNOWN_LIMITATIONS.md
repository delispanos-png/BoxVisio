# Known Limitations (Milestone 4 UAT)

1. Tenant A PharmacyOne SQL ingestion could not be validated against a real SQL Server instance in this environment.
2. SQL connector path was validated for failure/retry/DLQ behavior (invalid SQL Server mapping), but not for successful end-to-end incremental extraction from a live SQL Server source.
   - Worker logs show expected failure when SQL driver is unavailable: `Can't open lib 'ODBC Driver 18 for SQL Server'`.
3. UAT used local-run mode (API/worker in venv + postgres/redis in Docker) due Docker image build instability in this host environment.
4. Default seed admin email in `.env` (`admin@cloudon.local`) is rejected by strict `EmailStr` validation in API login; a valid email (`admin@boxvisio.com`) was seeded for UAT.
5. Local-run path currently relies on `/app` compatibility symlinks for static and Alembic paths.
