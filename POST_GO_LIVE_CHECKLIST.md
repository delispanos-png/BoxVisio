# Post Go-Live Checklist

## First 24 Hours
1. Monitor API health every 15 minutes:
```bash
curl -fsS https://bi.boxvisio.com/health
curl -fsS https://adminpanel.boxvisio.com/health
```
2. Verify no critical alerts (error rate, queue stuck, DLQ spike, postgres down).
3. Check ingestion progress for first tenant:
   - queue depth drops after sync
   - DLQ depth stable
4. Confirm KPI endpoints and UI pages responsive.
5. Verify one backup run completed successfully.
6. Verify one suspend/unsuspend action propagates immediately.

## First Week
1. Daily backup status review and restore sample test.
2. Review DB pool saturation and worker retry trends.
3. Validate API key rotation flow for at least one tenant.
4. Review audit logs for admin actions and anomalies.
5. Capacity review:
   - CPU/memory
   - disk growth
   - queue throughput
6. Conduct weekly post-go-live review with findings and tuning actions.
