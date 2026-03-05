# Production Security Verification

## 1) Ports and Firewall
Verify UFW rules:
```bash
sudo ufw status numbered
```

Expected:
- `80/tcp` allow
- `443/tcp` allow
- `22/tcp` allow only from `194.154.34.84`
- `5432/tcp` denied publicly
- `6379/tcp` denied publicly

## 2) TLS and Redirect
```bash
curl -I http://bi.boxvisio.com
curl -I https://bi.boxvisio.com
curl -I https://adminpanel.boxvisio.com
```

Expected:
- HTTP redirects to HTTPS.
- HTTPS responds with valid certificate.
- HSTS header present (`Strict-Transport-Security`).

## 3) Secrets Hygiene
1. Ensure `.env` is not committed and has no placeholder secrets.
2. Confirm startup fails if `CHANGE_ME` remains in critical vars.
3. Validate only required admins can access `.env` on host.

Recommended check:
```bash
cd /opt/cloudon-bi
rg -n "CHANGE_ME" .env
```

## 4) Auth and Access
1. Verify JWT auth required for protected endpoints.
2. Verify RBAC:
   - cloudon_admin can access admin routes.
   - tenant roles cannot call admin routes.
3. Verify API key + HMAC required for ingest endpoints.

## 5) Security Headers
```bash
curl -I https://bi.boxvisio.com/health
```

Expected headers include:
- `X-Content-Type-Options`
- `X-Frame-Options`
- `Referrer-Policy`
- `Content-Security-Policy`

## 6) Final Sign-off
- [ ] Firewall correct
- [ ] TLS valid and auto-renew enabled
- [ ] Secrets verified
- [ ] RBAC and ingest auth validated
- [ ] Security headers validated
