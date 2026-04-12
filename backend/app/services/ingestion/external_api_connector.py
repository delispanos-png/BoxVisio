from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

import httpx
from redis import Redis

from app.core.config import settings
from app.services.ingestion.base import (
    ALL_OPERATIONAL_STREAMS,
    Connector,
    ConnectorContext,
    IncrementalState,
    IngestEntity,
    OperationalIngestStream,
    normalize_stream_name,
)


class ExternalApiIngestConnector(Connector):
    connector_name = 'external_api'
    source_type = 'api'
    supported_streams = ALL_OPERATIONAL_STREAMS
    required_connection_parameters = ('base_url',)

    def fetch_rows(
        self,
        *,
        stream: OperationalIngestStream,
        entity: IngestEntity,
        context: ConnectorContext,
        state: IncrementalState,
        payload: dict | None = None,
    ) -> list[dict]:
        del entity
        del state

        records = self._records_from_direct_payload(payload)
        if records is not None:
            return self._normalize_records(records)

        endpoint = self._resolve_stream_endpoint(stream=stream, context=context)
        if not endpoint:
            return []

        body = self._build_request_body(stream=stream, context=context, payload=payload)
        response_json = self._call_endpoint(endpoint=endpoint, context=context, body=body)
        extracted = self._extract_stream_records(stream=stream, response=response_json)
        return self._normalize_records(extracted)

    @staticmethod
    def _records_from_direct_payload(payload: dict | None) -> list[dict[str, Any]] | None:
        if not payload:
            return None

        records = payload.get('records')
        if isinstance(records, list):
            return [dict(r) for r in records if isinstance(r, dict)]

        payloads = payload.get('payloads')
        if isinstance(payloads, dict):
            for stream_payload in payloads.values():
                if not isinstance(stream_payload, dict):
                    continue
                nested = stream_payload.get('records')
                if isinstance(nested, list):
                    return [dict(r) for r in nested if isinstance(r, dict)]
        return None

    @staticmethod
    def _stream_tokens(stream: OperationalIngestStream) -> tuple[str, ...]:
        if stream == 'sales_documents':
            return ('sales_documents', 'sales', 'sales_docs')
        if stream == 'purchase_documents':
            return ('purchase_documents', 'purchases', 'purchase_docs')
        if stream == 'inventory_documents':
            return ('inventory_documents', 'inventory', 'warehouse_documents')
        if stream == 'cash_transactions':
            return ('cash_transactions', 'cashflows', 'cashflow')
        if stream == 'supplier_balances':
            return ('supplier_balances', 'supplier_balance')
        if stream == 'customer_balances':
            return ('customer_balances', 'customer_balance')
        return ('operating_expenses', 'expenses', 'expense')

    def _resolve_stream_endpoint(self, *, stream: OperationalIngestStream, context: ConnectorContext) -> str | None:
        endpoints = context.stream_api_endpoint if isinstance(context.stream_api_endpoint, dict) else {}
        fallback_all: str | None = None

        for key, value in endpoints.items():
            if not isinstance(value, str) or not value.strip():
                continue
            token = str(key or '').strip()
            if not token:
                continue
            if token.lower().replace('-', '_').replace(' ', '_') in {'all', 'default', '*'}:
                fallback_all = value.strip()
                continue
            normalized = normalize_stream_name(token)
            if normalized == stream:
                return value.strip()

        if fallback_all:
            return fallback_all

        params = context.connection_parameters if isinstance(context.connection_parameters, dict) else {}
        base_url = str(params.get('base_url') or '').strip()
        if not base_url:
            return None
        if '/Get' in base_url and base_url.rstrip('/').endswith('ForBI'):
            return base_url

        stream_suffix = {
            'sales_documents': 'GetSalesDocumentsForBI',
            'purchase_documents': 'GetPurchaseDocumentsForBI',
            'inventory_documents': 'GetInventoryDocumentsForBI',
            'cash_transactions': 'GetCashTransactionsForBI',
            'supplier_balances': 'GetSupplierBalancesForBI',
            'customer_balances': 'GetCustomerBalancesForBI',
            'operating_expenses': 'GetOperatingExpensesForBI',
        }.get(stream)
        if stream_suffix:
            return f"{base_url.rstrip('/')}/{stream_suffix}"
        return base_url

    def _build_request_body(
        self,
        *,
        stream: OperationalIngestStream,
        context: ConnectorContext,
        payload: dict | None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        raw_payload = payload if isinstance(payload, dict) else {}

        params = context.connection_parameters if isinstance(context.connection_parameters, dict) else {}
        sync_defaults = params.get('sync_defaults')
        if isinstance(sync_defaults, dict):
            body.update(sync_defaults)
        request_defaults = params.get('request_defaults')
        if isinstance(request_defaults, dict):
            body.update(request_defaults)

        passthrough_keys = (
            'company',
            'limit',
            'debug',
            'fromDate',
            'toDate',
            'includeSales',
            'includePurchases',
            'includeInventory',
            'includeCashTransactions',
            'includeSupplierBalances',
            'includeCustomerBalances',
            'includeOperatingExpenses',
            'salesSourceCodes',
            'purchaseSourceCodes',
            'inventorySourceCodes',
            'cashSourceCodes',
            'supplierBalanceSourceCodes',
            'customerBalanceSourceCodes',
            'expenseSourceCodes',
        )
        for key in passthrough_keys:
            if key in raw_payload:
                body[key] = raw_payload[key]

        if 'from_date' in raw_payload and 'fromDate' not in body:
            body['fromDate'] = raw_payload['from_date']
        if 'to_date' in raw_payload and 'toDate' not in body:
            body['toDate'] = raw_payload['to_date']

        self._inject_softone_auth(body=body, context=context, payload=raw_payload)

        if stream == 'sales_documents':
            body.setdefault('includeSales', True)
            body.setdefault('includePurchases', False)
            body.setdefault('includeInventory', False)
        elif stream == 'purchase_documents':
            body.setdefault('includeSales', False)
            body.setdefault('includePurchases', True)
            body.setdefault('includeInventory', False)
        elif stream == 'inventory_documents':
            body.setdefault('includeSales', False)
            body.setdefault('includePurchases', False)
            body.setdefault('includeInventory', True)
        elif stream == 'cash_transactions':
            body.setdefault('includeSales', False)
            body.setdefault('includePurchases', False)
            body.setdefault('includeInventory', False)
            body.setdefault('includeCashTransactions', True)
            body.setdefault('includeSupplierBalances', False)
            body.setdefault('includeCustomerBalances', False)
            body.setdefault('includeOperatingExpenses', False)
        elif stream == 'supplier_balances':
            body.setdefault('includeSales', False)
            body.setdefault('includePurchases', False)
            body.setdefault('includeInventory', False)
            body.setdefault('includeCashTransactions', False)
            body.setdefault('includeSupplierBalances', True)
            body.setdefault('includeCustomerBalances', False)
            body.setdefault('includeOperatingExpenses', False)
        elif stream == 'customer_balances':
            body.setdefault('includeSales', False)
            body.setdefault('includePurchases', False)
            body.setdefault('includeInventory', False)
            body.setdefault('includeCashTransactions', False)
            body.setdefault('includeSupplierBalances', False)
            body.setdefault('includeCustomerBalances', True)
            body.setdefault('includeOperatingExpenses', False)
        elif stream == 'operating_expenses':
            body.setdefault('includeSales', False)
            body.setdefault('includePurchases', False)
            body.setdefault('includeInventory', False)
            body.setdefault('includeCashTransactions', False)
            body.setdefault('includeSupplierBalances', False)
            body.setdefault('includeCustomerBalances', False)
            body.setdefault('includeOperatingExpenses', True)

        return body

    @staticmethod
    def _as_bool(value: Any, default: bool = True) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            txt = value.strip().lower()
            if txt in {'1', 'true', 'yes', 'on', 'y'}:
                return True
            if txt in {'0', 'false', 'no', 'off', 'n'}:
                return False
        return default

    @staticmethod
    def _as_retry_attempts(value: Any, default: int = 3) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(1, min(parsed, 5))

    @staticmethod
    def _as_cache_ttl_seconds(value: Any, default: int = 900) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(60, min(parsed, 3600))

    @staticmethod
    def _redis() -> Redis:
        return Redis.from_url(settings.redis_url, decode_responses=True)

    @staticmethod
    def _softone_client_cache_key(
        *,
        service_url: str,
        username: str,
        app_id: str,
        company: str,
        branch: str,
        module: str,
        refid: str,
    ) -> str:
        raw = '|'.join(
            [
                service_url.strip(),
                username.strip(),
                app_id.strip(),
                company.strip(),
                branch.strip(),
                module.strip(),
                refid.strip(),
            ]
        )
        digest = hashlib.sha1(raw.encode('utf-8')).hexdigest()
        return f'softone:client_id:{digest}'

    def _read_cached_client_id(self, key: str) -> str | None:
        try:
            value = self._redis().get(key)
        except Exception:
            return None
        value = str(value or '').strip()
        return value or None

    def _write_cached_client_id(self, key: str, client_id: str, ttl_seconds: int) -> None:
        try:
            self._redis().setex(key, ttl_seconds, client_id)
        except Exception:
            return

    def _clear_cached_client_id(self, key: str) -> None:
        try:
            self._redis().delete(key)
        except Exception:
            return

    @staticmethod
    def _request_with_retry(
        client: httpx.Client,
        *,
        method: str,
        url: str,
        body: dict[str, Any],
        call_name: str,
        retry_attempts: int,
    ) -> httpx.Response:
        method_upper = str(method or 'POST').strip().upper()
        last_error: Exception | None = None
        for attempt in range(1, retry_attempts + 1):
            try:
                if method_upper == 'GET':
                    return client.get(url, params=body)
                return client.post(url, json=body)
            except httpx.RequestError as exc:
                last_error = exc
                if attempt >= retry_attempts:
                    break
                time.sleep(min(0.5 * attempt, 2.0))
        raise RuntimeError(
            f'{call_name} failed after {retry_attempts} attempts: {last_error!r}'
        ) from last_error

    @staticmethod
    def _softone_service_url(source_url: str) -> str:
        parsed = urlparse(source_url)
        path = parsed.path or ''
        idx = path.lower().find('/s1services')
        if idx >= 0:
            base_path = path[: idx + len('/s1services')]
            return f'{parsed.scheme}://{parsed.netloc}{base_path}'
        return source_url

    def _inject_softone_auth(self, *, body: dict[str, Any], context: ConnectorContext, payload: dict[str, Any]) -> None:
        params = context.connection_parameters if isinstance(context.connection_parameters, dict) else {}
        auth_type = str(params.get('auth_type') or '').strip().lower()
        auth_cfg = params.get('auth_config')
        auth_cfg = auth_cfg if isinstance(auth_cfg, dict) else {}
        client_key = str(auth_cfg.get('client_id_param') or params.get('client_id_param') or 'clientID')

        explicit_client = (
            payload.get('clientID')
            or payload.get('client_id')
            or params.get('clientID')
            or params.get('client_id')
            or auth_cfg.get('clientID')
            or auth_cfg.get('client_id')
            or auth_cfg.get('static_client_id')
        )
        if explicit_client:
            body[client_key] = explicit_client
            return

        if auth_type not in {'softone_clientid', 'softone_login', 'softone_web_login'}:
            return

        username = str(auth_cfg.get('username') or params.get('username') or '').strip()
        password = str(auth_cfg.get('password') or params.get('password') or '').strip()
        app_id = str(auth_cfg.get('app_id') or auth_cfg.get('appId') or params.get('app_id') or params.get('appId') or '').strip()
        if not username or not password or not app_id:
            return

        base_url = str(params.get('base_url') or '').strip()
        if not base_url:
            return

        service_url = self._softone_service_url(base_url)
        verify_tls = self._as_bool(params.get('verify_tls'), True)
        timeout_seconds = int(params.get('timeout_seconds') or 45)
        retry_attempts = self._as_retry_attempts(params.get('retry_attempts'), default=3)
        cache_ttl_seconds = self._as_cache_ttl_seconds(params.get('client_cache_ttl_seconds'), default=900)
        raw_headers = params.get('headers')
        headers = {str(k): str(v) for k, v in raw_headers.items()} if isinstance(raw_headers, dict) else {}
        cache_key = self._softone_client_cache_key(
            service_url=service_url,
            username=username,
            app_id=app_id,
            company=str(auth_cfg.get('company') or auth_cfg.get('COMPANY') or '').strip(),
            branch=str(auth_cfg.get('branch') or auth_cfg.get('BRANCH') or '').strip(),
            module=str(auth_cfg.get('module') or auth_cfg.get('MODULE') or '0').strip(),
            refid=str(auth_cfg.get('refid') or auth_cfg.get('REFID') or '').strip(),
        )
        cached_client_id = self._read_cached_client_id(cache_key)
        if cached_client_id:
            body[client_key] = cached_client_id
            return
        session_client_id = self._softone_login_and_authenticate(
            service_url=service_url,
            username=username,
            password=password,
            app_id=app_id,
            company=str(auth_cfg.get('company') or auth_cfg.get('COMPANY') or '').strip(),
            branch=str(auth_cfg.get('branch') or auth_cfg.get('BRANCH') or '').strip(),
            module=str(auth_cfg.get('module') or auth_cfg.get('MODULE') or '0').strip(),
            refid=str(auth_cfg.get('refid') or auth_cfg.get('REFID') or '').strip(),
            headers=headers,
            verify_tls=verify_tls,
            timeout_seconds=timeout_seconds,
            retry_attempts=retry_attempts,
        )
        body[client_key] = session_client_id
        self._write_cached_client_id(cache_key, session_client_id, cache_ttl_seconds)

    def _softone_login_and_authenticate(
        self,
        *,
        service_url: str,
        username: str,
        password: str,
        app_id: str,
        company: str,
        branch: str,
        module: str,
        refid: str,
        headers: dict[str, str],
        verify_tls: bool,
        timeout_seconds: int,
        retry_attempts: int,
    ) -> str:
        with httpx.Client(timeout=timeout_seconds, verify=verify_tls, headers=headers or None) as client:
            login_payload = {
                'service': 'login',
                'username': username,
                'password': password,
                'appId': app_id,
            }
            login_resp = self._request_with_retry(
                client,
                method='POST',
                url=service_url,
                body=login_payload,
                call_name='softone_login',
                retry_attempts=retry_attempts,
            )
            login_json = self._parse_json_response(login_resp, call_name='softone_login')
            if not self._as_bool(login_json.get('success'), False):
                raise RuntimeError(f"SoftOne login failed: {login_json.get('error') or 'unknown_error'}")

            temp_client = str(login_json.get('clientID') or '').strip()
            if not temp_client:
                raise RuntimeError('SoftOne login failed: missing clientID')

            objs = login_json.get('objs')
            first_obj = objs[0] if isinstance(objs, list) and objs and isinstance(objs[0], dict) else {}
            auth_payload = {
                'service': 'authenticate',
                'clientID': temp_client,
                'company': company or str(first_obj.get('COMPANY') or ''),
                'branch': branch or str(first_obj.get('BRANCH') or ''),
                'module': module or str(first_obj.get('MODULE') or '0'),
                'refid': refid or str(first_obj.get('REFID') or ''),
            }
            auth_resp = self._request_with_retry(
                client,
                method='POST',
                url=service_url,
                body=auth_payload,
                call_name='softone_authenticate',
                retry_attempts=retry_attempts,
            )
            auth_json = self._parse_json_response(auth_resp, call_name='softone_authenticate')
            if not self._as_bool(auth_json.get('success'), False):
                raise RuntimeError(f"SoftOne authenticate failed: {auth_json.get('error') or 'unknown_error'}")

            final_client = str(auth_json.get('clientID') or '').strip()
            if not final_client:
                raise RuntimeError('SoftOne authenticate failed: missing clientID')
            return final_client

    def _call_endpoint(self, *, endpoint: str, context: ConnectorContext, body: dict[str, Any]) -> dict[str, Any]:
        params = context.connection_parameters if isinstance(context.connection_parameters, dict) else {}
        method = str(params.get('method') or 'POST').strip().upper()
        verify_tls = self._as_bool(params.get('verify_tls'), True)
        timeout_seconds = int(params.get('timeout_seconds') or 45)
        retry_attempts = self._as_retry_attempts(params.get('retry_attempts'), default=3)
        raw_headers = params.get('headers')
        headers = {str(k): str(v) for k, v in raw_headers.items()} if isinstance(raw_headers, dict) else {}
        headers.setdefault('Content-Type', 'application/json')
        headers.setdefault('Accept', 'application/json')

        with httpx.Client(timeout=timeout_seconds, verify=verify_tls, headers=headers or None) as client:
            response = self._request_with_retry(
                client,
                method=method,
                url=endpoint,
                body=body,
                call_name='external_api_fetch',
                retry_attempts=retry_attempts,
            )
        response_json = self._parse_json_response(response, call_name='external_api_fetch')

        if not self._as_bool(response_json.get('success'), True):
            error_message = str(response_json.get('error') or '').strip()
            auth_type = str(params.get('auth_type') or '').strip().lower()
            auth_cfg = params.get('auth_config') if isinstance(params.get('auth_config'), dict) else {}
            client_key = str(auth_cfg.get('client_id_param') or params.get('client_id_param') or 'clientID')
            if (
                error_message
                and 'clientid' in error_message.lower()
                and auth_type in {'softone_clientid', 'softone_login', 'softone_web_login'}
            ):
                service_url = self._softone_service_url(str(params.get('base_url') or '').strip())
                if service_url:
                    cache_key = self._softone_client_cache_key(
                        service_url=service_url,
                        username=str(auth_cfg.get('username') or params.get('username') or '').strip(),
                        app_id=str(
                            auth_cfg.get('app_id')
                            or auth_cfg.get('appId')
                            or params.get('app_id')
                            or params.get('appId')
                            or ''
                        ).strip(),
                        company=str(auth_cfg.get('company') or auth_cfg.get('COMPANY') or '').strip(),
                        branch=str(auth_cfg.get('branch') or auth_cfg.get('BRANCH') or '').strip(),
                        module=str(auth_cfg.get('module') or auth_cfg.get('MODULE') or '0').strip(),
                        refid=str(auth_cfg.get('refid') or auth_cfg.get('REFID') or '').strip(),
                    )
                    self._clear_cached_client_id(cache_key)
                body.pop(client_key, None)
                self._inject_softone_auth(body=body, context=context, payload={})
                with httpx.Client(timeout=timeout_seconds, verify=verify_tls, headers=headers or None) as retry_client:
                    retry_response = self._request_with_retry(
                        retry_client,
                        method=method,
                        url=endpoint,
                        body=body,
                        call_name='external_api_fetch',
                        retry_attempts=retry_attempts,
                    )
                retry_json = self._parse_json_response(retry_response, call_name='external_api_fetch')
                if self._as_bool(retry_json.get('success'), True):
                    return retry_json
                error_message = str(retry_json.get('error') or '').strip() or error_message
            if error_message:
                raise RuntimeError(f'External API returned error: {error_message}')
        return response_json

    @staticmethod
    def _parse_json_response(response: httpx.Response, *, call_name: str) -> dict[str, Any]:
        if response.status_code >= 400:
            body = response.text[:300].replace('\n', ' ')
            raise RuntimeError(f'{call_name} failed with HTTP {response.status_code}: {body}')

        raw = response.content
        parsed: Any = None
        encodings: list[str] = []

        # Respect declared charset first (SoftOne commonly responds with windows-1253).
        if response.encoding:
            encodings.append(str(response.encoding))
        content_type = str(response.headers.get('content-type') or '')
        if 'charset=' in content_type.lower():
            declared = content_type.split('charset=', 1)[1].split(';', 1)[0].strip()
            if declared:
                encodings.append(declared)
        encodings.extend(['utf-8', 'windows-1253', 'iso-8859-7', 'latin-1'])

        tried: set[str] = set()
        for enc in encodings:
            key = enc.lower()
            if key in tried:
                continue
            tried.add(key)
            try:
                text_payload = raw.decode(enc)
                parsed = json.loads(text_payload)
                break
            except Exception:
                continue

        if parsed is None:
            body = raw[:300].decode('utf-8', errors='replace').replace('\n', ' ')
            raise RuntimeError(f'{call_name} returned non-JSON response: {body}')
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            return {'records': parsed}
        return {}

    def _extract_stream_records(self, *, stream: OperationalIngestStream, response: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(response, dict):
            return []

        records = response.get('records')
        if isinstance(records, list):
            return [dict(row) for row in records if isinstance(row, dict)]

        payloads = response.get('payloads')
        if isinstance(payloads, dict):
            for token in self._stream_tokens(stream):
                item = payloads.get(token)
                if isinstance(item, dict) and isinstance(item.get('records'), list):
                    return [dict(row) for row in item['records'] if isinstance(row, dict)]

        streams = response.get('streams')
        if isinstance(streams, dict):
            for token in self._stream_tokens(stream):
                item = streams.get(token)
                if isinstance(item, dict) and isinstance(item.get('records'), list):
                    return [dict(row) for row in item['records'] if isinstance(row, dict)]

        for token in self._stream_tokens(stream):
            nested = response.get(token)
            if isinstance(nested, dict) and isinstance(nested.get('records'), list):
                return [dict(row) for row in nested['records'] if isinstance(row, dict)]
            if isinstance(nested, list):
                return [dict(row) for row in nested if isinstance(row, dict)]

        return []

    @staticmethod
    def _normalize_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        now_iso = datetime.utcnow().isoformat()
        for record in records:
            row = dict(record)
            if row.get('updated_at') is None:
                row['updated_at'] = now_iso
            out.append(row)
        return out
