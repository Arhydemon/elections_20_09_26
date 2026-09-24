#!/usr/bin/env python3
"""Полная возобновляемая выгрузка публичных данных с izbirkom.ru.

Скрипт использует те же открытые HTTP API, что и веб-интерфейс сайта:

* каталог и карточки выборов;
* полные деревья комиссий;
* стандартные отчёты и карточки партий;
* кандидаты, партийные списки и полные карточки кандидатов;
* ход голосования;
* обычные протоколы и протоколы ДЭГ;
* финансовые отчёты и приложенные PDF/DOCX/XLSX.

Каждый ответ сначала сохраняется отдельно в raw/. Поэтому остановленный запуск
можно безопасно запустить повторно: уже полученные ответы не скачиваются снова.
После обхода из raw/ строятся удобные сводные JSONL-файлы в exports/.
"""

from __future__ import annotations

import argparse
import ast
import concurrent.futures
import contextlib
import datetime as dt
import hashlib
import gzip
import http.client
import io
import json
import mimetypes
import operator
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator


API = "http://apps.cikrf.ru/service/ik-inp-service-pbcopy"
REFERENCE = "http://apps.cikrf.ru/service/reference"
ORIGIN = "http://izbirkom.ru"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
)
OPS = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul}
FINANCIAL_REPORT_TYPES = tuple(range(70, 83))


class ExpectedUnavailable(Exception):
    """The requested report variant does not exist for this election."""


def solve_js_task(task: str) -> str:
    """Safely solve the arithmetic challenge returned by the public API."""
    match = re.fullmatch(r"\s*return\s+([0-9+*() -]+)\s*;\s*", task)
    if not match:
        raise ValueError(f"Unsupported challenge: {task!r}")
    tree = ast.parse(match.group(1), mode="eval")

    def visit(node: ast.AST) -> int:
        if isinstance(node, ast.Expression):
            return visit(node.body)
        if isinstance(node, ast.Constant) and type(node.value) is int:
            return node.value
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            return -visit(node.operand)
        if isinstance(node, ast.BinOp) and type(node.op) in OPS:
            return OPS[type(node.op)](visit(node.left), visit(node.right))
        raise ValueError("Challenge contains a non-arithmetic operation")

    return str(visit(tree))


class RateLimiter:
    def __init__(self, delay: float):
        self.delay = max(0.0, delay)
        self.lock = threading.Lock()
        self.last_request = 0.0
        self.blocked_until = 0.0

    def cooldown(self, seconds: float) -> None:
        with self.lock:
            self.blocked_until = max(self.blocked_until, time.monotonic() + seconds)

    def wait(self) -> None:
        while True:
            with self.lock:
                left = max(self.last_request + self.delay, self.blocked_until) - time.monotonic()
                if left <= 0:
                    self.last_request = time.monotonic()
                    return
            time.sleep(min(left, 0.25))


class BufferedResponse(io.BytesIO):
    def __init__(self, data: bytes, headers: Any):
        super().__init__(data)
        self.headers = headers


class Client:
    def __init__(self, timeout: int = 90, delay: float = 0.04):
        self.timeout = timeout
        self.api_key = ""
        self.key_lock = threading.RLock()
        self.rate = RateLimiter(delay)
        self.local = threading.local()
        self.connections: list[Any] = []
        self.stats_lock = threading.Lock()
        self.stats = {'requests': 0, 'ok': 0, 'bytes': 0, 'retries': 0, 'http429': 0}
        self.proxies = urllib.request.getproxies()

    def snapshot(self) -> dict[str, int]:
        with self.stats_lock:
            return dict(self.stats)

    def close(self) -> None:
        for connection in self.connections:
            connection.close()

    def transport(self, url: str, data: bytes | None, headers: dict[str, str]):
        parsed = urllib.parse.urlsplit(url)
        # Retain the system proxy/redirect behavior where it is needed.
        if self.proxies.get(parsed.scheme) and not urllib.request.proxy_bypass(parsed.hostname or ''):
            return urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=self.timeout)
        origin = (parsed.scheme, parsed.hostname, parsed.port)
        if not hasattr(self.local, 'connections'):
            self.local.connections = {}
        connection = self.local.connections.get(origin)
        if connection is None:
            cls = http.client.HTTPSConnection if parsed.scheme == 'https' else http.client.HTTPConnection
            connection = cls(parsed.hostname, parsed.port, timeout=self.timeout)
            self.local.connections[origin] = connection
            with self.stats_lock:
                self.connections.append(connection)
        target = urllib.parse.urlunsplit(('', '', parsed.path or '/', parsed.query, ''))
        try:
            connection.request('POST' if data is not None else 'GET', target, body=data, headers=headers)
            response = connection.getresponse()
            raw = response.read()
            status, reason, response_headers = response.status, response.reason, response.headers
            if response_headers.get('Content-Encoding', '').lower() == 'gzip':
                raw = gzip.decompress(raw)
            if 300 <= status < 400:
                return urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=self.timeout)
            if status >= 400:
                raise urllib.error.HTTPError(url, status, reason, response_headers, io.BytesIO(raw))
            return BufferedResponse(raw, response_headers)
        except (http.client.HTTPException, OSError):
            connection.close()
            raise

    def _headers(self, authenticated: bool) -> dict[str, str]:
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Origin": ORIGIN,
            "Referer": ORIGIN + "/",
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "gzip",
        }
        if authenticated:
            if not self.api_key:
                with self.key_lock:
                    if not self.api_key:
                        self.refresh_key()
            headers["X-Api-Key"] = self.api_key
            headers["X-Client-Fingerprint"] = USER_AGENT
        return headers

    def _open(
        self,
        url: str,
        *,
        payload: Any = None,
        authenticated: bool = True,
        accept_json: bool = True,
    ):
        data = None
        headers = self._headers(authenticated)
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"

        for attempt in range(7):
            self.rate.wait()
            with self.stats_lock:
                self.stats['requests'] += 1
                self.stats['retries'] += int(attempt > 0)
            try:
                response = self.transport(url, data, headers)
                with response:
                    raw = response.read()
                    response_headers = response.headers
                    if not isinstance(response, BufferedResponse) and response_headers.get('Content-Encoding', '').lower() == 'gzip':
                        raw = gzip.decompress(raw)
                with self.stats_lock:
                    self.stats['ok'] += 1
                    self.stats['bytes'] += len(raw)
                if accept_json:
                    if not raw:
                        return None
                    return json.loads(raw.decode("utf-8-sig"))
                return BufferedResponse(raw, response_headers)
            except urllib.error.HTTPError as error:
                error.close()
                if authenticated and error.code in (401, 403):
                    with self.key_lock:
                        if self.api_key == headers.get("X-Api-Key"):
                            self.refresh_key()
                    headers = self._headers(True)
                    if payload is not None:
                        headers["Content-Type"] = "application/json"
                    continue
                if error.code in (400, 404, 409, 418):
                    with contextlib.suppress(Exception):
                        error.read()
                    raise ExpectedUnavailable(f"HTTP {error.code}: {url}") from error
                if error.code not in (408, 425, 429, 500, 502, 503, 504) or attempt == 6:
                    raise
                print(
                    f"[СЕТЬ] HTTP {error.code}; повтор {attempt + 1}/6: {url}",
                    flush=True,
                )
                if error.code == 429:
                    with self.stats_lock:
                        self.stats['http429'] += 1
                    retry_after = error.headers.get("Retry-After", "")
                    try:
                        seconds = float(retry_after)
                    except ValueError:
                        try:
                            seconds = parsedate_to_datetime(retry_after).timestamp() - time.time()
                        except (ValueError, TypeError, OverflowError):
                            seconds = min(60.0, 2.0 ** (attempt + 1))
                    self.rate.cooldown(max(1.0, seconds))
            except (urllib.error.URLError, ConnectionError, TimeoutError, OSError, http.client.HTTPException) as error:
                if attempt == 6:
                    raise
                print(
                    f"[СЕТЬ] {type(error).__name__}: {error}; "
                    f"повтор {attempt + 1}/6: {url}",
                    flush=True,
                )
            time.sleep(min(20.0, 0.8 * (2**attempt)))
        raise RuntimeError("request retry loop exhausted")

    def refresh_key(self) -> None:
        last_error: Exception | None = None
        for base in (API, REFERENCE):
            try:
                print(f"[СТАРТ] Получаю ключ доступа: {base}", flush=True)
                challenge = self._open(
                    f"{base}/challenge/get", authenticated=False, accept_json=True
                )
                solved = self._open(
                    f"{base}/challenge/solve",
                    payload={
                        "pubToken": challenge["pubToken"],
                        "answer": solve_js_task(challenge["jsTask"]),
                        "fingerprint": USER_AGENT,
                    },
                    authenticated=False,
                    accept_json=True,
                )
                self.api_key = solved["apiKey"]
                return
            except Exception as error:
                last_error = error
        raise RuntimeError("Не удалось получить временный API-ключ") from last_error

    @staticmethod
    def _url(base: str, path: str, params: dict[str, Any] | None = None) -> str:
        url = base.rstrip("/") + "/" + path.lstrip("/")
        if params:
            clean = {k: v for k, v in params.items() if v is not None}
            url += "?" + urllib.parse.urlencode(clean, doseq=True)
        return url

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._open(self._url(API, path, params))

    def post(self, path: str, payload: Any) -> Any:
        return self._open(self._url(API, path), payload=payload)

    def download(
        self, path: str, params: dict[str, Any] | None = None
    ) -> tuple[bytes, dict[str, str]]:
        response = self._open(self._url(API, path, params), accept_json=False)
        try:
            return response.read(), {k.lower(): v for k, v in response.headers.items()}
        finally:
            response.close()


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}-{threading.get_ident()}")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, separators=(',', ':')), encoding="utf-8"
    )
    os.replace(temporary, path)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}-{threading.get_ident()}")
    temporary.write_bytes(data)
    os.replace(temporary, path)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def cached_json(
    path: Path,
    request: dict[str, Any],
    fetch: Callable[[], Any],
    *,
    refresh: bool = False,
) -> Any:
    if path.exists() and not refresh:
        saved = read_json(path)
        return saved.get("response") if isinstance(saved, dict) and "response" in saved else saved
    response = fetch()
    atomic_write_json(
        path,
        {
            "request": request,
            "fetchedAt": dt.datetime.now().astimezone().isoformat(),
            "response": response,
        },
    )
    return response


def hash_params(value: Any) -> str:
    packed = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(packed.encode("utf-8")).hexdigest()[:16]


def commission_partition(external_id: Any, count: int) -> int:
    return int(hashlib.sha256(str(external_id).encode('utf-8')).hexdigest(), 16) % count


def code(value: Any, default: str = "") -> str:
    if isinstance(value, dict):
        value = value.get("externalId", default)
    return str(value if value is not None else default)


def int_code(value: Any, default: int = 0) -> int:
    with contextlib.suppress(TypeError, ValueError):
        return int(code(value))
    return default


def iter_dates(start: str, finish: str) -> Iterator[str]:
    first = dt.date.fromisoformat(start[:10])
    last = dt.date.fromisoformat(finish[:10])
    while first <= last:
        yield first.isoformat()
        first += dt.timedelta(days=1)


def safe_filename(name: str, fallback: str = "file") -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip(" .")
    name = re.sub(r"\s+", " ", name)
    return (name or fallback)[:150]


def iter_objects(value: Any) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from iter_objects(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from iter_objects(nested)


def load_jsonl_map(path: Path) -> dict[Any, Any]:
    result: dict[Any, Any] = {}
    if not path.exists():
        return result
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                item = json.loads(line)
                result[item["id"]] = item.get("data", item)
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
    return result


class FullCrawler:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.out = Path(args.output).resolve()
        self.raw = self.out / "raw" / "elections"
        self.client = Client(timeout=args.timeout, delay=args.delay)
        self.print_lock = threading.Lock()
        self.error_lock = threading.Lock()
        self.progress_lock = threading.Lock()
        self.completed_count = 0
        self.run_started = time.monotonic()
        self.total_records = 0
        self.error_counts: dict[Any, int] = {}
        self.report_pool: concurrent.futures.ThreadPoolExecutor | None = None
        self.old_details = load_jsonl_map(self.out / "election_details.jsonl")
        self.old_trees = load_jsonl_map(self.out / "commission_trees.jsonl")

    def log(self, message: str) -> None:
        with self.print_lock:
            print(message, flush=True)

    def error(
        self, election_id: Any, stage: str, error: Exception, extra: Any = None
    ) -> None:
        record = {
            "time": dt.datetime.now().astimezone().isoformat(),
            "electionId": election_id,
            "stage": stage,
            "error": repr(error),
            "extra": extra,
        }
        with self.error_lock:
            self.error_counts[election_id] = self.error_counts.get(election_id, 0) + 1
            path = self.out / "errors.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8", newline="\n") as stream:
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")

    def mark_complete(self, election_id: Any) -> None:
        with self.progress_lock:
            self.completed_count += 1
            elapsed = max(0.001, time.monotonic() - self.run_started)
            errors = self.error_counts.get(election_id, 0)
            self.log(
                f"[ОБРАБОТАНО {self.completed_count}/{self.total_records}] "
                f"{election_id}; ошибок: {errors}; прошло {dt.timedelta(seconds=int(elapsed))}"
            )

    def catalog(self) -> list[dict[str, Any]]:
        self.out.mkdir(parents=True, exist_ok=True)
        request = {
            "votingDateFrom": self.args.from_date,
            "votingDateTo": self.args.to_date,
            "page": 1,
            "perPage": 10000,
        }
        response = cached_json(
            self.out / "catalog_full_raw.json",
            {"method": "GET", "path": "/elections", "params": request},
            lambda: self.client.get("/elections", request),
            refresh=self.args.refresh,
        )
        all_records = (
            response.get("content", []) if isinstance(response, dict) else []
        )
        # Keep the complete catalogue on disk even when this particular run is
        # restricted by --sample or --election-id.
        with (self.out / "elections.jsonl").open(
            "w", encoding="utf-8", newline="\n"
        ) as stream:
            for item in all_records:
                stream.write(json.dumps(item, ensure_ascii=False) + "\n")
        records = all_records
        if self.args.election_id:
            wanted = str(self.args.election_id)
            records = [
                item
                for item in records
                if str(item.get("id")) == wanted
                or str(item.get("externalId")) == wanted
            ]
        excluded = set(self.args.exclude_election_id)
        if excluded:
            records = [item for item in records
                       if str(item.get('id')) not in excluded
                       and str(item.get('externalId')) not in excluded]
        if self.args.election_id and not records:
            raise ValueError('Выбранные выборы отсутствуют в каталоге или исключены фильтром')
        if self.args.sample:
            records = records[: self.args.sample]
        return records

    def election_dir(self, election: dict[str, Any]) -> Path:
        return self.raw / str(election["id"])

    def get_detail(self, election: dict[str, Any]) -> dict[str, Any]:
        eid = election["id"]
        path = self.election_dir(election) / "detail.json"
        if path.exists() and not self.args.refresh:
            saved = read_json(path)
            return saved.get("response", saved)
        if eid in self.old_details and not self.args.refresh:
            detail = self.old_details[eid]
            atomic_write_json(path, {"source": "existing-jsonl", "response": detail})
            return detail
        return cached_json(
            path,
            {"method": "GET", "path": f"/elections/{eid}"},
            lambda: self.client.get(f"/elections/{eid}"),
            refresh=self.args.refresh,
        )

    def get_root(self, election: dict[str, Any]) -> dict[str, Any]:
        eid = election["id"]
        path = self.election_dir(election) / "commission_root.json"
        if path.exists() and not self.args.refresh:
            saved = read_json(path)
            return saved.get("response", saved)
        if eid in self.old_trees and not self.args.refresh:
            root = self.old_trees[eid]
            atomic_write_json(path, {"source": "existing-jsonl", "response": root})
            return root
        params = {"electionsId": eid}
        return cached_json(
            path,
            {"method": "GET", "path": "/commissionClassifiers", "params": params},
            lambda: self.client.get("/commissionClassifiers", params),
            refresh=self.args.refresh,
        )

    def subject_for_child(self, current: str, child: dict[str, Any]) -> str:
        if current and current != "00":
            return current
        if int_code(child.get("type")) == 2:
            with contextlib.suppress(TypeError, ValueError):
                return f"{int(child.get('number')):02d}"
        return current or "00"

    def commissions(
        self, election: dict[str, Any], detail: dict[str, Any]
    ) -> list[dict[str, Any]]:
        root = self.get_root(election)
        election_subject = code(
            detail.get("subjectRf") or election.get("subjectRf"), "00"
        )
        queue = deque([
            (root, election_subject, bool(detail.get("isDegPermitted")))
        ])
        seen: set[str] = set()
        flat: list[dict[str, Any]] = []
        folder = self.election_dir(election) / "commissions"

        def expand(item):
            node, subject, inherited_deg = item
            external_id = str(node.get("externalId") or "")
            full = node
            if node.get("hasChildren") and not node.get("children"):
                params = {
                    "electionsId": election["id"],
                    "subjectRf": subject,
                    "classifierId": external_id,
                }
                try:
                    full = cached_json(
                        folder / f"{external_id}.json",
                        {
                            "method": "GET",
                            "path": "/commissionClassifiers",
                            "params": params,
                        },
                        lambda p=params: self.client.get(
                            "/commissionClassifiers", p
                        ),
                        refresh=self.args.refresh,
                    )
                except Exception as error:
                    self.error(election["id"], "commission", error, params)
                    full = node
            current_deg = bool(full.get("isDegPermitted", inherited_deg))
            record = {
                **full,
                "_subjectRfCode": subject,
                "_isDegPermittedInherited": current_deg,
            }
            return record

        self.log(f"  {election['id']}: собираю дерево комиссий")
        last_log = time.monotonic()
        while queue:
            batch = []
            limit = self.args.commission_workers
            if self.args.test_limit:
                limit = min(limit, self.args.test_limit - len(flat))
            if limit <= 0:
                break
            while queue and len(batch) < limit:
                item = queue.popleft()
                external_id = str(item[0].get('externalId') or '')
                if external_id and external_id not in seen:
                    seen.add(external_id)
                    batch.append(item)
            for record in self.parallel_items(expand, batch):
                flat.append(record)
                for child in record.get('children') or []:
                    queue.append((child, self.subject_for_child(record['_subjectRfCode'], child),
                                  record['_isDegPermittedInherited']))
            if time.monotonic() - last_log >= 15:
                self.log(f"  {election['id']}: дерево — {len(flat)} комиссий, очередь {len(queue)}")
                last_log = time.monotonic()

        flat_path = self.election_dir(election) / "commissions_flat.jsonl"
        flat_path.parent.mkdir(parents=True, exist_ok=True)
        with flat_path.open("w", encoding="utf-8", newline="\n") as stream:
            for item in flat:
                stream.write(json.dumps(item, ensure_ascii=False) + "\n")
        return flat

    def parallel_items(self, function: Callable, items: Iterable) -> Iterator[Any]:
        """Shared executor, bounded submission, no nested work on its threads."""
        if self.report_pool is None:
            yield from map(function, items)
            return
        iterator = iter(items)
        pending = set()
        exhausted = False
        while pending or not exhausted:
            while not exhausted and len(pending) < self.args.commission_workers:
                try:
                    item = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                pending.add(self.report_pool.submit(function, item))
            if not pending:
                break
            done, pending = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                yield future.result()

    def report_json(
        self,
        election: dict[str, Any],
        folder: str,
        report_type: int,
        params: dict[str, Any],
    ) -> Any | None:
        request = {
            "method": "GET",
            "path": f"/reports/{report_type}",
            "params": params,
        }
        path = (
            self.election_dir(election)
            / folder
            / f"r{report_type}_{hash_params(params)}.json"
        )
        try:
            return cached_json(
                path,
                request,
                lambda: self.client.get(f"/reports/{report_type}", params),
                refresh=self.args.refresh,
            )
        except ExpectedUnavailable:
            return None
        except Exception as error:
            self.error(election["id"], f"report-{report_type}", error, params)
            return None

    def election_reports(self, election: dict[str, Any]) -> list[Any]:
        external_id = election["externalId"]
        params = {"electionsId": external_id}
        path = self.election_dir(election) / "election_report_catalog.json"
        try:
            catalog = cached_json(
                path,
                {"method": "GET", "path": "/reports", "params": params},
                lambda: self.client.get("/reports", params),
                refresh=self.args.refresh,
            )
        except Exception as error:
            self.error(
                election["id"], "election-report-catalog", error, params
            )
            return []
        responses = []
        for descriptor in catalog if isinstance(catalog, list) else []:
            with contextlib.suppress(TypeError, ValueError):
                report_type = int(descriptor["reportType"])
                response = self.report_json(
                    election, "election_reports", report_type, params
                )
                if response is not None:
                    responses.append(response)
        return responses

    def referendum_questions(self, detail: dict[str, Any]) -> list[Any]:
        result = []
        for obj in iter_objects(detail.get("referendumQuestions") or []):
            if obj.get("questionNumber") is not None:
                result.append(obj["questionNumber"])
        return list(dict.fromkeys(result))

    def commission_reports(
        self,
        election: dict[str, Any],
        detail: dict[str, Any],
        commissions: list[dict[str, Any]],
    ) -> None:
        if self.args.commission_shard:
            selected = set(self.args.commission_shard)
            all_count = len(commissions)
            commissions = [row for row in commissions if commission_partition(
                row.get('externalId'), self.args.commission_shards) in selected]
            self.log(f"  {election['id']}: на этом устройстве {len(commissions)}/{all_count} комиссий; "
                     f"части {sorted(selected)} из {self.args.commission_shards}")
        dates = list(
            iter_dates(
                str(
                    detail.get("startVotingDay") or detail.get("votingDate")
                )[:10],
                str(
                    detail.get("finishVotingDay") or detail.get("votingDate")
                )[:10],
            )
        )
        questions = self.referendum_questions(detail)
        election_level = int_code(detail.get("electionLevel"))
        election_deg = bool(detail.get("isDegPermitted"))

        def process_commission(commission: dict[str, Any]) -> None:
            ccid = commission.get("externalId")
            if not ccid:
                return
            params = {"commissionClassifierId": ccid}
            catalog_path = (
                self.election_dir(election)
                / "commission_report_catalog"
                / f"{ccid}.json"
            )
            try:
                catalog = cached_json(
                    catalog_path,
                    {"method": "GET", "path": "/reports", "params": params},
                    lambda p=params: self.client.get("/reports", p),
                    refresh=self.args.refresh,
                )
            except Exception as error:
                self.error(
                    election["id"], "commission-report-catalog", error, params
                )
                return

            available = set()
            for item in catalog if isinstance(catalog, list) else []:
                with contextlib.suppress(TypeError, ValueError, KeyError):
                    available.add(int(item["reportType"]))

            for report_type in sorted(available):
                if report_type == 453:
                    for date in dates:
                        self.report_json(
                            election,
                            "commission_reports",
                            report_type,
                            {"commissionClassifierId": ccid, "date": date},
                        )
                elif report_type == 242:
                    variants = []
                    for protocol_num in (1, 2):
                        variants.append(
                            {
                                "commissionClassifierId": ccid,
                                "protocolNum": protocol_num,
                            }
                        )
                        for question in questions:
                            variants.append(
                                {
                                    "commissionClassifierId": ccid,
                                    "protocolNum": protocol_num,
                                    "referendumQuestionNum": question,
                                }
                            )
                    # Some referendum configurations omit protocolNum entirely
                    # and select the protocol only by question number.
                    for question in questions:
                        variants.append(
                            {
                                "commissionClassifierId": ccid,
                                "referendumQuestionNum": question,
                            }
                        )
                    for variant in variants:
                        self.report_json(
                            election, "results", report_type, variant
                        )

                    classifier_type = int_code(commission.get("type"), -1)
                    deg_allowed_here = bool(
                        commission.get(
                            "_isDegPermittedInherited",
                            commission.get("isDegPermitted"),
                        )
                    )
                    if (
                        election_deg
                        and classifier_type != 0
                        and not (election_level == 1 and classifier_type == 2)
                        and deg_allowed_here
                    ):
                        for protocol_num in (1, 2):
                            variant = {
                                "parentCommissionClassifierId": ccid,
                                "protocolNum": protocol_num,
                            }
                            self.report_json(
                                election, "deg_results", report_type, variant
                            )
                            for question in questions:
                                self.report_json(
                                    election,
                                    "deg_results",
                                    report_type,
                                    {
                                        **variant,
                                        "referendumQuestionNum": question,
                                    },
                                )
                                self.report_json(
                                    election,
                                    "deg_results",
                                    report_type,
                                    {
                                        "parentCommissionClassifierId": ccid,
                                        "referendumQuestionNum": question,
                                    },
                                )
                else:
                    self.report_json(
                        election, "commission_reports", report_type, params
                    )

        if self.report_pool is None:
            raise RuntimeError("Commission executor is not running")
        # Bound each producer's queue so a federal election cannot enqueue
        # 95,000 futures ahead of all other elections or exhaust memory.
        pending: dict[concurrent.futures.Future, dict[str, Any]] = {}
        remaining = iter(commissions)
        finished = 0
        started = time.monotonic()
        last_log = started
        window = self.args.commission_workers
        exhausted = False
        self.log(f"  {election['id']}: начинаю отчёты {len(commissions)} комиссий")
        while pending or not exhausted:
            while not exhausted and len(pending) < window:
                try:
                    commission = next(remaining)
                except StopIteration:
                    exhausted = True
                    break
                pending[self.report_pool.submit(process_commission, commission)] = commission
            if not pending:
                break
            done, _ = concurrent.futures.wait(
                pending, timeout=15, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for future in done:
                commission = pending.pop(future)
                try:
                    future.result()
                except Exception as error:
                    self.error(election['id'], 'commission-reports', error,
                               {'commissionClassifierId': commission.get('externalId')})
                finished += 1
            now = time.monotonic()
            if now - last_log >= 15 or finished == len(commissions):
                speed = finished * 60 / max(0.001, now - started)
                self.log(
                    f"  {election['id']}: обработано комиссий {finished}/{len(commissions)}; "
                    f"{speed:.1f}/мин (включая кэш); ошибок выбора: "
                    f"{self.error_counts.get(election['id'], 0)}"
                )
                last_log = now

    def paging(
        self,
        election: dict[str, Any],
        folder: Path,
        path: str,
        base_payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        folder.mkdir(parents=True, exist_ok=True)
        rows: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = {
                **base_payload,
                "page": page,
                "perPage": self.args.page_size,
            }
            page_path = folder / f"page_{page:06d}.json"
            try:
                response = cached_json(
                    page_path,
                    {"method": "POST", "path": path, "body": payload},
                    lambda p=payload: self.client.post(path, p),
                    refresh=self.args.refresh,
                )
            except ExpectedUnavailable:
                break
            except Exception as error:
                self.error(election["id"], f"paging-{path}", error, payload)
                break
            content = (
                response.get("content") or []
                if isinstance(response, dict)
                else []
            )
            rows.extend(item for item in content if isinstance(item, dict))
            total_pages = (
                int(response.get("totalPages") or 0)
                if isinstance(response, dict)
                else 0
            )
            if self.args.test_limit or not content or page >= total_pages:
                break
            page += 1
        return rows

    def candidate_card(
        self, election: dict[str, Any], candidate_id: str
    ) -> None:
        self.report_json(
            election, "candidate_cards", 341, {"candidateId": candidate_id}
        )

    def candidates(
        self,
        election: dict[str, Any],
        detail: dict[str, Any],
        election_report_responses: list[Any],
    ) -> None:
        kind = int_code(detail.get("kind"))
        system_type = int_code(detail.get("systemType"))
        external_id = election["externalId"]
        modes: list[tuple[str, dict[str, Any]]] = []
        if kind == 1:
            modes.append(("all", {"showSelfNominated": True}))
        elif kind == 2:
            if system_type in (1, 3, 4, 5, 7):
                modes.append(
                    (
                        "majoritarian",
                        {
                            "showSelfNominated": True,
                            "onlyMajoritarian": True,
                        },
                    )
                )
            if system_type in (2, 3):
                modes.append(
                    (
                        "proportional",
                        {
                            "showSelfNominated": False,
                            "onlyProportional": True,
                        },
                    )
                )
            if not modes:
                modes.append(("all", {"showSelfNominated": True}))
        else:
            return

        candidate_rows: dict[str, dict[str, Any]] = {}
        base = self.election_dir(election) / "candidates"
        for mode, extra in modes:
            rows = self.paging(
                election,
                base / mode,
                "/candidate/paging",
                {"electionsId": external_id, **extra},
            )
            for row in rows:
                if row.get("id"):
                    candidate_rows[str(row["id"])] = row

        association_ids: set[str] = set()
        nsi_ids: set[str] = set()
        for response in election_report_responses:
            report_type = (
                str(response.get("reportType"))
                if isinstance(response, dict)
                else ""
            )
            for obj in iter_objects(response):
                if report_type == "236" and obj.get("associationId"):
                    association_ids.add(str(obj["associationId"]))
                if report_type == "240" and obj.get("id"):
                    nsi_ids.add(str(obj["id"]))

        association_iter = sorted(association_ids)
        if self.args.test_limit:
            association_iter = association_iter[: self.args.test_limit]
        for association_id in association_iter:
            rows = self.paging(
                election,
                base / "associations" / association_id,
                "/candidate/paging",
                {
                    "electionsId": external_id,
                    "associationId": association_id,
                    "showSelfNominated": False,
                    "onlyProportional": True,
                },
            )
            for row in rows:
                if row.get("id"):
                    candidate_rows[str(row["id"])] = row

        detail_ids = sorted(candidate_rows)
        if self.args.test_limit:
            detail_ids = detail_ids[: self.args.test_limit]
        for index, _ in enumerate(self.parallel_items(
            lambda candidate_id: self.candidate_card(election, candidate_id), detail_ids
        ), 1):
            if index % 250 == 0:
                self.log(
                    f"  {election['id']}: карточки кандидатов "
                    f"{index}/{len(detail_ids)}"
                )

        party_ids = sorted(nsi_ids | association_ids)
        if self.args.test_limit:
            party_ids = party_ids[: self.args.test_limit]
        for party_id in party_ids:
            path = (
                self.election_dir(election)
                / "associations"
                / f"{party_id}.json"
            )
            try:
                cached_json(
                    path,
                    {"method": "GET", "path": f"/nsi704/{party_id}"},
                    lambda pid=party_id: self.client.get(f"/nsi704/{pid}"),
                    refresh=self.args.refresh,
                )
            except ExpectedUnavailable:
                continue
            except Exception as error:
                self.error(
                    election["id"], "association", error, {"id": party_id}
                )

    def financial(self, election: dict[str, Any]) -> None:
        report_types: Iterable[int] = FINANCIAL_REPORT_TYPES
        if self.args.test_limit:
            report_types = list(report_types)[: self.args.test_limit]
        for report_type in report_types:
            rows = self.paging(
                election,
                self.election_dir(election) / "financial" / str(report_type),
                "/reports/77/search",
                {
                    "reportType": report_type,
                    "electionId": election["externalId"],
                },
            )
            if self.args.no_files:
                continue
            if self.args.test_limit:
                rows = rows[: self.args.test_limit]
            for row in rows:
                body = row.get("body") or {}
                financial_id = body.get("id") or (
                    row.get("extraParameters") or {}
                ).get("financialReportId")
                if not financial_id:
                    continue
                original_name = str(
                    body.get("fileName") or f"{financial_id}.bin"
                )
                filename = f"{financial_id}_{safe_filename(original_name)}"
                target = (
                    self.out
                    / "financial_files"
                    / str(report_type)
                    / filename
                )
                if (
                    target.exists()
                    and target.stat().st_size
                    and not self.args.refresh
                ):
                    continue
                try:
                    data, headers = self.client.download(
                        "/reports/77/files",
                        {"financialReportId": financial_id},
                    )
                    if not data:
                        raise ValueError("Пустой файл")
                    if Path(filename).suffix == "":
                        extension = (
                            mimetypes.guess_extension(
                                headers.get("content-type", "")
                            )
                            or ".bin"
                        )
                        target = target.with_suffix(extension)
                    atomic_write_bytes(target, data)
                except ExpectedUnavailable as error:
                    # At the time of testing (2026-09-21) the public frontend
                    # advertised this endpoint, but the server sometimes routed
                    # /files to /{id} and returned HTTP 400. Keep this visible in
                    # errors.jsonl so a run never claims that missing documents
                    # were successfully downloaded. A later rerun will retry it.
                    self.error(
                        election["id"],
                        "financial-file-unavailable",
                        error,
                        {
                            "financialReportId": financial_id,
                            "fileName": original_name,
                        },
                    )
                except Exception as error:
                    self.error(
                        election["id"],
                        "financial-file",
                        error,
                        {
                            "financialReportId": financial_id,
                            "fileName": original_name,
                        },
                    )

    def process_one(
        self, election: dict[str, Any], position: int, total: int
    ) -> None:
        eid = election["id"]
        name = str(election.get("name") or "")
        self.log(f"[{position}/{total}] {eid}: {name}")
        try:
            detail = self.get_detail(election)
            stages = self.args.stages
            commission_rows: list[dict[str, Any]] = []
            if "commissions" in stages or "reports" in stages:
                commission_rows = self.commissions(election, detail)
            election_report_responses: list[Any] = []
            if "reports" in stages:
                election_report_responses = self.election_reports(election)
                self.commission_reports(
                    election, detail, commission_rows
                )
            elif "candidates" in stages:
                election_report_responses = self.election_reports(election)
            if "candidates" in stages:
                self.candidates(
                    election, detail, election_report_responses
                )
            if "financial" in stages:
                self.financial(election)
            atomic_write_json(
                self.election_dir(election) / "complete.json",
                {
                    "electionId": eid,
                    "completedStages": sorted(stages),
                    "completedAt": dt.datetime.now().astimezone().isoformat(),
                    "testLimit": self.args.test_limit,
                    "downloadFiles": not self.args.no_files,
                    "commissionShards": self.args.commission_shards,
                    "commissionShard": self.args.commission_shard,
                    "errorsThisRun": self.error_counts.get(eid, 0),
                    "status": "with-errors" if self.error_counts.get(eid, 0) else "processed",
                },
            )
            self.mark_complete(eid)
        except Exception as error:
            self.error(eid, "election", error)
            self.log(f"  ОШИБКА {eid}: {error!r}")

    def run(self) -> None:
        self.log(
            f"[СТАРТ] Проверяю каталог выборов {self.args.from_date} — "
            f"{self.args.to_date}. Ожидание одного HTTP-ответа: "
            f"до {self.args.timeout} с; при сбое будут повторы."
        )
        records = self.catalog()
        self.total_records = len(records)
        self.log(f"Выборов в текущем запуске: {len(records)}")
        atomic_write_json(
            self.out / "run_manifest.json",
            {
                "startedAt": dt.datetime.now().astimezone().isoformat(),
                "dateFrom": self.args.from_date,
                "dateTo": self.args.to_date,
                "selectedElections": len(records),
                "stages": sorted(self.args.stages),
                "sample": self.args.sample,
                "electionId": self.args.election_id,
                "excludedElectionIds": self.args.exclude_election_id,
                "testLimit": self.args.test_limit,
                "downloadFiles": not self.args.no_files,
                "workers": self.args.workers,
                "commissionWorkers": self.args.commission_workers,
                "commissionShards": self.args.commission_shards,
                "commissionShard": self.args.commission_shard,
                "delay": self.args.delay,
            },
        )
        stop_monitor = threading.Event()

        def monitor():
            previous = self.client.snapshot()
            last = time.monotonic()
            while not stop_monitor.wait(15):
                current = self.client.snapshot()
                now = time.monotonic()
                speed = (current['ok'] - previous['ok']) / (now - last)
                self.log(f"[СЕТЬ] успешных HTTP-ответов/с: {speed:.1f}; "
                         f"всего: {current['ok']}; повторов: {current['retries']}; "
                         f"HTTP 429: {current['http429']} (кэш не включён)")
                previous, last = current, now
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()
        try:
            self.run_workers(records)
        finally:
            stop_monitor.set()
            monitor_thread.join()
            atomic_write_json(self.out / 'network_stats.json', self.client.snapshot())
            self.client.close()
        build_exports(self.out)
        errors = sum(self.error_counts.values())
        self.log(f"Обход закончен: {self.out}; ошибок за этот запуск: {errors}. "
                 "Подробности: errors.jsonl. Повторный запуск повторит неудачные запросы.")

    def run_workers(self, records) -> None:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.args.commission_workers
        ) as report_pool, concurrent.futures.ThreadPoolExecutor(
            max_workers=self.args.workers
        ) as pool:
            self.report_pool = report_pool
            futures = [
                pool.submit(
                    self.process_one, election, index, len(records)
                )
                for index, election in enumerate(records, 1)
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()


def unwrap_saved(path: Path) -> tuple[dict[str, Any], Any]:
    saved = read_json(path)
    if isinstance(saved, dict) and "response" in saved:
        return saved.get("request") or {}, saved["response"]
    return {}, saved


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    count = 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count


def build_exports(out: Path) -> None:
    """Build normalized views from cached raw responses without network access."""
    raw = out / "raw" / "elections"
    exports = out / "exports"
    exports.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}

    def election_details() -> Iterator[dict[str, Any]]:
        for path in sorted(raw.glob("*/detail.json")):
            election_id = path.parent.name
            _, response = unwrap_saved(path)
            if isinstance(response, dict):
                yield {"_numericElectionId": election_id, **response}

    counts["elections"] = write_jsonl(
        exports / "elections.jsonl", election_details()
    )

    def commission_rows() -> Iterator[dict[str, Any]]:
        for path in sorted(raw.glob("*/commissions_flat.jsonl")):
            election_id = path.parent.name
            with path.open(encoding="utf-8") as stream:
                for line in stream:
                    with contextlib.suppress(json.JSONDecodeError):
                        yield {
                            "electionId": election_id,
                            **json.loads(line),
                        }

    counts["commissions"] = write_jsonl(
        exports / "commissions.jsonl", commission_rows()
    )

    def candidate_rows() -> Iterator[dict[str, Any]]:
        seen: set[tuple[str, str, str]] = set()
        for path in sorted(raw.glob("*/candidates/**/page_*.json")):
            election_id = path.parts[len(raw.parts)]
            request, response = unwrap_saved(path)
            body = request.get("body") or {}
            mode = path.parent.name
            for row in response.get("content") or []:
                key = (election_id, mode, str(row.get("id")))
                if key in seen:
                    continue
                seen.add(key)
                yield {
                    "electionId": election_id,
                    "mode": mode,
                    "associationId": body.get("associationId"),
                    **row,
                }

    counts["candidates"] = write_jsonl(
        exports / "candidates.jsonl", candidate_rows()
    )

    def candidate_cards() -> Iterator[dict[str, Any]]:
        for path in sorted(raw.glob("*/candidate_cards/*.json")):
            election_id = path.parts[len(raw.parts)]
            _, response = unwrap_saved(path)
            if isinstance(response, dict):
                yield {
                    "electionId": election_id,
                    "reportId": response.get("id"),
                    "createdAt": response.get("createdAt"),
                    **(response.get("body") or {}),
                }

    counts["candidate_cards"] = write_jsonl(
        exports / "candidate_cards.jsonl", candidate_cards()
    )

    def associations() -> Iterator[dict[str, Any]]:
        for path in sorted(raw.glob("*/associations/*.json")):
            election_id = path.parts[len(raw.parts)]
            _, response = unwrap_saved(path)
            if isinstance(response, dict):
                yield {"electionId": election_id, **response}

    counts["associations"] = write_jsonl(
        exports / "associations.jsonl", associations()
    )

    report_globs = (
        "*/election_reports/*.json",
        "*/commission_reports/*.json",
        "*/results/*.json",
        "*/deg_results/*.json",
    )

    def reports() -> Iterator[dict[str, Any]]:
        for pattern in report_globs:
            for path in sorted(raw.glob(pattern)):
                election_id = path.parts[len(raw.parts)]
                request, response = unwrap_saved(path)
                if isinstance(response, dict):
                    yield {
                        "electionId": election_id,
                        "sourceGroup": path.parent.name,
                        "request": request,
                        **response,
                    }

    counts["report_responses"] = write_jsonl(
        exports / "report_responses.jsonl", reports()
    )

    def result_rows() -> Iterator[dict[str, Any]]:
        for group in ("results", "deg_results"):
            for path in sorted(raw.glob(f"*/{group}/*.json")):
                election_id = path.parts[len(raw.parts)]
                request, response = unwrap_saved(path)
                params = request.get("params") or {}
                body = (
                    response.get("body") or {}
                    if isinstance(response, dict)
                    else {}
                )
                for row in body.get("records") or []:
                    yield {
                        "electionId": election_id,
                        "isDeg": group == "deg_results",
                        "commissionClassifierId": body.get(
                            "commissionClassifierId"
                        )
                        or params.get("commissionClassifierId"),
                        "parentCommissionClassifierId": body.get(
                            "parentCommissionClassifierId"
                        )
                        or params.get("parentCommissionClassifierId"),
                        "protocolId": body.get("protocolId"),
                        "protocolNum": body.get("protocolNum")
                        or params.get("protocolNum"),
                        "signed": body.get("signed"),
                        "signDateTime": body.get("signDateTime"),
                        "referendumQuestionNum": body.get(
                            "referendumQuestionNum"
                        )
                        or params.get("referendumQuestionNum"),
                        **row,
                    }

    counts["result_rows"] = write_jsonl(
        exports / "result_rows.jsonl", result_rows()
    )

    def financial_rows() -> Iterator[dict[str, Any]]:
        for path in sorted(raw.glob("*/financial/*/page_*.json")):
            election_id = path.parts[len(raw.parts)]
            request, response = unwrap_saved(path)
            body_request = request.get("body") or {}
            for row in response.get("content") or []:
                yield {
                    "electionNumericId": election_id,
                    "requestedReportType": body_request.get("reportType"),
                    **row,
                }

    counts["financial_reports"] = write_jsonl(
        exports / "financial_reports.jsonl", financial_rows()
    )

    completed = list(raw.glob("*/complete.json"))
    completed_full = 0
    completed_test = 0
    completed_partial = 0
    legacy_unverified = 0
    completed_with_errors = 0
    for marker in completed:
        try:
            marker_data = read_json(marker)
            if "errorsThisRun" not in marker_data:
                legacy_unverified += 1
                continue
            if marker_data.get("errorsThisRun", 0):
                completed_with_errors += 1
                continue
            if int(marker_data.get("testLimit") or 0):
                completed_test += 1
            elif (set(marker_data.get("completedStages", [])) != parse_stages("all")
                  or bool(marker_data.get('commissionShard'))
                  or not marker_data.get("downloadFiles")):
                completed_partial += 1
            else:
                completed_full += 1
        except Exception:
            continue
    error_path = out / "errors.jsonl"
    errors = 0
    if error_path.exists():
        with error_path.open(encoding="utf-8") as stream:
            errors = sum(1 for line in stream if line.strip())
    financial_dir = out / "financial_files"
    audit = {
        "generatedAt": dt.datetime.now().astimezone().isoformat(),
        "source": ORIGIN,
        "completedElectionMarkers": len(completed),
        "completedFullElections": completed_full,
        "completedTestElections": completed_test,
        "completedPartialElections": completed_partial,
        "completedWithErrors": completed_with_errors,
        "legacyUnverifiedMarkers": legacy_unverified,
        "financialFiles": sum(
            1 for path in financial_dir.rglob("*") if path.is_file()
        )
        if financial_dir.exists()
        else 0,
        "loggedErrors": errors,
        **counts,
    }
    atomic_write_json(out / "audit_full.json", audit)


def parse_stages(value: str) -> set[str]:
    aliases = {
        "all": {"commissions", "reports", "candidates", "financial"},
        "core": {"commissions"},
    }
    values = {
        part.strip().lower() for part in value.split(",") if part.strip()
    }
    result: set[str] = set()
    for item in values:
        result.update(aliases.get(item, {item}))
    allowed = {"commissions", "reports", "candidates", "financial"}
    unknown = result - allowed
    if unknown:
        raise argparse.ArgumentTypeError(
            "Неизвестные этапы: " + ", ".join(sorted(unknown))
        )
    return result


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Полная возобновляемая выгрузка открытых данных izbirkom.ru"
        )
    )
    parser.add_argument("--from-date", default="2026-09-18")
    parser.add_argument("--to-date", default="2026-09-20")
    parser.add_argument("--output", default="data/izbirkom_2026-09-18_20")
    parser.add_argument(
        "--stages", type=parse_stages, default=parse_stages("all")
    )
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument('--commission-shards', type=int, default=1)
    parser.add_argument('--commission-shard', type=int, action='append', default=[],
                        help='часть комиссий, от 0 до commission-shards-1; можно повторять')
    parser.add_argument("--commission-workers", type=int, default=24,
                        help="общий пул комиссий для всех выборов (по умолчанию 24)")
    parser.add_argument("--delay", type=float, default=0.04)
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument(
        "--sample", type=int, default=0, help="обработать первые N выборов"
    )
    parser.add_argument(
        "--election-id",
        default="",
        help="обработать один numeric ID или external UUID",
    )
    parser.add_argument(
        "--exclude-election-id", action="append", default=[],
        help="исключить numeric ID или UUID; параметр можно повторять",
    )
    parser.add_argument(
        "--no-files",
        action="store_true",
        help="не скачивать PDF/DOCX/XLSX",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="заново запросить уже сохранённые JSON",
    )
    parser.add_argument(
        "--test-limit",
        type=int,
        default=0,
        help=(
            "ТОЛЬКО ДЛЯ ПРОВЕРКИ: ограничить комиссии, карточки "
            "и финансовые типы"
        ),
    )
    parser.add_argument(
        "--export-only",
        action="store_true",
        help="только пересобрать exports/ из уже скачанного raw/",
    )
    return parser


def main() -> None:
    parser = make_parser()
    args = parser.parse_args()
    if args.commission_shards < 1 or any(part < 0 or part >= args.commission_shards
                                        for part in args.commission_shard):
        parser.error('Некорректные номера частей комиссий')
    args.workers = max(1, args.workers)
    args.commission_workers = max(1, args.commission_workers)
    args.page_size = max(1, min(5000, args.page_size))
    if args.export_only:
        build_exports(Path(args.output).resolve())
        print("Экспорт пересобран.")
        return
    FullCrawler(args).run()


if __name__ == "__main__":
    main()
