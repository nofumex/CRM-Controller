import html
import json
import os
import re
import sqlite3
import threading
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"
DB_PATH = ROOT / "bot_data.sqlite3"
KRASNOYARSK_TZ = "Asia/Krasnoyarsk"
DB_INIT_LOCK = threading.RLock()
SNAPSHOT_REFRESH_SECONDS = 15 * 60
SNAPSHOT_REFRESH_LOCK = threading.Lock()
SNAPSHOT_MEMORY_LOCK = threading.RLock()
SNAPSHOT_MEMORY: Dict[str, Tuple[Dict[str, Any], str, str]] = {}
DEAL_DETAIL_MEMORY_LOCK = threading.RLock()
DEAL_DETAIL_MEMORY: Dict[int, Dict[str, Any]] = {}

REFERRAL_PIPELINE_IDS = (10915210,)
LAWYER_PIPELINE_IDS = (11110422,)
JUDICIAL_PIPELINE_ID = 11037010
SALES_PIPELINE_ID = 867829
SALES_FAILED_STATUS_ID = 143
FAILED_REASON_FIELD_ID = 589709

SOURCE_LABELS = {
    "court": "Клиенты по судебному приказу",
    "agents": "Заявки от агентов реферальной программы",
    "lawyers": "Контакты от юристов",
}

SALES_GROUP_STATUSES = {
    "не смог реализовать",
    "не было коммуникации",
    "мертвое царство",
    "закрыто и не реализовано",
    "принял заявку",
}


def load_env(path: Path) -> Dict[str, str]:
    values = dict(os.environ)
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return values


ENV = load_env(ENV_PATH)


def local_tz() -> timezone:
    if ZoneInfo:
        return ZoneInfo(KRASNOYARSK_TZ)
    return timezone(timedelta(hours=7))


def now_local() -> datetime:
    return datetime.now(local_tz())


def utc_dt(value: str) -> datetime:
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def utc_iso_from_dt(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat() + "Z"


def local_day_bounds_utc(local_day: Optional[Any] = None) -> Tuple[str, str]:
    day = local_day or now_local().date()
    start_local = datetime.combine(day, datetime.min.time()).replace(tzinfo=local_tz())
    end_local = start_local + timedelta(days=1)
    return utc_iso_from_dt(start_local), utc_iso_from_dt(end_local)


def format_local_timestamp(value: str) -> str:
    try:
        return utc_dt(value).astimezone(local_tz()).strftime("%Y-%m-%d %H:%M:%S GMT+7")
    except Exception:
        return str(value)


def is_weekend_local(value: Optional[datetime] = None) -> bool:
    current = value or now_local()
    return current.astimezone(local_tz()).weekday() >= 5


def utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=False)


def chunks(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


class ApiError(RuntimeError):
    pass


def http_json(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    payload: Optional[Any] = None,
    timeout: int = 60,
    retries: int = 3,
    session: Optional[requests.Session] = None,
) -> Any:
    request_headers = dict(headers or {})
    request_headers.setdefault("Accept", "application/json")
    transport = session or requests.Session()

    for attempt in range(retries + 1):
        try:
            response = transport.request(
                method,
                url,
                headers=request_headers,
                json=payload,
                timeout=timeout,
            )
            if response.status_code == 204:
                return None
            if response.status_code == 429 and attempt < retries:
                retry_after = int(response.headers.get("Retry-After", "2"))
                time.sleep(max(retry_after, 2))
                continue
            if response.status_code >= 400:
                raise ApiError(f"{method} {url} -> {response.status_code}: {response.text}")
            return response.json() if response.content else None
        except requests.RequestException as exc:
            if attempt < retries:
                time.sleep(min(2 * (attempt + 1), 8))
                continue
            raise ApiError(f"{method} {url} -> network error: {exc}") from exc


class AmoClient:
    def __init__(self, base_url: str, token: str):
        if not base_url or not token:
            raise ApiError("AMOCRM_BASE_URL and AMOCRM_ACCESS_TOKEN are required")
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}
        self.session = requests.Session()
        self.session.trust_env = False

    def _url(self, path: str, params: Optional[List[Tuple[str, Any]]] = None) -> str:
        if params:
            return f"{self.base_url}{path}?{urlencode(params)}"
        return f"{self.base_url}{path}"

    def get(self, path: str, params: Optional[List[Tuple[str, Any]]] = None) -> Any:
        return http_json("GET", self._url(path, params), self.headers, session=self.session)

    def patch(self, path: str, payload: Any) -> Any:
        return http_json("PATCH", self._url(path), self.headers, payload, session=self.session)

    def lead_url(self, lead_id: int) -> str:
        return f"{self.base_url}/leads/detail/{lead_id}"

    def list_pipelines(self) -> List[Dict[str, Any]]:
        data = self.get("/api/v4/leads/pipelines")
        return data.get("_embedded", {}).get("pipelines", [])

    def get_pipeline(self, pipeline_id: int) -> Dict[str, Any]:
        return self.get(f"/api/v4/leads/pipelines/{pipeline_id}")

    def status_name(self, pipeline_id: int, status_id: int) -> str:
        pipeline = self.get_pipeline(pipeline_id)
        for status in pipeline.get("_embedded", {}).get("statuses", []):
            if int(status["id"]) == int(status_id):
                return status["name"]
        return str(status_id)

    def pipeline_name(self, pipeline_id: int) -> str:
        return self.get_pipeline(pipeline_id).get("name", str(pipeline_id))

    def list_users(self) -> List[Dict[str, Any]]:
        data = self.get("/api/v4/users", [("limit", 250)])
        return data.get("_embedded", {}).get("users", [])

    def find_user(self, query: str) -> Optional[Dict[str, Any]]:
        users = self.list_users()
        query = query.strip()
        if query.isdigit():
            for user in users:
                if int(user["id"]) == int(query):
                    return user
        lower_query = query.lower()
        matches = [u for u in users if lower_query in u.get("name", "").lower()]
        if len(matches) == 1:
            return matches[0]
        exact = [u for u in users if lower_query == u.get("name", "").lower()]
        return exact[0] if exact else None

    def list_leads_by_status(self, pipeline_id: int, status_id: int) -> List[Dict[str, Any]]:
        all_leads: List[Dict[str, Any]] = []
        page = 1
        while True:
            params = [
                ("filter[statuses][0][pipeline_id]", pipeline_id),
                ("filter[statuses][0][status_id]", status_id),
                ("limit", 250),
                ("page", page),
            ]
            data = self.get("/api/v4/leads", params) or {}
            batch = data.get("_embedded", {}).get("leads", [])
            all_leads.extend(batch)
            if not data.get("_links", {}).get("next") or not batch:
                return all_leads
            page += 1

    def get_leads_by_ids(self, lead_ids: List[int]) -> List[Dict[str, Any]]:
        found: List[Dict[str, Any]] = []
        for part in chunks(lead_ids, 50):
            params: List[Tuple[str, Any]] = [("limit", 250)]
            params.extend(("filter[id][]", lead_id) for lead_id in part)
            data = self.get("/api/v4/leads", params) or {}
            found.extend(data.get("_embedded", {}).get("leads", []))
        return found

    def list_pipeline_leads(self, pipeline_id: int, with_contacts: bool = False) -> List[Dict[str, Any]]:
        pipeline = self.get_pipeline(pipeline_id)
        found: Dict[int, Dict[str, Any]] = {}
        for status in pipeline.get("_embedded", {}).get("statuses", []):
            page = 1
            while True:
                params: List[Tuple[str, Any]] = [
                    ("filter[statuses][0][pipeline_id]", pipeline_id),
                    ("filter[statuses][0][status_id]", int(status["id"])),
                    ("limit", 250),
                    ("page", page),
                ]
                if with_contacts:
                    params.append(("with", "contacts"))
                data = self.get("/api/v4/leads", params) or {}
                batch = data.get("_embedded", {}).get("leads", [])
                for lead in batch:
                    found[int(lead["id"])] = lead
                if not batch or not data.get("_links", {}).get("next"):
                    break
                page += 1
        return list(found.values())

    def get_contacts_by_ids(self, contact_ids: Iterable[int]) -> List[Dict[str, Any]]:
        found: List[Dict[str, Any]] = []
        for part in chunks(sorted(set(int(value) for value in contact_ids)), 50):
            params: List[Tuple[str, Any]] = [("limit", 250), ("with", "leads")]
            params.extend(("filter[id][]", contact_id) for contact_id in part)
            data = self.get("/api/v4/contacts", params) or {}
            found.extend(data.get("_embedded", {}).get("contacts", []))
        return found

    def find_contacts(self, query: str) -> List[Dict[str, Any]]:
        data = self.get(
            "/api/v4/contacts",
            [("query", query), ("limit", 250), ("with", "leads")],
        ) or {}
        return data.get("_embedded", {}).get("contacts", [])

    def get_lead(self, lead_id: int) -> Dict[str, Any]:
        data = self.get(f"/api/v4/leads/{lead_id}")
        if not data:
            raise ApiError(f"Сделка {lead_id} не найдена")
        return data

    def lead_status_events(self, lead_ids: Iterable[int]) -> List[Dict[str, Any]]:
        all_events: Dict[str, Dict[str, Any]] = {}
        for part in chunks(sorted(set(int(value) for value in lead_ids)), 10):
            page = 1
            while True:
                params: List[Tuple[str, Any]] = [
                    ("filter[type]", "lead_status_changed"),
                    ("filter[entity]", "lead"),
                    ("limit", 100),
                    ("page", page),
                ]
                params.extend(("filter[entity_id][]", lead_id) for lead_id in part)
                data = self.get("/api/v4/events", params) or {}
                events = data.get("_embedded", {}).get("events", [])
                for event in events:
                    all_events[str(event["id"])] = event
                if not events or not data.get("_links", {}).get("next"):
                    break
                page += 1
        return sorted(all_events.values(), key=lambda item: (int(item.get("created_at") or 0), str(item["id"])))


class TelegramClient:
    def __init__(self, token: str):
        if not token:
            raise ApiError("TELEGRAM_BOT_TOKEN is required")
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.session = requests.Session()
        self.session.trust_env = False

    def api(self, method: str, payload: Dict[str, Any], retries: int = 3) -> Any:
        return http_json(
            "POST",
            f"{self.base_url}/{method}",
            payload=payload,
            retries=retries,
            session=self.session,
        )

    def get_updates(self, offset: Optional[int]) -> List[Dict[str, Any]]:
        payload = {"timeout": 25, "allowed_updates": ["message", "callback_query"]}
        if offset is not None:
            payload["offset"] = offset
        data = self.api("getUpdates", payload)
        return data.get("result", [])

    def send(self, chat_id: int, text: str, keyboard: Optional[List[List[Dict[str, str]]]] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if keyboard:
            payload["reply_markup"] = {"inline_keyboard": keyboard}
        return self.api("sendMessage", payload)

    def edit(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        keyboard: Optional[List[List[Dict[str, str]]]] = None,
    ) -> None:
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "reply_markup": {"inline_keyboard": keyboard or []},
        }
        try:
            self.api("editMessageText", payload, retries=0)
        except ApiError as exc:
            if "message is not modified" not in str(exc):
                raise

    def delete(self, chat_id: int, message_id: int) -> None:
        try:
            self.api("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
        except ApiError:
            pass

    def answer_callback(self, callback_id: str, text: str = "") -> None:
        try:
            self.api("answerCallbackQuery", {"callback_query_id": callback_id, "text": text}, retries=0)
        except ApiError:
            pass


@contextmanager
def db() -> Iterable[sqlite3.Connection]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def ensure_db_ready() -> None:
    with DB_INIT_LOCK:
        conn = sqlite3.connect(DB_PATH)
        try:
            required = {"settings", "monitor_snapshots", "deal_details", "chat_panels"}
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('settings', 'monitor_snapshots', 'deal_details', 'chat_panels')"
            ).fetchall()
        finally:
            conn.close()
        if {row[0] for row in rows} != required:
            init_db()


def init_db() -> None:
    with DB_INIT_LOCK:
        with db() as conn:
            conn.executescript(
                """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS managers (
                user_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                daily_limit INTEGER NOT NULL DEFAULT 15,
                enabled INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 100
            );

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                trigger TEXT NOT NULL,
                source_pipeline_id INTEGER NOT NULL,
                source_status_id INTEGER NOT NULL,
                dest_pipeline_id INTEGER NOT NULL,
                dest_status_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                message TEXT
            );

            CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                lead_id INTEGER NOT NULL,
                lead_name TEXT NOT NULL,
                lead_number INTEGER,
                from_pipeline_id INTEGER NOT NULL,
                from_status_id INTEGER NOT NULL,
                to_pipeline_id INTEGER NOT NULL,
                to_status_id INTEGER NOT NULL,
                responsible_user_id INTEGER NOT NULL,
                responsible_name TEXT NOT NULL,
                moved_at TEXT NOT NULL,
                UNIQUE(run_id, lead_id)
            );

            CREATE TABLE IF NOT EXISTS source_history_leads (
                lead_id INTEGER PRIMARY KEY,
                source_pipeline_id INTEGER NOT NULL,
                source_status_id INTEGER,
                discovered_from TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_event_at INTEGER
            );

            CREATE TABLE IF NOT EXISTS admin_states (
                chat_id INTEGER PRIMARY KEY,
                state TEXT NOT NULL,
                payload TEXT
            );

            CREATE TABLE IF NOT EXISTS monitor_snapshots (
                source_key TEXT PRIMARY KEY,
                refreshed_at TEXT NOT NULL,
                payload TEXT NOT NULL,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS deal_details (
                lead_id INTEGER PRIMARY KEY,
                refreshed_at TEXT NOT NULL,
                payload TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS chat_panels (
                chat_id INTEGER PRIMARY KEY,
                message_id INTEGER NOT NULL
            );
            """
            )
def get_setting(key: str) -> str:
    ensure_db_ready()
    with db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else ""


def set_setting(key: str, value: Any) -> None:
    ensure_db_ready()
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )


def config() -> Dict[str, int]:
    return {
        "source_pipeline_id": int(get_setting("source_pipeline_id")),
        "source_status_id": int(get_setting("source_status_id")),
        "dest_pipeline_id": int(get_setting("dest_pipeline_id")),
        "dest_status_id": int(get_setting("dest_status_id")),
    }


def managers(enabled_only: bool = False) -> List[sqlite3.Row]:
    ensure_db_ready()
    query = "SELECT * FROM managers"
    if enabled_only:
        query += " WHERE enabled = 1"
    query += " ORDER BY sort_order, name"
    with db() as conn:
        return conn.execute(query).fetchall()


def lead_number(lead: Dict[str, Any]) -> Optional[int]:
    match = re.search(r"#(\d+)\s*$", str(lead.get("name", "")))
    return int(match.group(1)) if match else None


def set_state(chat_id: int, state: str, payload: Optional[Dict[str, Any]] = None) -> None:
    ensure_db_ready()
    with db() as conn:
        conn.execute(
            "INSERT INTO admin_states(chat_id, state, payload) VALUES(?, ?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET state = excluded.state, payload = excluded.payload",
            (chat_id, state, json.dumps(payload or {}, ensure_ascii=False)),
        )


def get_state(chat_id: int) -> Optional[sqlite3.Row]:
    ensure_db_ready()
    with db() as conn:
        return conn.execute("SELECT * FROM admin_states WHERE chat_id = ?", (chat_id,)).fetchone()


def clear_state(chat_id: int) -> None:
    ensure_db_ready()
    with db() as conn:
        conn.execute("DELETE FROM admin_states WHERE chat_id = ?", (chat_id,))


def keyboard(rows: List[List[Tuple[str, str]]]) -> List[List[Dict[str, str]]]:
    return [[{"text": text, "callback_data": data} for text, data in row] for row in rows]


def show(
    tg: TelegramClient,
    chat_id: int,
    text: str,
    markup: Optional[List[List[Dict[str, str]]]] = None,
    message_id: Optional[int] = None,
) -> Dict[str, Any]:
    if message_id:
        tg.edit(chat_id, message_id, text, markup)
        return {"ok": True, "result": {"message_id": message_id}}
    return tg.send(chat_id, text, markup)


def send_panel(
    tg: TelegramClient,
    chat_id: int,
    text: str,
    markup: Optional[List[List[Dict[str, str]]]] = None,
) -> Dict[str, Any]:
    ensure_db_ready()
    sent = tg.send(chat_id, text, markup)
    new_message_id = int(sent.get("result", {}).get("message_id") or 0)
    if new_message_id:
        with db() as conn:
            conn.execute(
                "INSERT INTO chat_panels(chat_id, message_id) VALUES(?, ?) "
                "ON CONFLICT(chat_id) DO UPDATE SET message_id = excluded.message_id",
                (chat_id, new_message_id),
            )
    return sent


def remember_panel(chat_id: int, message_id: int) -> None:
    if not message_id:
        return
    ensure_db_ready()
    with db() as conn:
        conn.execute(
            "INSERT INTO chat_panels(chat_id, message_id) VALUES(?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET message_id = excluded.message_id",
            (chat_id, message_id),
        )


def main_menu() -> List[List[Dict[str, str]]]:
    return keyboard(
        [
            [("⚖️ Клиенты по судебному приказу", "source:court")],
            [("🤝 Заявки от агентов", "source:agents")],
            [("🧑‍⚖️ Контакты от юристов", "source:lawyers")],
        ]
    )


def amo() -> AmoClient:
    return AmoClient(ENV.get("AMOCRM_BASE_URL", ""), ENV.get("AMOCRM_ACCESS_TOKEN", ""))


def format_route(client: AmoClient) -> str:
    cfg = config()
    try:
        src_pipeline = client.pipeline_name(cfg["source_pipeline_id"])
        src_status = client.status_name(cfg["source_pipeline_id"], cfg["source_status_id"])
        dst_pipeline = client.pipeline_name(cfg["dest_pipeline_id"])
        dst_status = client.status_name(cfg["dest_pipeline_id"], cfg["dest_status_id"])
    except Exception:
        src_pipeline = str(cfg["source_pipeline_id"])
        src_status = str(cfg["source_status_id"])
        dst_pipeline = str(cfg["dest_pipeline_id"])
        dst_status = str(cfg["dest_status_id"])
    return (
        f"<b>Откуда</b>\n{esc(src_pipeline)} → {esc(src_status)}\n\n"
        f"<b>Куда</b>\n{esc(dst_pipeline)} → {esc(dst_status)}"
    )


def summary_text() -> str:
    client = amo()
    mgr_lines = []
    for manager in managers():
        mark = "✅" if manager["enabled"] else "⏸"
        mgr_lines.append(f"{mark} {esc(manager['name'])}: <b>{manager['daily_limit']}</b>")
    return (
        "⚙️ <b>Настройки переводов</b>\n\n"
        f"{format_route(client)}\n\n"
        f"<b>Время</b>\n{esc(get_setting('schedule_time'))} GMT+7\n\n"
        "<b>Менеджеры</b>\n" + "\n".join(mgr_lines)
    )


def start_run(trigger: str, cfg: Dict[str, int]) -> int:
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO runs(started_at, trigger, source_pipeline_id, source_status_id,
                             dest_pipeline_id, dest_status_id, status)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_iso(),
                trigger,
                cfg["source_pipeline_id"],
                cfg["source_status_id"],
                cfg["dest_pipeline_id"],
                cfg["dest_status_id"],
                "running",
            ),
        )
        return int(cur.lastrowid)


def finish_run(run_id: int, status: str, message: str) -> None:
    with db() as conn:
        conn.execute(
            "UPDATE runs SET finished_at = ?, status = ?, message = ? WHERE id = ?",
            (utc_iso(), status, message, run_id),
        )


def run_status(run_id: int) -> str:
    with db() as conn:
        row = conn.execute("SELECT status FROM runs WHERE id = ?", (run_id,)).fetchone()
        return row["status"] if row else "unknown"


def run_title(run_id: int, auto: bool = False) -> str:
    status = run_status(run_id)
    mark = {"done": "✅", "blocked": "🛑", "empty": "▫️", "error": "⚠️", "running": "⏳"}.get(status, "•")
    label = "Автозапуск" if auto else "Запуск"
    return f"{mark} {label} #{run_id}"


def record_transfer(run_id: int, lead: Dict[str, Any], manager: sqlite3.Row, cfg: Dict[str, int]) -> None:
    with db() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO transfers(
                run_id, lead_id, lead_name, lead_number, from_pipeline_id, from_status_id,
                to_pipeline_id, to_status_id, responsible_user_id, responsible_name, moved_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                int(lead["id"]),
                lead.get("name", str(lead["id"])),
                lead_number(lead),
                cfg["source_pipeline_id"],
                cfg["source_status_id"],
                cfg["dest_pipeline_id"],
                cfg["dest_status_id"],
                int(manager["user_id"]),
                manager["name"],
                utc_iso(),
            ),
        )
        conn.execute(
            """
            INSERT INTO source_history_leads(
                lead_id, source_pipeline_id, source_status_id, discovered_from, first_seen_at, last_event_at
            )
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(lead_id) DO UPDATE SET
                source_pipeline_id = excluded.source_pipeline_id,
                source_status_id = excluded.source_status_id,
                discovered_from = source_history_leads.discovered_from || ',bot',
                last_event_at = COALESCE(excluded.last_event_at, source_history_leads.last_event_at)
            """,
            (
                int(lead["id"]),
                cfg["source_pipeline_id"],
                cfg["source_status_id"],
                "bot",
                utc_iso(),
                None,
            ),
        )


RUN_LOCK = threading.Lock()


def daily_transfer_capacity(active_managers: Optional[List[sqlite3.Row]] = None) -> int:
    selected = active_managers if active_managers is not None else managers(enabled_only=True)
    return sum(max(0, int(manager["daily_limit"])) for manager in selected)


def transfers_on_local_date(local_day: Optional[Any] = None) -> int:
    start_utc, end_utc = local_day_bounds_utc(local_day)
    with db() as conn:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM transfers WHERE moved_at >= ? AND moved_at < ?",
                (start_utc, end_utc),
            ).fetchone()[0]
        )


def perform_transfer(trigger: str = "manual") -> Tuple[int, str]:
    if not RUN_LOCK.acquire(blocking=False):
        raise RuntimeError("Перевод уже выполняется")
    try:
        cfg = config()
        run_id = start_run(trigger, cfg)
        try:
            if is_weekend_local():
                message = "Сегодня выходной по GMT+7. Переводы сделок в субботу и воскресенье заблокированы."
                finish_run(run_id, "blocked", message)
                return run_id, message

            active_managers = managers(enabled_only=True)
            if not active_managers:
                raise RuntimeError("Нет активных менеджеров")

            client = amo()
            capacity = daily_transfer_capacity(active_managers)
            today_count = transfers_on_local_date()
            if today_count > 0:
                message = (
                    "Сегодня по GMT+7 перевод уже выполнялся. "
                    f"Уже переведено: {today_count}. Лимит на одну итерацию: {capacity}. "
                    "Новая итерация заблокирована."
                )
                finish_run(run_id, "blocked", message)
                return run_id, message
            if capacity <= 0:
                message = "Лимит переводов равен 0. Ничего не перевожу."
                finish_run(run_id, "blocked", message)
                return run_id, message

            source_leads = client.list_leads_by_status(cfg["source_pipeline_id"], cfg["source_status_id"])
            source_leads.sort(key=lambda item: (lead_number(item) or 0, int(item["id"])), reverse=True)

            assignments: List[Tuple[Dict[str, Any], sqlite3.Row]] = []
            cursor = 0
            for manager in active_managers:
                limit = max(0, int(manager["daily_limit"]))
                for lead in source_leads[cursor : cursor + limit]:
                    assignments.append((lead, manager))
                cursor += limit

            if len(assignments) > capacity:
                assignments = assignments[:capacity]

            if not assignments:
                finish_run(run_id, "empty", "Нет сделок для перевода")
                return run_id, "Нет сделок для перевода"

            payload = [
                {
                    "id": int(lead["id"]),
                    "pipeline_id": cfg["dest_pipeline_id"],
                    "status_id": cfg["dest_status_id"],
                    "responsible_user_id": int(manager["user_id"]),
                }
                for lead, manager in assignments
            ]
            client.move_leads(payload)
            for lead, manager in assignments:
                record_transfer(run_id, lead, manager, cfg)
            clear_moved_cache()

            total = len(assignments)
            by_manager: Dict[str, int] = {}
            for _, manager in assignments:
                by_manager[manager["name"]] = by_manager.get(manager["name"], 0) + 1
            lines = [f"{name}: {count}" for name, count in by_manager.items()]
            message = f"Переведено {total} сделок\n" + "\n".join(lines)
            finish_run(run_id, "done", message)
            return run_id, message
        except Exception as exc:
            finish_run(run_id, "error", str(exc))
            raise
    finally:
        RUN_LOCK.release()


def monitoring_text() -> str:
    client = amo()
    cfg = config()
    with db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM transfers").fetchone()[0]
        unique_total = conn.execute("SELECT COUNT(DISTINCT lead_id) FROM transfers").fetchone()[0]
        last_runs = conn.execute(
            "SELECT * FROM runs ORDER BY id DESC LIMIT 5"
        ).fetchall()
        by_manager = conn.execute(
            "SELECT responsible_name, COUNT(*) count FROM transfers GROUP BY responsible_user_id, responsible_name ORDER BY count DESC"
        ).fetchall()
    today_count = transfers_on_local_date()
    daily_capacity = daily_transfer_capacity()

    try:
        source_count = len(client.list_leads_by_status(cfg["source_pipeline_id"], cfg["source_status_id"]))
        dest_count = len(client.list_leads_by_status(cfg["dest_pipeline_id"], cfg["dest_status_id"]))
    except Exception:
        source_count = -1
        dest_count = -1

    manager_lines = "\n".join(
        f"• {esc(row['responsible_name'])}: <b>{row['count']}</b>" for row in by_manager
    ) or "Пока пусто"
    run_lines = []
    for run in last_runs:
        mark = {"done": "✅", "error": "⚠️", "running": "⏳", "empty": "▫️", "blocked": "🛑"}.get(run["status"], "•")
        run_lines.append(f"{mark} #{run['id']} {esc(format_local_timestamp(run['started_at']))} · {esc(run['message'] or run['status'])}")
    if not run_lines:
        run_lines.append("Пока запусков не было")

    source_text = "н/д" if source_count < 0 else str(source_count)
    dest_text = "н/д" if dest_count < 0 else str(dest_count)
    return (
        "📊 <b>Мониторинг</b>\n\n"
        f"Всего переводов ботом: <b>{total}</b>\n"
        f"Уникальных сделок: <b>{unique_total}</b>\n"
        f"За сегодня (GMT+7): <b>{today_count}</b> из <b>{daily_capacity}</b>\n"
        f"В исходном этапе сейчас: <b>{source_text}</b>\n"
        f"В целевом этапе сейчас: <b>{dest_text}</b>\n\n"
        "<b>По менеджерам</b>\n"
        f"{manager_lines}\n\n"
        "<b>Последние запуски</b>\n"
        + "\n".join(run_lines)
    )


def transferred_ids() -> List[int]:
    with db() as conn:
        rows = conn.execute("SELECT DISTINCT lead_id FROM transfers ORDER BY lead_id DESC").fetchall()
        return [int(row["lead_id"]) for row in rows]


def source_history_ids(pipeline_ids: Optional[Iterable[int]] = None) -> List[int]:
    params: Tuple[Any, ...] = ()
    where = ""
    selected = tuple(int(value) for value in (pipeline_ids or []))
    if selected:
        placeholders = ",".join("?" for _ in selected)
        where = f" WHERE source_pipeline_id IN ({placeholders})"
        params = selected
    with db() as conn:
        rows = conn.execute(
            f"SELECT lead_id FROM source_history_leads{where} ORDER BY lead_id DESC",
            params,
        ).fetchall()
        return [int(row["lead_id"]) for row in rows]


def event_status(event: Dict[str, Any], key: str) -> Optional[Dict[str, int]]:
    values = event.get(key) or []
    for value in values:
        status = value.get("lead_status") if isinstance(value, dict) else None
        if status and "pipeline_id" in status and "id" in status:
            return {"pipeline_id": int(status["pipeline_id"]), "status_id": int(status["id"])}
    return None


def remember_source_history_lead(
    lead_id: int,
    source_pipeline_id: int,
    source_status_id: Optional[int],
    discovered_from: str,
    event_at: Optional[int] = None,
) -> None:
    with db() as conn:
        conn.execute(
            """
            INSERT INTO source_history_leads(
                lead_id, source_pipeline_id, source_status_id, discovered_from, first_seen_at, last_event_at
            )
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(lead_id) DO UPDATE SET
                source_pipeline_id = excluded.source_pipeline_id,
                source_status_id = COALESCE(excluded.source_status_id, source_history_leads.source_status_id),
                discovered_from = CASE
                    WHEN instr(source_history_leads.discovered_from, excluded.discovered_from) > 0
                    THEN source_history_leads.discovered_from
                    ELSE source_history_leads.discovered_from || ',' || excluded.discovered_from
                END,
                last_event_at = MAX(COALESCE(source_history_leads.last_event_at, 0), COALESCE(excluded.last_event_at, 0))
            """,
            (lead_id, source_pipeline_id, source_status_id, discovered_from, utc_iso(), event_at),
        )


def seed_source_history_from_transfers() -> None:
    with db() as conn:
        rows = conn.execute(
            """
            SELECT lead_id, from_pipeline_id, from_status_id, MAX(moved_at) moved_at
            FROM transfers
            GROUP BY lead_id, from_pipeline_id, from_status_id
            """
        ).fetchall()
    for row in rows:
        remember_source_history_lead(
            int(row["lead_id"]),
            int(row["from_pipeline_id"]),
            int(row["from_status_id"]),
            "bot",
            None,
        )


def sync_source_history_from_amo(client: AmoClient, pipeline_id: int) -> int:
    pipeline = client.get_pipeline(pipeline_id)
    statuses = pipeline.get("_embedded", {}).get("statuses", [])
    setting_key = f"source_history_last_sync_at_{pipeline_id}"
    last_sync = int(get_setting(setting_key) or "0")
    from_ts = max(0, last_sync - 60) if last_sync else 0
    max_event_at = last_sync
    found = 0

    for status in statuses:
        status_id = int(status["id"])
        page = 1
        while True:
            params: List[Tuple[str, Any]] = [
                ("limit", 250),
                ("page", page),
                ("filter[type]", "lead_status_changed"),
                ("filter[value_before][leads_statuses][0][pipeline_id]", pipeline_id),
                ("filter[value_before][leads_statuses][0][status_id]", status_id),
            ]
            if from_ts:
                params.append(("filter[created_at][from]", from_ts))

            data = client.get("/api/v4/events", params) or {}
            events = data.get("_embedded", {}).get("events", [])
            for event in events:
                before = event_status(event, "value_before")
                if not before or before["pipeline_id"] != pipeline_id:
                    continue
                event_at = int(event.get("created_at") or 0)
                max_event_at = max(max_event_at, event_at)
                remember_source_history_lead(
                    int(event["entity_id"]),
                    pipeline_id,
                    before["status_id"],
                    "amo_events",
                    event_at,
                )
                found += 1

            if not data.get("_links", {}).get("next") or not events:
                break
            page += 1

    if max_event_at > last_sync:
        set_setting(setting_key, max_event_at)
    return found


def pipeline_status_labels(client: AmoClient, groups: Dict[Tuple[int, int], List[Dict[str, Any]]]) -> Dict[Tuple[int, int], str]:
    labels: Dict[Tuple[int, int], str] = {}
    pipeline_cache: Dict[int, Dict[str, Any]] = {}
    for pipeline_id, status_id in groups:
        try:
            if pipeline_id not in pipeline_cache:
                pipeline_cache[pipeline_id] = client.get_pipeline(pipeline_id)
            pipeline = pipeline_cache[pipeline_id]
            pipeline_name = pipeline.get("name", str(pipeline_id))
            status_name = str(status_id)
            for status in pipeline.get("_embedded", {}).get("statuses", []):
                if int(status["id"]) == int(status_id):
                    status_name = status["name"]
                    break
            labels[(pipeline_id, status_id)] = f"{pipeline_name} → {status_name}"
        except Exception:
            labels[(pipeline_id, status_id)] = f"{pipeline_id} → {status_id}"
    return labels


def normalized_stage_name(value: str) -> str:
    return " ".join(value.lower().replace("ё", "е").split())


def moved_group_key(pipeline_name: str, status_name: str, pipeline_id: int, status_id: int) -> Tuple[str, str]:
    normalized_pipeline = normalized_stage_name(pipeline_name)
    normalized_status = normalized_stage_name(status_name)
    if normalized_pipeline == "прогрев базы":
        return "group:warmup", "Прогрев базы"
    if normalized_status in SALES_GROUP_STATUSES:
        return "group:sales", "Отдел продаж"
    return f"actual:{pipeline_id}:{status_id}", f"{pipeline_name} → {status_name}"


def moved_groups(current: List[Dict[str, Any]], client: AmoClient) -> Dict[str, Dict[str, Any]]:
    raw_groups: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
    for lead in current:
        key = (int(lead["pipeline_id"]), int(lead["status_id"]))
        raw_groups.setdefault(key, []).append(lead)

    labels = pipeline_status_labels(client, raw_groups)
    result: Dict[str, Dict[str, Any]] = {}
    for (pipeline_id, status_id), leads in raw_groups.items():
        pipeline_name, status_name = labels[(pipeline_id, status_id)].split(" → ", 1)
        group_key, group_label = moved_group_key(pipeline_name, status_name, pipeline_id, status_id)
        stage_key = f"actual:{pipeline_id}:{status_id}"
        stage_label = f"{pipeline_name} → {status_name}"
        if group_key not in result:
            result[group_key] = {"label": group_label, "leads": [], "stages": {}, "sort": 0}
        result[group_key]["leads"].extend(leads)
        result[group_key]["stages"][stage_key] = {"label": stage_label, "leads": list(leads)}

    for group in result.values():
        group["leads"].sort(key=lambda item: (lead_number(item) or 0, int(item["id"])), reverse=True)
        for stage in group["stages"].values():
            stage["leads"].sort(key=lambda item: (lead_number(item) or 0, int(item["id"])), reverse=True)
    return result


def compact_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(lead["id"]),
        "name": lead.get("name") or str(lead["id"]),
        "pipeline_id": int(lead["pipeline_id"]),
        "status_id": int(lead["status_id"]),
        "price": int(lead.get("price") or 0),
        "responsible_user_id": int(lead.get("responsible_user_id") or 0),
        "created_at": int(lead.get("created_at") or 0),
        "updated_at": int(lead.get("updated_at") or 0),
    }


def normalize_phone(value: Any) -> Optional[str]:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 11 and digits[0] in "78":
        digits = digits[-10:]
    if len(digits) != 10:
        return None
    return digits


def contact_phones(contact: Dict[str, Any]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for field in contact.get("custom_fields_values") or []:
        field_code = str(field.get("field_code") or "").upper()
        field_name = normalized_stage_name(str(field.get("field_name") or ""))
        if field_code != "PHONE" and "телефон" not in field_name:
            continue
        for item in field.get("values") or []:
            raw = str(item.get("value") or "").strip()
            normalized = normalize_phone(raw)
            if normalized:
                result[normalized] = raw
    return result


def empty_snapshot() -> Dict[str, Any]:
    return {"ids": [], "current": [], "groups": {}, "stats": {}}


def save_snapshot(source_key: str, payload: Dict[str, Any]) -> None:
    refreshed_at = utc_iso()
    with db() as conn:
        conn.execute(
            """
            INSERT INTO monitor_snapshots(source_key, refreshed_at, payload, error)
            VALUES(?, ?, ?, NULL)
            ON CONFLICT(source_key) DO UPDATE SET
                refreshed_at = excluded.refreshed_at,
                payload = excluded.payload,
                error = NULL
            """,
            (source_key, refreshed_at, json.dumps(payload, ensure_ascii=False)),
        )
    with SNAPSHOT_MEMORY_LOCK:
        SNAPSHOT_MEMORY[source_key] = (payload, refreshed_at, "")


def save_snapshot_error(source_key: str, error: Exception) -> None:
    with db() as conn:
        existing = conn.execute(
            "SELECT source_key FROM monitor_snapshots WHERE source_key = ?", (source_key,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE monitor_snapshots SET error = ? WHERE source_key = ?",
                (str(error), source_key),
            )
            with SNAPSHOT_MEMORY_LOCK:
                cached = SNAPSHOT_MEMORY.get(source_key)
                if cached:
                    SNAPSHOT_MEMORY[source_key] = (cached[0], cached[1], str(error))
        else:
            conn.execute(
                "INSERT INTO monitor_snapshots(source_key, refreshed_at, payload, error) VALUES(?, '', ?, ?)",
                (source_key, json.dumps(empty_snapshot()), str(error)),
            )


def load_snapshot(source_key: str) -> Tuple[Dict[str, Any], str, str]:
    with SNAPSHOT_MEMORY_LOCK:
        cached = SNAPSHOT_MEMORY.get(source_key)
        if cached:
            return cached
    ensure_db_ready()
    with db() as conn:
        row = conn.execute(
            "SELECT refreshed_at, payload, error FROM monitor_snapshots WHERE source_key = ?",
            (source_key,),
        ).fetchone()
    if not row:
        return empty_snapshot(), "", ""
    try:
        payload = json.loads(row["payload"] or "{}")
    except json.JSONDecodeError:
        payload = empty_snapshot()
    result = (payload, row["refreshed_at"] or "", row["error"] or "")
    with SNAPSHOT_MEMORY_LOCK:
        SNAPSHOT_MEMORY[source_key] = result
    return result


def crm_catalog(client: AmoClient) -> Tuple[Dict[int, str], Dict[Tuple[int, int], str]]:
    pipelines: Dict[int, str] = {}
    statuses: Dict[Tuple[int, int], str] = {}
    for pipeline in client.list_pipelines():
        pipeline_id = int(pipeline["id"])
        pipelines[pipeline_id] = pipeline.get("name") or str(pipeline_id)
        for status in pipeline.get("_embedded", {}).get("statuses", []):
            statuses[(pipeline_id, int(status["id"]))] = status.get("name") or str(status["id"])
    return pipelines, statuses


def stage_label(
    pipeline_id: int,
    status_id: int,
    pipelines: Dict[int, str],
    statuses: Dict[Tuple[int, int], str],
) -> str:
    return f"{pipelines.get(pipeline_id, str(pipeline_id))} → {statuses.get((pipeline_id, status_id), str(status_id))}"


def format_epoch(timestamp: int) -> str:
    if not timestamp:
        return "неизвестно"
    return datetime.fromtimestamp(timestamp, timezone.utc).astimezone(local_tz()).strftime("%d.%m.%Y %H:%M")


def format_path_epoch(timestamp: int) -> str:
    if not timestamp:
        return "неизвестно"
    return datetime.fromtimestamp(timestamp, timezone.utc).astimezone(local_tz()).strftime("%d.%m.%Y %H:%M:%S")


def build_deal_detail_payload(
    lead: Dict[str, Any],
    events: List[Dict[str, Any]],
    pipelines: Dict[int, str],
    statuses: Dict[Tuple[int, int], str],
    users: Dict[int, str],
    sources: Optional[List[str]] = None,
) -> Dict[str, Any]:
    lead_id = int(lead["id"])
    relevant = [event for event in events if int(event.get("entity_id") or 0) == lead_id]
    relevant.sort(key=lambda item: (int(item.get("created_at") or 0), str(item.get("id") or "")))
    timeline: List[Dict[str, Any]] = []
    if relevant:
        first_before = event_status(relevant[0], "value_before")
        if first_before:
            timeline.append(
                {
                    "at": int(lead.get("created_at") or relevant[0].get("created_at") or 0),
                    "label": stage_label(
                        first_before["pipeline_id"], first_before["status_id"], pipelines, statuses
                    ),
                }
            )
        for event in relevant:
            after = event_status(event, "value_after")
            if not after:
                continue
            label = stage_label(after["pipeline_id"], after["status_id"], pipelines, statuses)
            if timeline and timeline[-1]["label"] == label:
                continue
            timeline.append({"at": int(event.get("created_at") or 0), "label": label})
    if not timeline:
        timeline.append(
            {
                "at": int(lead.get("created_at") or 0),
                "label": stage_label(
                    int(lead["pipeline_id"]), int(lead["status_id"]), pipelines, statuses
                ),
            }
        )
    failure_reason: Optional[str] = None
    if int(lead["pipeline_id"]) == SALES_PIPELINE_ID and int(lead["status_id"]) == SALES_FAILED_STATUS_ID:
        for field in lead.get("custom_fields_values") or []:
            if int(field.get("field_id") or 0) != FAILED_REASON_FIELD_ID:
                continue
            values = [str(item.get("value") or "").strip() for item in field.get("values") or []]
            failure_reason = ", ".join(value for value in values if value) or "Не заполнена"
            break
        if failure_reason is None:
            failure_reason = "Не заполнена"
    return {
        "id": lead_id,
        "name": lead.get("name") or str(lead_id),
        "price": int(lead.get("price") or 0),
        "responsible": users.get(int(lead.get("responsible_user_id") or 0), "Не назначен"),
        "created_at": int(lead.get("created_at") or 0),
        "updated_at": int(lead.get("updated_at") or 0),
        "current_stage": stage_label(
            int(lead["pipeline_id"]), int(lead["status_id"]), pipelines, statuses
        ),
        "initial_stage": timeline[0]["label"],
        "timeline": timeline,
        "sources": sources or [],
        "failure_reason": failure_reason,
    }


def save_deal_details(details: Iterable[Dict[str, Any]]) -> None:
    prepared = list(details)
    rows = [(int(detail["id"]), utc_iso(), json.dumps(detail, ensure_ascii=False)) for detail in prepared]
    if not rows:
        return
    with db() as conn:
        conn.executemany(
            """
            INSERT INTO deal_details(lead_id, refreshed_at, payload)
            VALUES(?, ?, ?)
            ON CONFLICT(lead_id) DO UPDATE SET
                refreshed_at = excluded.refreshed_at,
                payload = excluded.payload
            """,
            rows,
        )
    with DEAL_DETAIL_MEMORY_LOCK:
        for detail in prepared:
            DEAL_DETAIL_MEMORY[int(detail["id"])] = detail


def load_deal_detail(lead_id: int) -> Optional[Dict[str, Any]]:
    with DEAL_DETAIL_MEMORY_LOCK:
        cached = DEAL_DETAIL_MEMORY.get(lead_id)
        if cached:
            return cached
    ensure_db_ready()
    with db() as conn:
        row = conn.execute("SELECT payload FROM deal_details WHERE lead_id = ?", (lead_id,)).fetchone()
    if not row:
        return None
    try:
        detail = json.loads(row["payload"])
        with DEAL_DETAIL_MEMORY_LOCK:
            DEAL_DETAIL_MEMORY[lead_id] = detail
        return detail
    except json.JSONDecodeError:
        return None


def refresh_monitor_deal_details(client: AmoClient, snapshots: Dict[str, Dict[str, Any]]) -> None:
    sources_by_lead: Dict[int, List[str]] = {}
    for source_key, payload in snapshots.items():
        for lead in payload.get("current") or []:
            lead_id = int(lead["id"])
            sources_by_lead.setdefault(lead_id, []).append(SOURCE_LABELS[source_key])
    if not sources_by_lead:
        return
    leads = {
        int(lead["id"]): lead
        for lead in client.get_leads_by_ids(list(sources_by_lead))
    }
    pipelines, statuses = crm_catalog(client)
    users = {int(user["id"]): user.get("name") or str(user["id"]) for user in client.list_users()}
    events = client.lead_status_events(leads)
    save_deal_details(
        build_deal_detail_payload(
            lead, events, pipelines, statuses, users, sources_by_lead.get(lead_id)
        )
        for lead_id, lead in leads.items()
    )


def monitored_source_labels(lead_id: int) -> List[str]:
    labels: List[str] = []
    for source_key in SOURCE_LABELS:
        payload, _, _ = load_snapshot(source_key)
        if any(int(lead["id"]) == lead_id for lead in payload.get("current") or []):
            labels.append(SOURCE_LABELS[source_key])
    return labels


def fetch_and_cache_deal_detail(lead_id: int) -> Dict[str, Any]:
    client = amo()
    lead = client.get_lead(lead_id)
    pipelines, statuses = crm_catalog(client)
    users = {int(user["id"]): user.get("name") or str(user["id"]) for user in client.list_users()}
    events = client.lead_status_events([lead_id])
    detail = build_deal_detail_payload(
        lead, events, pipelines, statuses, users, monitored_source_labels(lead_id)
    )
    save_deal_details([detail])
    return detail


def refresh_referral_snapshot(client: AmoClient) -> Dict[str, Any]:
    for pipeline_id in REFERRAL_PIPELINE_IDS:
        sync_source_history_from_amo(client, pipeline_id)
        for lead in client.list_pipeline_leads(pipeline_id):
            remember_source_history_lead(
                int(lead["id"]), pipeline_id, int(lead["status_id"]), "current_pipeline"
            )
    ids = source_history_ids(REFERRAL_PIPELINE_IDS)
    current = [
        compact_lead(lead)
        for lead in client.get_leads_by_ids(ids)
        if int(lead["pipeline_id"]) not in REFERRAL_PIPELINE_IDS
    ] if ids else []
    return {
        "ids": ids,
        "current": current,
        "groups": moved_groups(current, client),
        "stats": {"source_pipelines": len(REFERRAL_PIPELINE_IDS)},
    }


def refresh_lawyer_snapshot(client: AmoClient) -> Dict[str, Any]:
    for pipeline_id in LAWYER_PIPELINE_IDS:
        sync_source_history_from_amo(client, pipeline_id)
        for lead in client.list_pipeline_leads(pipeline_id):
            remember_source_history_lead(
                int(lead["id"]), pipeline_id, int(lead["status_id"]), "current_pipeline"
            )
    ids = source_history_ids(LAWYER_PIPELINE_IDS)
    current = [
        compact_lead(lead)
        for lead in client.get_leads_by_ids(ids)
        if int(lead["pipeline_id"]) not in LAWYER_PIPELINE_IDS
    ] if ids else []
    return {
        "ids": ids,
        "current": current,
        "groups": moved_groups(current, client),
        "stats": {"source_pipelines": len(LAWYER_PIPELINE_IDS)},
    }


def refresh_court_snapshot(client: AmoClient) -> Dict[str, Any]:
    judicial_leads = client.list_pipeline_leads(JUDICIAL_PIPELINE_ID, with_contacts=True)
    judicial_ids = {int(lead["id"]) for lead in judicial_leads}
    contact_ids = {
        int(contact["id"])
        for lead in judicial_leads
        for contact in lead.get("_embedded", {}).get("contacts", [])
    }
    phones: Dict[str, str] = {}
    for contact in client.get_contacts_by_ids(contact_ids):
        phones.update(contact_phones(contact))

    matching_lead_ids: set[int] = set()
    for normalized in phones:
        for contact in client.find_contacts(normalized):
            if normalized not in contact_phones(contact):
                continue
            matching_lead_ids.update(
                int(lead["id"])
                for lead in contact.get("_embedded", {}).get("leads", [])
            )
    matching_lead_ids.difference_update(judicial_ids)
    current = [
        compact_lead(lead)
        for lead in client.get_leads_by_ids(sorted(matching_lead_ids))
        if int(lead["pipeline_id"]) != JUDICIAL_PIPELINE_ID
    ]
    ids = sorted((int(lead["id"]) for lead in current), reverse=True)
    return {
        "ids": ids,
        "current": current,
        "groups": moved_groups(current, client),
        "stats": {
            "judicial_deals": len(judicial_leads),
            "phones": len(phones),
        },
    }


def refresh_all_snapshots() -> None:
    if not SNAPSHOT_REFRESH_LOCK.acquire(blocking=False):
        return
    try:
        client = amo()
        refreshers = {
            "court": refresh_court_snapshot,
            "agents": refresh_referral_snapshot,
            "lawyers": refresh_lawyer_snapshot,
        }
        refreshed_snapshots: Dict[str, Dict[str, Any]] = {}
        for source_key, refresher in refreshers.items():
            try:
                payload = refresher(client)
                save_snapshot(source_key, payload)
                refreshed_snapshots[source_key] = payload
            except Exception as exc:
                save_snapshot_error(source_key, exc)
                print(f"Snapshot refresh error ({source_key}): {exc}")
        try:
            refresh_monitor_deal_details(client, refreshed_snapshots)
        except Exception as exc:
            print(f"Deal details refresh error: {exc}")
    finally:
        SNAPSHOT_REFRESH_LOCK.release()


def snapshot_status_line(refreshed_at: str, error: str) -> str:
    if not refreshed_at:
        return "⏳ Первый снимок данных ещё формируется."
    line = f"Обновлено: <b>{esc(format_local_timestamp(refreshed_at))}</b>"
    if error:
        line += "\n⚠️ Последнее обновление не удалось; показан предыдущий снимок."
    return line


def source_overview(source_key: str) -> Tuple[str, List[List[Dict[str, str]]]]:
    payload, refreshed_at, error = load_snapshot(source_key)
    label = SOURCE_LABELS.get(source_key, "Мониторинг клиентов")
    groups = payload.get("groups") or {}
    rows: List[List[Tuple[str, str]]] = []
    for group_key, group in sorted(groups.items(), key=lambda item: len(item[1]["leads"]), reverse=True):
        if group_key.startswith("group:"):
            callback = f"srcg|{source_key}|{group_key}"
        else:
            callback = f"srcs|{source_key}|{group_key}|0"
        rows.append([(f"{group['label']} · {len(group['leads'])}", callback)])
    rows.append([("← Главное меню", "menu")])

    if groups:
        description = "Выбери этап, чтобы увидеть сделки со ссылками."
    else:
        description = "В последнем снимке подходящих сделок нет."
    text = (
        f"🗂 <b>{esc(label)}</b>\n\n"
        f"Сделок: <b>{len(payload.get('current') or [])}</b>\n"
        f"{snapshot_status_line(refreshed_at, error)}\n\n"
        f"{description}"
    )
    return text, keyboard(rows)


def source_group_stages_text(source_key: str, group_key: str) -> Tuple[str, List[List[Dict[str, str]]]]:
    payload, _, _ = load_snapshot(source_key)
    group = (payload.get("groups") or {}).get(group_key)
    if not group:
        return "Раздел не найден в текущем снимке.", keyboard([[("← К этапам", f"source:{source_key}")]])
    rows: List[List[Tuple[str, str]]] = []
    for stage_key, stage in sorted(group["stages"].items(), key=lambda item: len(item[1]["leads"]), reverse=True):
        rows.append([(f"{stage['label']} · {len(stage['leads'])}", f"srcs|{source_key}|{stage_key}|0")])
    rows.append([("← К этапам", f"source:{source_key}")])
    text = (
        f"🗂 <b>{esc(group['label'])}</b>\n\n"
        f"Всего сделок: <b>{len(group['leads'])}</b>\n\n"
        "Выбери этап, чтобы увидеть сделки со ссылками."
    )
    return text, keyboard(rows)


def source_stage_text(source_key: str, stage_key: str, page: int) -> Tuple[str, List[List[Dict[str, str]]]]:
    payload, _, _ = load_snapshot(source_key)
    groups = payload.get("groups") or {}
    group = groups.get(stage_key)
    parent_group_key: Optional[str] = None
    if group:
        leads, title = group["leads"], group["label"]
    else:
        stage = None
        for candidate_key, candidate in groups.items():
            if stage_key in candidate["stages"]:
                stage = candidate["stages"][stage_key]
                parent_group_key = candidate_key
                break
        leads = stage["leads"] if stage else []
        title = stage["label"] if stage else "Сделки"
    per_page = 5
    pages = max(1, (len(leads) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    part = leads[page * per_page : (page + 1) * per_page]
    lines = [f"📍 <b>{esc(title)}</b>", "", f"Сделок: <b>{len(leads)}</b>"]
    if pages > 1:
        lines.extend(["", f"Страница <b>{page + 1}</b> из <b>{pages}</b>"])
    rows: List[List[Dict[str, str]]] = []
    for lead in part:
        label = str(lead.get("name") or lead["id"]).strip()
        if len(label) > 58:
            label = label[:55].rstrip() + "…"
        callback = f"dl|{int(lead['id'])}|{source_key}|{stage_key}|{page}"
        rows.append([{"text": label, "callback_data": callback}])
    if not part:
        lines.extend(["", "На этой странице пусто"])
    nav: List[Tuple[str, str]] = []
    if page > 0:
        nav.append(("← Назад", f"srcs|{source_key}|{stage_key}|{page - 1}"))
    if page < pages - 1:
        nav.append(("Вперёд →", f"srcs|{source_key}|{stage_key}|{page + 1}"))
    if nav:
        rows.append([{"text": text, "callback_data": callback} for text, callback in nav])
    if parent_group_key:
        rows.append([{"text": "← К этапам раздела", "callback_data": f"srcg|{source_key}|{parent_group_key}"}])
    else:
        rows.append([{"text": "← К этапам", "callback_data": f"source:{source_key}"}])
    return "\n".join(lines), rows


def deal_detail_text(detail: Dict[str, Any]) -> str:
    price = f"{int(detail.get('price') or 0):,}".replace(",", " ")
    lines = [
        f"📋 <b>{esc(detail.get('name') or detail['id'])} (<code>{int(detail['id'])}</code>)</b>",
        "",
        f"Текущий этап: <b>{esc(detail.get('current_stage'))}</b>",
        f"Начальный этап: <b>{esc(detail.get('initial_stage'))}</b>",
        f"Ответственный: <b>{esc(detail.get('responsible'))}</b>",
    ]
    if detail.get("current_stage") == "Отдел продаж → Не смог реализовать":
        lines.append(
            f"Причина «Не смог реализовать»: <b>{esc(detail.get('failure_reason') or 'Не заполнена')}</b>"
        )
    lines.extend(
        [
            f"Бюджет: <b>{price} ₽</b>",
            f"Создана: <b>{format_epoch(int(detail.get('created_at') or 0))}</b>",
            f"Обновлена: <b>{format_epoch(int(detail.get('updated_at') or 0))}</b>",
        ]
    )
    if detail.get("sources"):
        lines.append(f"Источник мониторинга: <b>{esc(', '.join(detail['sources']))}</b>")
    lines.extend(["", "<b>Путь сделки:</b>"])
    timeline = detail.get("timeline") or []
    visible = timeline
    omitted = 0
    if len(timeline) > 35:
        visible = timeline[:1] + timeline[-34:]
        omitted = len(timeline) - len(visible)
    for index, item in enumerate(visible):
        if len(visible) == 1 or index == len(visible) - 1:
            branch = "└"
        elif index == 0:
            branch = "┌"
        else:
            branch = "├"
        lines.append(
            f"{branch} {esc(item.get('label'))} — {format_path_epoch(int(item.get('at') or 0))}"
        )
        if index == 0 and omitted:
            lines.append(f"… пропущено промежуточных переходов: {omitted}")
    return "\n".join(lines)


def deal_detail_view(
    detail: Dict[str, Any],
    back_callback: Optional[str] = None,
) -> Tuple[str, List[List[Dict[str, str]]]]:
    base_url = ENV.get("AMOCRM_BASE_URL", "").rstrip("/")
    rows: List[List[Dict[str, str]]] = []
    rows.append(
        [{"text": "Сделка в amoCRM", "url": f"{base_url}/leads/detail/{int(detail['id'])}"}]
    )
    if back_callback:
        rows.append([{"text": "← К списку сделок", "callback_data": back_callback}])
    else:
        rows.append([{"text": "← Главное меню", "callback_data": "menu"}])
    return deal_detail_text(detail), rows


def managers_text() -> Tuple[str, List[List[Dict[str, str]]]]:
    rows: List[List[Tuple[str, str]]] = []
    lines = ["👥 <b>Менеджеры</b>", "", "Нажми на менеджера, чтобы изменить лимит или выключить."]
    for manager in managers():
        mark = "✅" if manager["enabled"] else "⏸"
        label = f"{mark} {manager['name']} · {manager['daily_limit']}"
        rows.append([(label, f"mgr:{manager['user_id']}")])
    rows.append([("➕ Добавить", "mgr:add")])
    rows.append([("← Админ", "admin")])
    return "\n".join(lines), keyboard(rows)


def manager_detail(user_id: int) -> Tuple[str, List[List[Dict[str, str]]]]:
    with db() as conn:
        manager = conn.execute("SELECT * FROM managers WHERE user_id = ?", (user_id,)).fetchone()
    if not manager:
        return "Менеджер не найден", keyboard([[("← Менеджеры", "mgrs")]])
    mark = "активен" if manager["enabled"] else "выключен"
    text = (
        f"👤 <b>{esc(manager['name'])}</b>\n\n"
        f"amoCRM ID: <code>{manager['user_id']}</code>\n"
        f"Лимит в день: <b>{manager['daily_limit']}</b>\n"
        f"Статус: <b>{mark}</b>"
    )
    toggle = "⏸ Выключить" if manager["enabled"] else "✅ Включить"
    return text, keyboard(
        [
            [("Изменить лимит", f"mgr:cnt:{user_id}"), (toggle, f"mgr:toggle:{user_id}")],
            [("Удалить", f"mgr:del:{user_id}")],
            [("← Менеджеры", "mgrs")],
        ]
    )


def pipelines_text(kind: str) -> Tuple[str, List[List[Dict[str, str]]]]:
    client = amo()
    rows: List[List[Tuple[str, str]]] = []
    for pipeline in client.list_pipelines():
        rows.append([(pipeline["name"], f"pl:{kind}:{pipeline['id']}")])
    rows.append([("← Маршрут", "route")])
    label = "источника" if kind == "src" else "назначения"
    return f"🧭 <b>Выбери воронку {label}</b>", keyboard(rows)


def statuses_text(kind: str, pipeline_id: int) -> Tuple[str, List[List[Dict[str, str]]]]:
    client = amo()
    pipeline = client.get_pipeline(pipeline_id)
    rows: List[List[Tuple[str, str]]] = []
    for status in pipeline.get("_embedded", {}).get("statuses", []):
        rows.append([(status["name"], f"st:{kind}:{pipeline_id}:{status['id']}")])
    rows.append([("← Воронки", f"route:{kind}")])
    return f"🧭 <b>{esc(pipeline['name'])}</b>\nВыбери этап.", keyboard(rows)


def set_route(kind: str, pipeline_id: int, status_id: int) -> str:
    if kind == "src":
        set_setting("source_pipeline_id", pipeline_id)
        set_setting("source_status_id", status_id)
        return "Источник обновлён"
    set_setting("dest_pipeline_id", pipeline_id)
    set_setting("dest_status_id", status_id)
    return "Назначение обновлено"


def is_admin(user_id: int) -> bool:
    raw = ENV.get("TELEGRAM_ADMIN_IDS", "").strip()
    if not raw:
        return False
    allowed = {part.strip() for part in raw.split(",") if part.strip()}
    return str(user_id) in allowed


def unauthorized_text(user_id: int) -> str:
    return (
        "Доступ закрыт.\n\n"
        "Добавь свой Telegram ID в <code>TELEGRAM_ADMIN_IDS</code> в .env и перезапусти бота.\n"
        f"Твой ID: <code>{user_id}</code>"
    )


def handle_state(chat_id: int, text: str, tg: TelegramClient, incoming_message_id: Optional[int] = None) -> bool:
    state = get_state(chat_id)
    if not state:
        return False

    state_name = state["state"]
    payload = json.loads(state["payload"] or "{}")
    target_message_id = payload.get("message_id")

    if incoming_message_id:
        tg.delete(chat_id, incoming_message_id)

    def respond(message: str, markup: Optional[List[List[Dict[str, str]]]] = None) -> None:
        show(tg, chat_id, message, markup, target_message_id)

    if text.strip().lower() in {"/cancel", "отмена"}:
        clear_state(chat_id)
        respond("Отменено.", main_menu())
        return True

    if state_name == "set_time":
        if not re.fullmatch(r"\d{2}:\d{2}", text.strip()):
            respond(
                "⏰ <b>Время запуска</b>\n\n"
                "Введи время в формате <code>09:00</code>.\n"
                "Отмена: <code>/cancel</code>",
                keyboard([[("← Админ", "admin")]]),
            )
            return True
        hour, minute = map(int, text.strip().split(":"))
        if hour > 23 or minute > 59:
            respond(
                "⏰ <b>Время запуска</b>\n\n"
                "Такого времени нет. Пример: <code>09:00</code>.\n"
                "Отмена: <code>/cancel</code>",
                keyboard([[("← Админ", "admin")]]),
            )
            return True
        set_setting("schedule_time", f"{hour:02d}:{minute:02d}")
        clear_state(chat_id)
        respond(f"Готово. Новое время: <b>{hour:02d}:{minute:02d}</b> GMT+7.", admin_menu())
        return True

    if state_name == "set_count":
        if not text.strip().isdigit():
            respond(
                "👥 <b>Лимит менеджера</b>\n\n"
                "Введи целое число, например <code>15</code>.\n"
                "Отмена: <code>/cancel</code>",
                keyboard([[("← Менеджеры", "mgrs")]]),
            )
            return True
        limit = int(text.strip())
        with db() as conn:
            conn.execute("UPDATE managers SET daily_limit = ? WHERE user_id = ?", (limit, payload["user_id"]))
        clear_state(chat_id)
        text_out, kb = managers_text()
        respond("Лимит обновлён.\n\n" + text_out, kb)
        return True

    if state_name == "add_manager":
        parts = text.strip().rsplit(" ", 1)
        query = parts[0]
        limit = 15
        if len(parts) == 2 and parts[1].isdigit():
            limit = int(parts[1])
        user = amo().find_user(query)
        if not user:
            respond(
                "➕ <b>Добавить менеджера</b>\n\n"
                "Не нашёл такого пользователя. Пришли amoCRM ID или точное имя.\n"
                "Пример: <code>7074220 15</code>\n"
                "Отмена: <code>/cancel</code>",
                keyboard([[("← Менеджеры", "mgrs")]]),
            )
            return True
        with db() as conn:
            max_sort = conn.execute("SELECT COALESCE(MAX(sort_order), 0) FROM managers").fetchone()[0]
            conn.execute(
                "INSERT INTO managers(user_id, name, daily_limit, enabled, sort_order) VALUES(?, ?, ?, 1, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET name = excluded.name, daily_limit = excluded.daily_limit, enabled = 1",
                (int(user["id"]), user["name"], limit, max_sort + 10),
            )
        clear_state(chat_id)
        text_out, kb = managers_text()
        respond(f"Добавлен: <b>{esc(user['name'])}</b> · лимит {limit}\n\n{text_out}", kb)
        return True

    if state_name == "route_manual":
        if not re.fullmatch(r"\d+\s+\d+\s+(src|dst)", text.strip()):
            respond(
                "🧭 <b>Маршрут вручную</b>\n\n"
                "Формат: <code>pipeline_id status_id src</code> или <code>pipeline_id status_id dst</code>.\n"
                "Отмена: <code>/cancel</code>",
                keyboard([[("← Маршрут", "route")]]),
            )
            return True
        pipeline_id_raw, status_id_raw, kind = text.strip().split()
        message = set_route(kind, int(pipeline_id_raw), int(status_id_raw))
        clear_state(chat_id)
        respond(f"{message}.\n\n{summary_text()}", admin_menu())
        return True

    return False


def extract_lead_id(text: str) -> Optional[int]:
    value = text.strip()
    if re.fullmatch(r"\d{5,}", value):
        return int(value)
    match = re.fullmatch(r"https?://[^\s]+/leads/detail/(\d+)(?:[/?#].*)?", value, re.IGNORECASE)
    return int(match.group(1)) if match else None


def handle_message(update: Dict[str, Any], tg: TelegramClient) -> None:
    message = update.get("message") or {}
    chat_id = int(message.get("chat", {}).get("id"))
    user_id = int(message.get("from", {}).get("id"))
    text = str(message.get("text") or "").strip()

    if not is_admin(user_id):
        send_panel(tg, chat_id, unauthorized_text(user_id))
        return
    clear_state(chat_id)
    lead_id = extract_lead_id(text)
    if lead_id:
        detail = load_deal_detail(lead_id)
        if detail:
            detail_text, markup = deal_detail_view(detail)
            send_panel(tg, chat_id, detail_text, markup)
            return
        pending = send_panel(tg, chat_id, f"⏳ Собираю информацию о сделке <code>{lead_id}</code>…")
        pending_message_id = int(pending.get("result", {}).get("message_id") or 0)
        try:
            detail = fetch_and_cache_deal_detail(lead_id)
            detail_text, markup = deal_detail_view(detail)
            show(tg, chat_id, detail_text, markup, pending_message_id or None)
        except Exception as exc:
            show(
                tg,
                chat_id,
                f"⚠️ Не удалось получить сделку <code>{lead_id}</code>.\n\n{esc(str(exc))}",
                main_menu(),
                pending_message_id or None,
            )
        return
    send_panel(
        tg,
        chat_id,
        "📊 <b>Мониторинг клиентских сделок</b>\n\nВыберите, откуда пришёл клиент.",
        main_menu(),
    )


def handle_callback(update: Dict[str, Any], tg: TelegramClient) -> None:
    callback = update["callback_query"]
    data = callback.get("data", "")
    chat_id = int(callback["message"]["chat"]["id"])
    message_id = int(callback["message"]["message_id"])
    user_id = int(callback["from"]["id"])
    tg.answer_callback(callback["id"])
    remember_panel(chat_id, message_id)

    def render(text: str, markup: Optional[List[List[Dict[str, str]]]] = None) -> None:
        show(tg, chat_id, text, markup, message_id)

    if not is_admin(user_id):
        render(unauthorized_text(user_id))
        return

    if data == "menu":
        render("📊 <b>Мониторинг клиентских сделок</b>\n\nВыберите, откуда пришёл клиент.", main_menu())
    elif data.startswith("source:"):
        source_key = data.split(":", 1)[1]
        text, kb = source_overview(source_key)
        render(text, kb)
    elif data.startswith("srcg|"):
        _, source_key, group_key = data.split("|", 2)
        text, kb = source_group_stages_text(source_key, group_key)
        render(text, kb)
    elif data.startswith("srcs|"):
        _, source_key, stage_key, page = data.split("|", 3)
        text, kb = source_stage_text(source_key, stage_key, int(page))
        render(text, kb)
    elif data.startswith("dl|"):
        _, lead_id, source_key, stage_key, page = data.split("|", 4)
        detail = load_deal_detail(int(lead_id))
        if detail is None:
            detail = fetch_and_cache_deal_detail(int(lead_id))
        back_callback = f"srcs|{source_key}|{stage_key}|{page}"
        text, kb = deal_detail_view(detail, back_callback)
        render(text, kb)
    else:
        render("Не понял действие. Вернёмся в меню.", main_menu())


def snapshot_refresh_loop() -> None:
    while True:
        started_at = time.monotonic()
        try:
            refresh_all_snapshots()
        except Exception:
            traceback.print_exc()
        elapsed = time.monotonic() - started_at
        time.sleep(max(1.0, SNAPSHOT_REFRESH_SECONDS - elapsed))


def poll_loop(tg: TelegramClient) -> None:
    offset: Optional[int] = None
    while True:
        try:
            updates = tg.get_updates(offset)
            for update in updates:
                offset = int(update["update_id"]) + 1
                try:
                    if "message" in update:
                        handle_message(update, tg)
                    elif "callback_query" in update:
                        handle_callback(update, tg)
                except Exception as exc:
                    if isinstance(exc, ApiError):
                        print(f"Update handling API error: {exc}")
                    else:
                        traceback.print_exc()
                    chat = update.get("message", {}).get("chat") or update.get("callback_query", {}).get("message", {}).get("chat")
                    if chat:
                        callback_message = update.get("callback_query", {}).get("message")
                        if callback_message:
                            tg.edit(
                                int(chat["id"]),
                                int(callback_message["message_id"]),
                                "⚠️ Ошибка\n\n" + esc(str(exc)),
                                main_menu(),
                            )
                        else:
                            tg.send(int(chat["id"]), "⚠️ Ошибка\n\n" + esc(str(exc)), main_menu())
        except ApiError as exc:
            print(f"Polling API error: {exc}")
            time.sleep(5)
        except Exception:
            traceback.print_exc()
            time.sleep(5)


def main() -> None:
    init_db()
    token = ENV.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        print("TELEGRAM_BOT_TOKEN is missing. Add it to .env and restart.")
        return
    if not ENV.get("TELEGRAM_ADMIN_IDS", "").strip():
        print("TELEGRAM_ADMIN_IDS is missing. Add your Telegram numeric ID to .env.")
    tg = TelegramClient(token)
    threading.Thread(target=snapshot_refresh_loop, daemon=True).start()
    print("Bot started. Press Ctrl+C to stop.")
    poll_loop(tg)


if __name__ == "__main__":
    main()
