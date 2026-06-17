import html
import json
import os
import re
import sqlite3
import threading
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"
DB_PATH = ROOT / "bot_data.sqlite3"
KRASNOYARSK_TZ = "Asia/Krasnoyarsk"
DB_INIT_LOCK = threading.RLock()

SOURCE_PIPELINE_DEFAULT = 10915210
SOURCE_STATUS_DEFAULT = 85847182
DEST_PIPELINE_DEFAULT = 867829
DEST_STATUS_DEFAULT = 86529234

DEFAULT_MANAGERS = [
    (3298921, "Ольга Шевелева", 15, 1, 10),
    (7074220, "Дегтярева Юлия", 15, 1, 20),
    (2328073, "Павел", 15, 1, 30),
]

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


def now_local() -> datetime:
    if ZoneInfo:
        return datetime.now(ZoneInfo(KRASNOYARSK_TZ))
    return datetime.utcnow()


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
    timeout: int = 40,
    retries: int = 2,
) -> Any:
    body = None
    request_headers = dict(headers or {})
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    request_headers.setdefault("Accept", "application/json")

    for attempt in range(retries + 1):
        req = Request(url, data=body, headers=request_headers, method=method)
        try:
            with urlopen(req, timeout=timeout) as response:
                text = response.read().decode("utf-8")
                return json.loads(text) if text else None
        except HTTPError as exc:
            text = exc.read().decode("utf-8", errors="replace")
            if exc.code == 429 and attempt < retries:
                retry_after = int(exc.headers.get("Retry-After", "2"))
                time.sleep(max(retry_after, 2))
                continue
            raise ApiError(f"{method} {url} -> {exc.code}: {text}") from exc
        except URLError as exc:
            if attempt < retries:
                time.sleep(2)
                continue
            raise ApiError(f"{method} {url} -> {exc}") from exc


class AmoClient:
    def __init__(self, base_url: str, token: str):
        if not base_url or not token:
            raise ApiError("AMOCRM_BASE_URL and AMOCRM_ACCESS_TOKEN are required")
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}

    def _url(self, path: str, params: Optional[List[Tuple[str, Any]]] = None) -> str:
        if params:
            return f"{self.base_url}{path}?{urlencode(params)}"
        return f"{self.base_url}{path}"

    def get(self, path: str, params: Optional[List[Tuple[str, Any]]] = None) -> Any:
        return http_json("GET", self._url(path, params), self.headers)

    def patch(self, path: str, payload: Any) -> Any:
        return http_json("PATCH", self._url(path), self.headers, payload)

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
            data = self.get("/api/v4/leads", params)
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
            data = self.get("/api/v4/leads", params)
            found.extend(data.get("_embedded", {}).get("leads", []))
        return found

    def move_leads(self, payload: List[Dict[str, Any]]) -> None:
        for part in chunks(payload, 50):
            self.patch("/api/v4/leads", part)
            time.sleep(0.25)


class TelegramClient:
    def __init__(self, token: str):
        if not token:
            raise ApiError("TELEGRAM_BOT_TOKEN is required")
        self.base_url = f"https://api.telegram.org/bot{token}"

    def api(self, method: str, payload: Dict[str, Any]) -> Any:
        return http_json("POST", f"{self.base_url}/{method}", payload=payload)

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
            self.api("editMessageText", payload)
        except ApiError as exc:
            if "message is not modified" not in str(exc):
                raise

    def delete(self, chat_id: int, message_id: int) -> None:
        try:
            self.api("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
        except ApiError:
            pass

    def answer_callback(self, callback_id: str, text: str = "") -> None:
        self.api("answerCallbackQuery", {"callback_query_id": callback_id, "text": text})


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
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'settings'"
            ).fetchone()
        finally:
            conn.close()
        if not row:
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
            """
            )
            defaults = {
                "source_pipeline_id": str(SOURCE_PIPELINE_DEFAULT),
                "source_status_id": str(SOURCE_STATUS_DEFAULT),
                "dest_pipeline_id": str(DEST_PIPELINE_DEFAULT),
                "dest_status_id": str(DEST_STATUS_DEFAULT),
                "schedule_time": "09:00",
                "timezone": KRASNOYARSK_TZ,
                "last_auto_run_date": "",
                "source_history_last_sync_at": "0",
                "source_history_last_checked_at": "0",
            }
            for key, value in defaults.items():
                conn.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (key, value))
            existing = conn.execute("SELECT COUNT(*) FROM managers").fetchone()[0]
            if existing == 0:
                conn.executemany(
                    "INSERT INTO managers(user_id, name, daily_limit, enabled, sort_order) VALUES(?, ?, ?, ?, ?)",
                    DEFAULT_MANAGERS,
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


def main_menu() -> List[List[Dict[str, str]]]:
    return keyboard(
        [
            [("📊 Мониторинг", "mon"), ("🗂 Сделки из TG/Max", "moved")],
            [("⚙️ Админ-панель", "admin")],
            [("▶️ Перевести сейчас", "run_now")],
        ]
    )


def admin_menu() -> List[List[Dict[str, str]]]:
    return keyboard(
        [
            [("👥 Менеджеры", "mgrs"), ("🧭 Маршрут", "route")],
            [("⏰ Время", "sched"), ("📋 Сводка", "summary")],
            [("← Главное меню", "menu")],
        ]
    )


def route_menu() -> List[List[Dict[str, str]]]:
    return keyboard(
        [
            [("Откуда", "route:src"), ("Куда", "route:dst")],
            [("Ввести ID вручную", "route:manual")],
            [("← Админ", "admin")],
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
        f"<b>Время</b>\n{esc(get_setting('schedule_time'))} по Красноярску\n\n"
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


def transfers_in_last_24h() -> int:
    cutoff = (datetime.utcnow() - timedelta(hours=24)).replace(microsecond=0).isoformat() + "Z"
    with db() as conn:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM transfers WHERE moved_at >= ?",
                (cutoff,),
            ).fetchone()[0]
        )


def perform_transfer(trigger: str = "manual") -> Tuple[int, str]:
    if not RUN_LOCK.acquire(blocking=False):
        raise RuntimeError("Перевод уже выполняется")
    try:
        cfg = config()
        active_managers = managers(enabled_only=True)
        if not active_managers:
            raise RuntimeError("Нет активных менеджеров")

        client = amo()
        run_id = start_run(trigger, cfg)
        try:
            capacity = daily_transfer_capacity(active_managers)
            recent_count = transfers_in_last_24h()
            if recent_count > 0:
                message = (
                    "За последние 24 часа перевод уже выполнялся. "
                    f"Уже переведено: {recent_count}. Лимит на одну итерацию: {capacity}. "
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
    last_24h = transfers_in_last_24h()
    capacity_24h = daily_transfer_capacity()

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
        run_lines.append(f"{mark} #{run['id']} {esc(run['started_at'])} · {esc(run['message'] or run['status'])}")
    if not run_lines:
        run_lines.append("Пока запусков не было")

    source_text = "н/д" if source_count < 0 else str(source_count)
    dest_text = "н/д" if dest_count < 0 else str(dest_count)
    return (
        "📊 <b>Мониторинг</b>\n\n"
        f"Всего переводов ботом: <b>{total}</b>\n"
        f"Уникальных сделок: <b>{unique_total}</b>\n"
        f"За последние 24 часа: <b>{last_24h}</b> из <b>{capacity_24h}</b>\n"
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


def source_history_ids() -> List[int]:
    with db() as conn:
        rows = conn.execute("SELECT lead_id FROM source_history_leads ORDER BY lead_id DESC").fetchall()
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


def sync_source_history_from_amo(force: bool = False) -> int:
    cfg = config()
    client = amo()
    checked_at = int(get_setting("source_history_last_checked_at") or "0")
    now_ts = int(time.time())
    if not force and checked_at and now_ts - checked_at < 300:
        return 0

    pipeline = client.get_pipeline(cfg["source_pipeline_id"])
    statuses = pipeline.get("_embedded", {}).get("statuses", [])
    last_sync = int(get_setting("source_history_last_sync_at") or "0")
    from_ts = max(0, last_sync - 60) if last_sync else 0
    max_event_at = last_sync
    found = 0

    seed_source_history_from_transfers()

    for status in statuses:
        status_id = int(status["id"])
        page = 1
        while True:
            params: List[Tuple[str, Any]] = [
                ("limit", 250),
                ("page", page),
                ("filter[type]", "lead_status_changed"),
                ("filter[value_before][leads_statuses][0][pipeline_id]", cfg["source_pipeline_id"]),
                ("filter[value_before][leads_statuses][0][status_id]", status_id),
            ]
            if from_ts:
                params.append(("filter[created_at][from]", from_ts))

            data = client.get("/api/v4/events", params) or {}
            events = data.get("_embedded", {}).get("events", [])
            for event in events:
                before = event_status(event, "value_before")
                if not before or before["pipeline_id"] != cfg["source_pipeline_id"]:
                    continue
                event_at = int(event.get("created_at") or 0)
                max_event_at = max(max_event_at, event_at)
                remember_source_history_lead(
                    int(event["entity_id"]),
                    cfg["source_pipeline_id"],
                    before["status_id"],
                    "amo_events",
                    event_at,
                )
                found += 1

            if not data.get("_links", {}).get("next") or not events:
                break
            page += 1

    if max_event_at > last_sync:
        set_setting("source_history_last_sync_at", max_event_at)
    set_setting("source_history_last_checked_at", now_ts)
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
        if group_key not in result:
            result[group_key] = {"label": group_label, "leads": [], "sort": 0}
        result[group_key]["leads"].extend(leads)

    for group in result.values():
        group["leads"].sort(key=lambda item: (lead_number(item) or 0, int(item["id"])), reverse=True)
    return result


def moved_overview(force_sync: bool = False) -> Tuple[str, List[List[Dict[str, str]]]]:
    sync_source_history_from_amo(force=force_sync)
    ids = source_history_ids()
    if not ids:
        return (
            "🗂 <b>Сделки из TG/Max</b>\n\n"
            "Пока не нашёл сделок, которые уходили из исходной воронки.",
            keyboard([[("← Главное меню", "menu")]]),
        )
    client = amo()
    current = client.get_leads_by_ids(ids)
    groups = moved_groups(current, client)
    rows: List[List[Tuple[str, str]]] = []
    text_lines = [
        "🗂 <b>Сделки, которые когда-либо уходили из исходной воронки</b>",
        "",
        f"Всего в истории: <b>{len(ids)}</b>",
        f"Найдено сейчас в amoCRM: <b>{len(current)}</b>",
        "",
        "Выбери этап, чтобы увидеть сделки со ссылками.",
    ]
    for group_key, group in sorted(groups.items(), key=lambda item: len(item[1]["leads"]), reverse=True):
        rows.append([(f"{group['label']} · {len(group['leads'])}", f"mvg:{group_key}:0")])
    rows.append([("Обновить", "moved_refresh")])
    rows.append([("← Главное меню", "menu")])
    return "\n".join(text_lines), keyboard(rows)


def moved_stage_text(group_key: str, page: int) -> Tuple[str, List[List[Dict[str, str]]]]:
    ids = source_history_ids()
    client = amo()
    groups = moved_groups(client.get_leads_by_ids(ids), client)
    group = groups.get(group_key, {"label": "Сделки", "leads": []})
    leads = group["leads"]
    title = group["label"]
    per_page = 10
    pages = max(1, (len(leads) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    part = leads[page * per_page : (page + 1) * per_page]
    lines = [f"📍 <b>{esc(title)}</b>", "", f"Сделок: <b>{len(leads)}</b>", ""]
    for lead in part:
        num = lead_number(lead)
        name = f"#{num}" if num else lead.get("name", lead["id"])
        lines.append(f"• <a href=\"{esc(client.lead_url(int(lead['id'])))}\">{esc(name)}</a> · {esc(lead.get('name'))}")
    if not part:
        lines.append("На этой странице пусто")

    nav: List[Tuple[str, str]] = []
    if page > 0:
        nav.append(("← Назад", f"mvg:{group_key}:{page - 1}"))
    if page < pages - 1:
        nav.append(("Вперёд →", f"mvg:{group_key}:{page + 1}"))
    rows = []
    if nav:
        rows.append(nav)
    rows.append([("← К этапам", "moved")])
    return "\n".join(lines), keyboard(rows)


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
        respond(f"Готово. Новое время: <b>{hour:02d}:{minute:02d}</b> по Красноярску.", admin_menu())
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


def handle_message(update: Dict[str, Any], tg: TelegramClient) -> None:
    message = update.get("message") or {}
    chat_id = int(message.get("chat", {}).get("id"))
    user_id = int(message.get("from", {}).get("id"))
    message_id = int(message.get("message_id", 0))
    text = message.get("text", "").strip()

    if not is_admin(user_id):
        tg.send(chat_id, unauthorized_text(user_id))
        return

    if handle_state(chat_id, text, tg, message_id):
        return

    if text.startswith("/start") or text.startswith("/menu"):
        tg.send(chat_id, "Добро пожаловать. Здесь всё самое нужное для аккуратной раздачи сделок.", main_menu())
    elif text.startswith("/run"):
        run_id, message_out = perform_transfer("manual")
        tg.send(chat_id, f"✅ Запуск #{run_id}\n\n{esc(message_out)}", main_menu())
    else:
        tg.send(chat_id, "Выбери действие.", main_menu())


def handle_callback(update: Dict[str, Any], tg: TelegramClient) -> None:
    callback = update["callback_query"]
    data = callback.get("data", "")
    chat_id = int(callback["message"]["chat"]["id"])
    message_id = int(callback["message"]["message_id"])
    user_id = int(callback["from"]["id"])
    tg.answer_callback(callback["id"])

    def render(text: str, markup: Optional[List[List[Dict[str, str]]]] = None) -> None:
        show(tg, chat_id, text, markup, message_id)

    if not is_admin(user_id):
        render(unauthorized_text(user_id))
        return

    waits_for_text = data == "mgr:add" or data.startswith("mgr:cnt:") or data == "route:manual" or data == "sched"
    if not waits_for_text:
        clear_state(chat_id)

    if data == "menu":
        render("Главное меню.", main_menu())
    elif data == "admin":
        render("⚙️ <b>Админ-панель</b>", admin_menu())
    elif data == "summary":
        render(summary_text(), admin_menu())
    elif data == "mon":
        render(monitoring_text(), keyboard([[("Обновить", "mon")], [("← Главное меню", "menu")]]))
    elif data in {"moved", "moved_refresh"}:
        text, kb = moved_overview(force_sync=data == "moved_refresh")
        render(text, kb)
    elif data.startswith("mvg:"):
        parts = data.split(":")
        page = int(parts[-1])
        group_key = ":".join(parts[1:-1])
        text, kb = moved_stage_text(group_key, page)
        render(text, kb)
    elif data == "run_now":
        render("⏳ Перевожу сделки. Обычно это занимает несколько секунд...", keyboard([[("← Главное меню", "menu")]]))
        run_id, message_out = perform_transfer("manual")
        render(f"✅ Запуск #{run_id}\n\n{esc(message_out)}", main_menu())
    elif data == "mgrs":
        text, kb = managers_text()
        render(text, kb)
    elif data == "mgr:add":
        set_state(chat_id, "add_manager", {"message_id": message_id})
        render(
            "Пришли amoCRM ID или точное имя менеджера и лимит.\n\n"
            "Пример: <code>7074220 15</code>\n"
            "Отмена: <code>/cancel</code>",
            keyboard([[("← Менеджеры", "mgrs")]]),
        )
    elif data.startswith("mgr:cnt:"):
        user_id_to_change = int(data.split(":")[2])
        set_state(chat_id, "set_count", {"user_id": user_id_to_change, "message_id": message_id})
        render(
            "👥 <b>Лимит менеджера</b>\n\n"
            "Введи новый лимит сделок в день. Например: <code>15</code>\n"
            "Отмена: <code>/cancel</code>",
            keyboard([[("← Менеджеры", "mgrs")]]),
        )
    elif data.startswith("mgr:toggle:"):
        user_id_to_toggle = int(data.split(":")[2])
        with db() as conn:
            conn.execute("UPDATE managers SET enabled = CASE enabled WHEN 1 THEN 0 ELSE 1 END WHERE user_id = ?", (user_id_to_toggle,))
        text, kb = manager_detail(user_id_to_toggle)
        render(text, kb)
    elif data.startswith("mgr:del:"):
        user_id_to_delete = int(data.split(":")[2])
        with db() as conn:
            conn.execute("DELETE FROM managers WHERE user_id = ?", (user_id_to_delete,))
        text, kb = managers_text()
        render("Удалено.\n\n" + text, kb)
    elif data.startswith("mgr:"):
        user_id_detail = int(data.split(":")[1])
        text, kb = manager_detail(user_id_detail)
        render(text, kb)
    elif data == "route":
        render("🧭 <b>Маршрут перевода</b>\n\n" + format_route(amo()), route_menu())
    elif data in {"route:src", "route:dst"}:
        kind = data.split(":")[1]
        text, kb = pipelines_text(kind)
        render(text, kb)
    elif data == "route:manual":
        set_state(chat_id, "route_manual", {"message_id": message_id})
        render(
            "Пришли ID в формате:\n"
            "<code>pipeline_id status_id src</code> для источника\n"
            "<code>pipeline_id status_id dst</code> для назначения\n\n"
            "Отмена: <code>/cancel</code>",
            keyboard([[("← Маршрут", "route")]]),
        )
    elif data.startswith("pl:"):
        _, kind, pipeline_id = data.split(":")
        text, kb = statuses_text(kind, int(pipeline_id))
        render(text, kb)
    elif data.startswith("st:"):
        _, kind, pipeline_id, status_id = data.split(":")
        message = set_route(kind, int(pipeline_id), int(status_id))
        render(f"{message}.\n\n{summary_text()}", route_menu())
    elif data == "sched":
        set_state(chat_id, "set_time", {"message_id": message_id})
        render(
            f"Сейчас: <b>{esc(get_setting('schedule_time'))}</b> по Красноярску.\n"
            "Пришли новое время в формате <code>09:00</code>.\n"
            "Отмена: <code>/cancel</code>",
            keyboard([[("← Админ", "admin")]]),
        )
    else:
        render("Не понял действие. Вернёмся в меню.", main_menu())


def notify_admins(tg: TelegramClient, text: str) -> None:
    raw = ENV.get("TELEGRAM_ADMIN_IDS", "").strip()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            tg.send(int(part), text, main_menu())
        except Exception:
            traceback.print_exc()


def scheduler_loop(tg: TelegramClient) -> None:
    while True:
        try:
            scheduled = get_setting("schedule_time") or "09:00"
            current = now_local()
            today = current.strftime("%Y-%m-%d")
            current_hm = current.strftime("%H:%M")
            if current_hm == scheduled and get_setting("last_auto_run_date") != today:
                set_setting("last_auto_run_date", today)
                run_id, message = perform_transfer("auto")
                notify_admins(tg, f"✅ Автозапуск #{run_id}\n\n{esc(message)}")
        except Exception as exc:
            notify_admins(tg, "⚠️ Ошибка автозапуска\n\n" + esc(str(exc)))
        time.sleep(20)


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
    threading.Thread(target=scheduler_loop, args=(tg,), daemon=True).start()
    print("Bot started. Press Ctrl+C to stop.")
    poll_loop(tg)


if __name__ == "__main__":
    main()
