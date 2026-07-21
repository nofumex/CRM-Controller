import argparse
import csv
import json
import os
import re
import sqlite3
import ssl
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import bot
from groq import Groq

OUT_DIR = ROOT / "reports"
CACHE_DIR = ROOT / ".analysis_cache"
TRANSCRIPTS_PATH = CACHE_DIR / "groq_transcripts.json"
ANALYSIS_PATH = CACHE_DIR / "groq_analysis.json"

REASON_FIELD_ID = 589709
NOT_REALIZED_STATUS_ID = 143
COLD_NAME_RE = re.compile(r"#\d+\s*$")


def chunks(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def status_maps(client: bot.AmoClient) -> Tuple[Dict[int, str], Dict[Tuple[int, int], str]]:
    pipelines = client.list_pipelines()
    pipeline_names: Dict[int, str] = {}
    status_names: Dict[Tuple[int, int], str] = {}
    for pipeline in pipelines:
        pipeline_id = int(pipeline["id"])
        pipeline_names[pipeline_id] = pipeline["name"]
        full_pipeline = client.get_pipeline(pipeline_id)
        for status in full_pipeline.get("_embedded", {}).get("statuses", []):
            status_names[(pipeline_id, int(status["id"]))] = status["name"]
    return pipeline_names, status_names


def get_leads_with_contacts(client: bot.AmoClient, lead_ids: List[int]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for part in chunks(lead_ids, 50):
        params: List[Tuple[str, Any]] = [("limit", 250), ("with", "contacts")]
        params.extend(("filter[id][]", lead_id) for lead_id in part)
        data = client.get("/api/v4/leads", params) or {}
        result.extend(data.get("_embedded", {}).get("leads", []))
    return result


def is_cold_lead(lead: Dict[str, Any]) -> bool:
    name = lead.get("name") or ""
    return bool(COLD_NAME_RE.search(name)) and (name.startswith("Сделка ") or "32350089" in name)


def custom_field_value(lead: Dict[str, Any], field_id: int) -> str:
    for field in lead.get("custom_fields_values") or []:
        if int(field.get("field_id") or 0) == field_id:
            values = field.get("values") or []
            return "; ".join(str(value.get("value", "")) for value in values if value.get("value") is not None)
    return ""


def all_contact_notes(client: bot.AmoClient, contact_id: int) -> List[Dict[str, Any]]:
    notes: List[Dict[str, Any]] = []
    page = 1
    while True:
        data = client.get(f"/api/v4/contacts/{contact_id}/notes", [("limit", 250), ("page", page)]) or {}
        batch = data.get("_embedded", {}).get("notes", [])
        notes.extend(batch)
        if not data.get("_links", {}).get("next") or not batch:
            return notes
        page += 1


def call_notes_for_lead(client: bot.AmoClient, lead: Dict[str, Any]) -> List[Dict[str, Any]]:
    calls: List[Dict[str, Any]] = []
    contacts = lead.get("_embedded", {}).get("contacts") or []
    for contact in contacts:
        contact_id = int(contact["id"])
        for note in all_contact_notes(client, contact_id):
            if note.get("note_type") not in {"call_in", "call_out"}:
                continue
            params = note.get("params") or {}
            duration = int(params.get("duration") or 0)
            calls.append(
                {
                    "note_id": note["id"],
                    "contact_id": contact_id,
                    "type": note.get("note_type"),
                    "duration": duration,
                    "link": params.get("link") or "",
                    "phone": str(params.get("phone") or ""),
                    "created_at": note.get("created_at"),
                    "responsible_user_id": note.get("responsible_user_id"),
                }
            )
    calls.sort(key=lambda item: (item["duration"], item.get("created_at") or 0), reverse=True)
    return calls


def groq_transcribe(api_key: str, mp3_url: str, note_id: int) -> str:
    with tempfile.NamedTemporaryFile(suffix=f"_{note_id}.mp3", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        mp3_request = Request(mp3_url, headers={"User-Agent": "Mozilla/5.0"})
        ssl_context = ssl._create_unverified_context()
        with urlopen(mp3_request, timeout=120, context=ssl_context) as response:
            tmp.write(response.read())
    try:
        groq_client = Groq(api_key=api_key)
        with tmp_path.open("rb") as audio_file:
            transcription = groq_client.audio.transcriptions.create(
                file=(tmp_path.name, audio_file.read()),
                model="whisper-large-v3",
                language="ru",
            )
        return transcription.text or ""
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass


def groq_analyze(api_key: str, lead: Dict[str, Any], calls: List[Dict[str, Any]], transcript_text: str) -> str:
    prompt = (
        "Ты анализируешь холодный звонок юридической компании по банкротству/долгам. "
        "Лид холодный: человек с просрочками по долгам, не оставлял явную заявку на разговор. "
        "Нужно кратко и по делу объяснить, почему менеджер не смог реализовать холодный звонок. "
        "Пиши на русском, 2-4 предложения. Укажи: контактность клиента, качество открытия, выявление боли, "
        "работу с возражениями, следующий шаг. Не выдумывай факты вне транскрипта.\n\n"
        f"Сделка: {lead.get('id')} / {lead.get('name')}\n"
        f"Звонки: {json.dumps([{k:v for k,v in c.items() if k!='link'} for c in calls], ensure_ascii=False)}\n"
        f"Транскрипт:\n{transcript_text[:18000]}"
    )
    groq_client = Groq(api_key=api_key)
    completion = groq_client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=[
            {"role": "system", "content": "Ты строгий, практичный аналитик продаж. Отвечай без воды."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        max_tokens=700,
    )
    return completion.choices[0].message.content.strip()


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "lead_id",
        "lead_name",
        "lead_url",
        "pipeline",
        "status",
        "responsible_user_id",
        "reason_not_realized",
        "calls_total",
        "calls_over_15s",
        "call_durations_over_15s",
        "transcribed_call_note_ids",
        "ai_analysis",
        "transcript_excerpt",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="Limit cold leads for test run")
    parser.add_argument("--skip-groq", action="store_true")
    args = parser.parse_args()

    bot.init_db()
    client = bot.amo()
    try:
        bot.sync_source_history_from_amo(force=True)
    except Exception as exc:
        cached_ids = bot.source_history_ids()
        if not cached_ids:
            raise
        print(f"WARNING: source history sync failed, using {len(cached_ids)} cached lead ids: {exc}")
    pipeline_names, status_names = status_maps(client)

    lead_ids = bot.source_history_ids()
    leads = get_leads_with_contacts(client, lead_ids)
    cold_leads = [lead for lead in leads if is_cold_lead(lead)]
    cold_leads.sort(key=lambda item: int(item["id"]))
    if args.limit:
        cold_leads = cold_leads[: args.limit]

    transcripts = load_json(TRANSCRIPTS_PATH)
    analyses = load_json(ANALYSIS_PATH)
    groq_key = bot.ENV.get("GROQ_API_KEY", "")
    if not args.skip_groq and not groq_key:
        raise RuntimeError("GROQ_API_KEY is missing in .env")

    rows: List[Dict[str, Any]] = []
    for index, lead in enumerate(cold_leads, start=1):
        print(f"[{index}/{len(cold_leads)}] {lead['id']} {lead.get('name')}")
        calls = call_notes_for_lead(client, lead)
        long_calls = [call for call in calls if call["duration"] > 15 and call["link"]]
        transcribed_parts: List[str] = []
        for call in long_calls:
            cache_key = str(call["note_id"])
            previous_transcript = transcripts.get(cache_key, "")
            needs_transcript = (
                cache_key not in transcripts
                or (not args.skip_groq and str(previous_transcript).startswith("[TRANSCRIPTION_ERROR]"))
            )
            if needs_transcript and not args.skip_groq:
                try:
                    transcripts[cache_key] = groq_transcribe(groq_key, call["link"], int(call["note_id"]))
                    save_json(TRANSCRIPTS_PATH, transcripts)
                    time.sleep(0.4)
                except Exception as exc:
                    transcripts[cache_key] = f"[TRANSCRIPTION_ERROR] {type(exc).__name__}: {exc}"
                    save_json(TRANSCRIPTS_PATH, transcripts)
            if cache_key in transcripts and not str(transcripts[cache_key]).startswith("[TRANSCRIPTION_ERROR]"):
                transcribed_parts.append(f"Звонок {call['note_id']} ({call['duration']} сек): {transcripts[cache_key]}")

        transcript_text = "\n\n".join(transcribed_parts)
        lead_analysis_key = str(lead["id"])
        existing_analysis = analyses.get(lead_analysis_key, "")
        needs_analysis = (
            lead_analysis_key not in analyses
            or (long_calls and not args.skip_groq and existing_analysis in {"Транскрипт не получен.", ""})
            or (long_calls and not args.skip_groq and str(existing_analysis).startswith("[ANALYSIS_ERROR]"))
        )
        if needs_analysis:
            if long_calls and transcript_text and not args.skip_groq:
                try:
                    analyses[lead_analysis_key] = groq_analyze(groq_key, lead, long_calls, transcript_text)
                    save_json(ANALYSIS_PATH, analyses)
                    time.sleep(0.4)
                except Exception as exc:
                    analyses[lead_analysis_key] = f"[ANALYSIS_ERROR] {type(exc).__name__}: {exc}"
                    save_json(ANALYSIS_PATH, analyses)
            elif not long_calls:
                analyses[lead_analysis_key] = "Нет звонка дольше 15 секунд: по записи нельзя оценить качество коммуникации."
            else:
                analyses[lead_analysis_key] = "Транскрипт не получен."

        pipeline = pipeline_names.get(int(lead["pipeline_id"]), str(lead["pipeline_id"]))
        status = status_names.get((int(lead["pipeline_id"]), int(lead["status_id"])), str(lead["status_id"]))
        reason = ""
        if int(lead["status_id"]) == NOT_REALIZED_STATUS_ID:
            reason = custom_field_value(lead, REASON_FIELD_ID)
        rows.append(
            {
                "lead_id": lead["id"],
                "lead_name": lead.get("name", ""),
                "lead_url": client.lead_url(int(lead["id"])),
                "pipeline": pipeline,
                "status": status,
                "responsible_user_id": lead.get("responsible_user_id", ""),
                "reason_not_realized": reason,
                "calls_total": len(calls),
                "calls_over_15s": len(long_calls),
                "call_durations_over_15s": ", ".join(str(call["duration"]) for call in long_calls),
                "transcribed_call_note_ids": ", ".join(str(call["note_id"]) for call in long_calls if str(call["note_id"]) in transcripts),
                "ai_analysis": analyses.get(lead_analysis_key, ""),
                "transcript_excerpt": transcript_text[:1200],
            }
        )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"cold_leads_analysis_{stamp}.csv"
    write_csv(out_path, rows)
    summary = {
        "output": str(out_path),
        "cold_leads": len(cold_leads),
        "with_any_calls": sum(1 for row in rows if row["calls_total"]),
        "with_calls_over_15s": sum(1 for row in rows if row["calls_over_15s"]),
        "not_realized": sum(1 for row in rows if row["status"] == "Не смог реализовать"),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
