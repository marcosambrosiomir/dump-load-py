import glob
import json
import os
import re
import shlex
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from app.catalog import load_catalog
from app.runner import _build_progress_env, _prepare_progress_command, _prepare_progress_command_for_sudo

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
LOGS_DIR = os.path.join(BASE_DIR, "logs")
RUNTIME_DIR = os.path.join(BASE_DIR, "runtime")
JOBS_DIR = os.path.join(RUNTIME_DIR, "jobs")
INDEX_PATH = os.path.join(RUNTIME_DIR, "index.json")
DEFAULT_TAIL_LINES = 200

_LOCK = threading.RLock()
_ELAPSED_ONLY_LINE = re.compile(r'^\d{2}:\d{2}:\d{2}$')


_SECONDARY_ERROR_PATTERNS = (
    "error",
    "erro",
    "errno",
    "permission denied",
    "nao foi possivel",
    "não foi possível",
    "can\'t attach",
    "cannot attach",
    "cannot open",
    "não foi possível",
    "nao ha servidor carregado",
    "não há servidor carregado",
)


def _utc_now():
    return datetime.now(timezone.utc).isoformat()


def ensure_runtime_dirs():
    os.makedirs(JOBS_DIR, exist_ok=True)
    os.makedirs(LOGS_DIR, exist_ok=True)


def _safe_load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def _safe_write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temporary_path = f"{path}.tmp"
    with open(temporary_path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    os.replace(temporary_path, path)


def _index_data():
    return _safe_load_json(INDEX_PATH, {"active_job_id": None, "last_job_id": None})


def _save_index(data):
    _safe_write_json(INDEX_PATH, data)


def _job_dir(job_id):
    return os.path.join(JOBS_DIR, job_id)


def _state_path(job_id):
    return os.path.join(_job_dir(job_id), "state.json")


def _log_path(job_id):
    return os.path.join(LOGS_DIR, f"{job_id}.log")


def _sanitize_log_token(value):
    token = (value or "").strip().lower()
    for char in ("/", "\\", " ", ":"):
        token = token.replace(char, "_")
    return token or "job"


def _command_log_path(command_name, db_name):
    file_name = f"{_sanitize_log_token(command_name)}_{_sanitize_log_token(db_name)}.log"
    return os.path.join(LOGS_DIR, file_name)


def _read_state(job_id):
    return _safe_load_json(_state_path(job_id), None)


def _save_state(job_id, state):
    _safe_write_json(_state_path(job_id), state)


def _append_log(job_id, line):
    with _LOCK:
        normalized_line = (line or "").rstrip()
        os.makedirs(LOGS_DIR, exist_ok=True)
        log_path = _log_path(job_id)

        if _ELAPSED_ONLY_LINE.match(normalized_line) and os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
                lines = handle.read().splitlines()

            if lines and not _ELAPSED_ONLY_LINE.search(lines[-1]):
                lines[-1] = f"{lines[-1].rstrip()} {normalized_line}".rstrip()
                with open(log_path, "w", encoding="utf-8") as handle:
                    handle.write("\n".join(lines) + "\n")
                return

        with open(log_path, "a", encoding="utf-8") as handle:
            handle.write(normalized_line + "\n")


def _replace_last_log_line(job_id, expected_line, new_line):
    with _LOCK:
        log_path = _log_path(job_id)
        if not os.path.exists(log_path):
            return False

        with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
            lines = handle.read().splitlines()

        if not lines or lines[-1] != expected_line.rstrip():
            return False

        lines[-1] = new_line.rstrip()
        with open(log_path, "w", encoding="utf-8") as handle:
            if lines:
                handle.write("\n".join(lines) + "\n")
        return True


def _append_path_log(path, line):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(line.rstrip() + "\n")


def _is_secondary_error_line(line):
    normalized = (line or "").strip().lower()
    if not normalized:
        return False

    return any(token in normalized for token in _SECONDARY_ERROR_PATTERNS)


def _touch_file(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8"):
        pass


# Regex para capturar contadores de registros descarregados/despejados
_RE_RECORD_COUNT = re.compile(r'(\d[\d .]*)\s*registros\s+des(?:carregados|pejados)')

# Estado de progresso por job_id
_progress_state = {}


def _format_elapsed_hms(elapsed_seconds):
    total_seconds = max(0, int(elapsed_seconds or 0))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _reset_progress_state(job_id, heartbeat_label=None):
    _progress_state[job_id] = {
        'last_count': None,
        'heartbeat_label': heartbeat_label,
        'started_at': time.monotonic(),
    }
    return _progress_state[job_id]


def _flush_progress_count(job_id):
    state = _progress_state.get(job_id)
    if not state or state.get('last_count') is None:
        return

    elapsed = _format_elapsed_hms(time.monotonic() - state.get('started_at', time.monotonic()))
    heartbeat_label = (state.get('heartbeat_label') or '').strip()
    if heartbeat_label:
        started_line = f"[EXEC] {heartbeat_label}"
        final_line = f"[EXEC] {heartbeat_label} - {state['last_count']} registros - {elapsed}"
        if not _replace_last_log_line(job_id, started_line, final_line):
            _append_log(job_id, final_line)
    else:
        _append_log(job_id, f"[EXEC] {state['last_count']} registros - {elapsed}")
    state['last_count'] = None


def _live_progress_line(job_id):
    state = _progress_state.get(job_id)
    if not state:
        return None, None

    heartbeat_label = (state.get('heartbeat_label') or '').strip()
    last_count = state.get('last_count')
    if not heartbeat_label or last_count is None:
        return None, None

    elapsed = _format_elapsed_hms(time.monotonic() - state.get('started_at', time.monotonic()))
    base_line = f"[EXEC] {heartbeat_label}"
    live_line = f"{base_line} - {last_count} registros parciais - {elapsed}"
    return base_line, live_line


def _append_file_tail(job_id, path, positions):
    if not os.path.exists(path):
        return

    position = positions.get(path, 0)
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            handle.seek(position)
            chunk = handle.read()
            positions[path] = handle.tell()
    except OSError:
        return

    if not chunk:
        return

    state = _progress_state.setdefault(job_id, {'last_count': None, 'heartbeat_label': None, 'started_at': time.monotonic()})

    for line in chunk.splitlines():
        if not line.strip():
            continue
        # Filtra linhas de ruído do Progress runtime log
        if re.search(r'\d{2}/\d{2}/\d{2}@\d{2}:\d{2}:\d{2}\.\d+\+\d+.*Logging level set to', line):
            continue
        if _PROGRESS_OUTPUT_NOISE.search(line):
            # Guarda a contagem para emitir apenas no final da tabela
            m = _RE_RECORD_COUNT.search(line)
            if m:
                count = m.group(1).strip().replace(' ', '').replace('.', '')
                state['last_count'] = count
            continue
        # Se havia contagem pendente, emite total final antes da próxima linha real
        _flush_progress_count(job_id)
        _append_log(job_id, line)


def _append_file_tail_to_path(path, positions, destination_path):
    if not os.path.exists(path):
        return

    position = positions.get(path, 0)
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            handle.seek(position)
            chunk = handle.read()
            positions[path] = handle.tell()
    except OSError:
        return

    if chunk:
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        with open(destination_path, "a", encoding="utf-8") as destination_handle:
            destination_handle.write(chunk)


def _command_head(command):
    if not command:
        return ""

    try:
        parts = shlex.split(command)
    except ValueError:
        parts = command.split()

    return parts[0] if parts else ""


def _command_diagnostics(command):
    head = _command_head(command)
    diagnostics = [
        f"[DIAG] cwd={os.getcwd()}",
        f"[DIAG] PATH={os.environ.get('PATH', '')}",
    ]

    if head:
        diagnostics.append(f"[DIAG] primeiro_token={head}")
        if os.path.isabs(head):
            diagnostics.append(
                f"[DIAG] primeiro_token_existe={os.path.exists(head)} executavel={os.access(head, os.X_OK)}"
            )

    return diagnostics


_PROGRESS_OUTPUT_NOISE = re.compile(
    r'OpenEdge Release'
    r'|Usando .ndice.*para despejo'
    r'|\d+ registros despejados'
    r'|Dump binario completo'
    r'|Dump bin.rio completo'
    r'|\*\* O banco .* sendo usado em modo multi'
    r'|\d[\d ]*registros descarregados'
)


def _append_command_output(job_id, raw_log_path, output_text):
    if not output_text:
        return

    for line in output_text.splitlines():
        if not line.strip():
            continue
        if raw_log_path:
            _append_path_log(raw_log_path, line)
        if _PROGRESS_OUTPUT_NOISE.search(line):
            continue
        _append_log(job_id, line)
        if _is_secondary_error_line(line):
            _append_log(job_id, f"[SECONDARY-ERROR] {line.strip()}")
            _append_path_log(raw_log_path, line)


def _run_shell_command(job_id, command, follow_log_path=None, raw_log_path=None, label=None, heartbeat_label=None):
    prepared_command = _prepare_progress_command_for_sudo(command)
    _reset_progress_state(job_id, heartbeat_label=heartbeat_label)

    if label:
        _append_log(job_id, label.replace(command, prepared_command) if command else label)
    if raw_log_path:
        _touch_file(raw_log_path)
        _append_path_log(raw_log_path, f"[START] {label or prepared_command}")
        _append_path_log(raw_log_path, f"[CMD] {prepared_command}")
        for diagnostic_line in _command_diagnostics(prepared_command):
            _append_path_log(raw_log_path, diagnostic_line)

    # Quando não há follow_log_path mas queremos capturar, redireciona stdout
    # para um arquivo temporário e segue-o. Isso evita buffering de pipe que
    # congela readline() em processos que bufferizam em bloco (proutil, etc.)
    _stdout_tempfile = None
    if raw_log_path and not follow_log_path:
        import tempfile
        _stdout_tempfile = tempfile.NamedTemporaryFile(
            mode='w', suffix='.stdout', dir=LOGS_DIR, delete=False,
        )
        _stdout_tempfile.close()
        # Redireciona stdout do shell para o arquivo temporário
        prepared_command = f"{prepared_command} >> {_stdout_tempfile.name} 2>&1"
        follow_log_path = _stdout_tempfile.name

    capture_output = raw_log_path is not None and _stdout_tempfile is None
    process = subprocess.Popen(
        prepared_command,
        shell=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE if capture_output else subprocess.DEVNULL,
        stderr=subprocess.STDOUT if capture_output else subprocess.DEVNULL,
        env=_build_progress_env(),
    )

    positions = {}
    while process.poll() is None:
        if follow_log_path:
            _append_file_tail(job_id, follow_log_path, positions)
            if raw_log_path and raw_log_path != follow_log_path:
                _append_file_tail_to_path(follow_log_path, positions, raw_log_path)
        time.sleep(0.3)

    # Leitura final após término
    if follow_log_path:
        _append_file_tail(job_id, follow_log_path, positions)
        if raw_log_path and raw_log_path != follow_log_path:
            _append_file_tail_to_path(follow_log_path, positions, raw_log_path)

    _flush_progress_count(job_id)

    if capture_output:
        stdout, _ = process.communicate()
        if stdout:
            if isinstance(stdout, bytes):
                stdout = stdout.decode("utf-8", errors="replace")
            _append_command_output(job_id, raw_log_path, stdout)

    # Limpa arquivo temporário de stdout
    if _stdout_tempfile:
        try:
            os.unlink(_stdout_tempfile.name)
        except OSError:
            pass

    if raw_log_path:
        _append_path_log(raw_log_path, f"[EXIT] rc={process.returncode}")
        if process.returncode == 127:
            _append_path_log(
                raw_log_path,
                "[DIAG] rc=127 normalmente indica comando ausente ou falha dentro do wrapper do shell; confira PATH, dependências do script e ambiente do serviço",
            )
            _append_log(job_id, "[DIAG] rc=127 normalmente indica comando ausente ou falha dentro do wrapper do shell; confira PATH, dependências do script e ambiente do serviço")

    return process.returncode


def _tail_log(log_path, limit=DEFAULT_TAIL_LINES):
    if not os.path.exists(log_path):
        return []

    with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
        lines = handle.read().splitlines()

    if limit and len(lines) > limit:
        return lines[-limit:]

    return lines

HOST_ROOT_MOUNT = os.environ.get("HOST_ROOT_MOUNT", "/hostfs")


def _resolve_host_path(base_path):
    if not base_path:
        return ""

    if os.path.isdir(base_path):
        return base_path

    mounted_path = os.path.join(HOST_ROOT_MOUNT, base_path.lstrip("/"))
    if os.path.isdir(mounted_path):
        return mounted_path

    return ""


def _resolve_base_path(base_path):
    return _resolve_host_path(base_path)


def _resolve_write_path(base_path):
    if not base_path:
        return ""

    if base_path.startswith(HOST_ROOT_MOUNT.rstrip("/") + "/") or base_path == HOST_ROOT_MOUNT:
        return base_path

    if base_path == "/totvs/database" or base_path.startswith("/totvs/database/"):
        return base_path

    return os.path.join(HOST_ROOT_MOUNT, base_path.lstrip("/"))


def _extract_follow_log_path(command):
    match = re.search(r"-logfile\s+(\S+)", command or "")
    if match:
        return match.group(1).strip().strip('"\'')

    return ""


def _step_log_dir(dump_path, load_path, db_name, operation):
    if operation == "load":
        return os.path.join(load_path, f"logs_{db_name}")

    return os.path.join(dump_path, db_name, "logs")


def _step_transcript_path(item, db_exec_path, step_number, step_title, operation="dump"):
    db_name = _database_name_from_path(db_exec_path)
    dump_path = item.get("dump_path", "") or os.path.join("/totvs/database/dump", db_name)
    load_path = item.get("load_path", "") or os.path.join("/totvs/database/load", db_name)
    log_dir = _step_log_dir(dump_path, load_path, db_name, operation)
    step_token = _sanitize_log_token(f"{step_number}_{step_title}")
    return os.path.join(log_dir, f"{step_token}.cmd.log")


def _count_matching_files(base_path, mask):
    resolved_base_path = _resolve_base_path(base_path)
    if not resolved_base_path:
        return 0

    pattern = os.path.join(resolved_base_path, mask)
    return sum(1 for path in glob.glob(pattern) if os.path.isfile(path))


def _list_matching_files(base_path, mask):
    resolved_base_path = _resolve_base_path(base_path)
    if not resolved_base_path:
        return []

    pattern = os.path.join(resolved_base_path, mask)
    return [path for path in sorted(glob.glob(pattern)) if os.path.isfile(path)]


def _database_name_from_path(db_path):
    normalized_path = (db_path or "").rstrip("/\\")
    if not normalized_path:
        return "database"
    return os.path.basename(normalized_path) or "database"


def _dump_destination_for_item(item, db_name):
    dump_root = item.get("dump_path", "") or os.path.join("/totvs/database/dump", db_name)
    return os.path.join(dump_root, db_name)


def _count_selected_files(selected_dbs):
    total = 0
    for db in selected_dbs:
        if db.get("selected_file_path"):
            total += 1
        else:
            total += _count_matching_files(db.get("dump_db_path", ""), db.get("db_mask", ""))
    return total


def _recalculate_state(state):
    items = state.get("items", [])
    total_dbs = len(items)
    completed_dbs = sum(1 for item in items if item.get("status") in ("done", "offline", "skipped", "error"))
    running_dbs = sum(1 for item in items if item.get("status") == "running")
    pending_dbs = sum(1 for item in items if item.get("status") in ("pending", "queued", None, ""))
    offline_dbs = sum(1 for item in items if item.get("status") == "offline")
    failed_dbs = sum(1 for item in items if item.get("status") == "error")
    progress_points = sum(int(item.get("progress", 0)) for item in items)
    overall_progress = int(progress_points / max(total_dbs, 1)) if total_dbs else 0

    state["total_dbs"] = total_dbs
    state["completed_dbs"] = completed_dbs
    state["running_dbs"] = running_dbs
    state["offline_dbs"] = offline_dbs
    state["failed_dbs"] = failed_dbs
    state["overall_progress"] = overall_progress
    state["active_dbs"] = [item.get("dump_db_path", "") for item in items if item.get("status") == "running"]
    state["active_labels"] = [item.get("message", "") for item in items if item.get("status") == "running" and item.get("message")]

    if state.get("status") == "running" and running_dbs == 0 and pending_dbs == 0:
        if failed_dbs:
            state["status"] = "failed"
        elif offline_dbs:
            state["status"] = "completed_with_warnings"
        else:
            state["status"] = "completed"

    if state.get("status") in ("completed", "completed_with_warnings", "failed"):
        if state.get("status") in ("completed", "completed_with_warnings"):
            state["overall_progress"] = 100
        state["finished_at"] = state.get("finished_at") or _utc_now()

    return state


def _update_state(job_id, **changes):
    with _LOCK:
        state = _read_state(job_id)
        if not state:
            return None

        state.update(changes)
        state["updated_at"] = _utc_now()
        _recalculate_state(state)
        _save_state(job_id, state)
        return state


def _update_item(job_id, index, **changes):
    with _LOCK:
        state = _read_state(job_id)
        if not state:
            return None

        items = state.get("items", [])
        if 0 <= index < len(items):
            items[index].update(changes)

        state["items"] = items
        state["updated_at"] = _utc_now()
        _recalculate_state(state)
        _save_state(job_id, state)
        return state


def _new_item(db):
    return {
        "dump_db_path": db.get("dump_db_path", ""),
        "db_mask": db.get("db_mask", ""),
        "dump_path": db.get("dump_path", ""),
        "load_path": db.get("load_path", ""),
        "selected_file_path": db.get("selected_file_path", ""),
        "status": "pending",
        "progress": 0,
        "message": "Aguardando início",
    }


def _prime_job_logs(selected_dbs):
    for db in selected_dbs:
        db_path = db.get("dump_db_path") or ""
        mask = db.get("db_mask", "")
        dump_path = db.get("dump_path", "") or os.path.join("/totvs/database/dump", _database_name_from_path(db_path))
        selected_file_path = db.get("selected_file_path") or ""
        files = [selected_file_path] if selected_file_path else _list_matching_files(db_path, mask)

        for file_path in files:
            if not file_path:
                continue

            db_name = os.path.splitext(os.path.basename(file_path))[0]
            dump_destination = os.path.join(dump_path, db_name)
            _touch_file(_command_log_path("mkdir", db_name))
            _touch_file(_command_log_path("dumpdf", db_name))
            _touch_file(os.path.join(dump_destination, "logs", "00_dumpdf_put.log"))


def _safe_format(template, context):
    if not template:
        return ""

    class _FallbackDict(dict):
        def __missing__(self, key):
            return "{" + key + "}"

    try:
        return template.format_map(_FallbackDict(context))
    except Exception:
        return template


def _build_preview_context(item, config, db_exec_path, operation="dump"):
    db_root_path = item.get("dump_db_path", "") or ""
    db_name = _database_name_from_path(db_exec_path)
    dump_path = item.get("dump_path", "") or os.path.join("/totvs/database/dump", db_name)
    load_path = item.get("load_path", "") or os.path.join("/totvs/database/load", db_name)
    resolved_dump_path = _resolve_write_path(dump_path)
    resolved_load_path = _resolve_write_path(load_path)
    dump_df_path = os.path.join(resolved_dump_path, f"{db_name}.df")
    dlc_path = config.get("progress", {}).get("dlc", "")
    if os.path.isdir(dlc_path):
        resolved_dlc_path = dlc_path
    else:
        resolved_dlc_path = _resolve_host_path(dlc_path) or dlc_path
    dlc_bin = os.path.join(resolved_dlc_path.rstrip("/"), "bin")
    return {
        "db_root_path": db_root_path,
        "db_path": db_exec_path,
        "db_name": db_name,
        "dump_path": resolved_dump_path,
        "dump_path_source": dump_path,
        "dump_df_path": dump_df_path,
        "load_path": resolved_load_path,
        "load_path_source": load_path,
        "temp_path": config.get("dump", {}).get("output_dir", "") or "/tmp",
        "log_dir": _step_log_dir(resolved_dump_path, resolved_load_path, db_name, operation),
        "dlc_bin": dlc_bin,
        "dlc_path": dlc_path,
        "resolved_dlc_path": resolved_dlc_path,
        "db_status": "",
        "db_opts": "-b",
    }


def _ensure_execution_directories(context):
    dump_path = context.get("dump_path") or ""
    load_path = context.get("load_path") or ""
    log_dir = context.get("log_dir") or ""

    if dump_path:
        os.makedirs(dump_path, exist_ok=True)

    if load_path:
        os.makedirs(load_path, exist_ok=True)

    if log_dir:
        os.makedirs(log_dir, exist_ok=True)


def _load_prerequisite_messages(context):
    load_path = context.get("load_path") or ""
    target_load_path = context.get("load_path_source") or load_path
    db_name = context.get("db_name") or ""
    if not load_path or not target_load_path or not db_name:
        return []

    db_file_path = os.path.join(load_path, f"{db_name}.db")
    structure_path = os.path.join(load_path, f"{db_name}.st")
    messages = []
    normalized_load_path = os.path.normpath(load_path)
    normalized_target_load_path = os.path.normpath(target_load_path)

    if os.path.exists(db_file_path):
        messages.append(f"Banco de destino já existe: {db_file_path}")

    if not os.path.exists(structure_path):
        messages.append(f"Arquivo de estrutura não encontrado: {structure_path}")

    if messages:
        return messages

    invalid_extent_paths = []
    rewritten_lines = []
    with open(structure_path, "r", encoding="utf-8", errors="replace") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            rewritten_line = raw_line
            line = raw_line.strip()
            if not line or line.startswith("#"):
                rewritten_lines.append(rewritten_line)
                continue

            match = re.search(r'(/\S+)', line)
            if not match:
                rewritten_lines.append(rewritten_line)
                continue

            extent_path = match.group(1)
            extent_dir = os.path.normpath(os.path.dirname(extent_path))
            if extent_dir != normalized_target_load_path:
                corrected_path = os.path.join(normalized_target_load_path, os.path.basename(extent_path))
                invalid_extent_paths.append((line_number, extent_path, corrected_path))
                rewritten_line = raw_line.replace(extent_path, corrected_path, 1)

            rewritten_lines.append(rewritten_line)

    if invalid_extent_paths:
        with open(structure_path, "w", encoding="utf-8") as handle:
            handle.writelines(rewritten_lines)

        preview = "; ".join(
            f"linha {line_number}: {extent_path} -> {corrected_path}"
            for line_number, extent_path, corrected_path in invalid_extent_paths[:5]
        )
        extra = ""
        if len(invalid_extent_paths) > 5:
            extra = f"; e mais {len(invalid_extent_paths) - 5} registro(s)"
        messages.append(
            "Arquivo de estrutura corrigido para usar o diretório de load "
            f"{normalized_target_load_path}: {preview}{extra}"
        )
        messages.append("Confira visualmente o arquivo .st ajustado antes de prosseguir")
        messages.append("Processo abortado após os ajustes; execute novamente se aceitar as correções feitas")

    return messages


def _append_step_result(job_id, step_number, step_title, generated_path=None, exit_code=0):
    _append_log(job_id, f"[DRY-RUN] Passo {step_number} - {step_title} retorno: {exit_code}")
    if generated_path:
        _append_log(job_id, f"[DRY-RUN] Resultado simulado: {generated_path}")


def _simulate_exit_code(step_number, command):
    if step_number == 1 and "-C busy" in (command or ""):
        return 6

    return 0


def _is_busy_check_command(command):
    return "-C busy" in (command or "")


def _is_load_progress_command(operation, command, context):
    if operation != "load":
        return False

    load_db_path = os.path.join(context.get("load_path") or "", context.get("db_name") or "")
    if not load_db_path:
        return False

    return "_progres" in (command or "") and f"-db {load_db_path}" in (command or "")


def _apply_load_busy_context(job_id, context, db_log, transcript_path):
    busy_command = f"{context.get('dlc_bin', '')}/proutil {context.get('load_path', '')}/{context.get('db_name', '')} -C busy"
    rc = _run_shell_command(
        job_id,
        busy_command,
        raw_log_path=transcript_path,
        heartbeat_label=f"{context.get('db_name', '')} Verificacao do banco destino",
    )
    busy_info = _busy_execution_options(rc)
    if not busy_info["valid"]:
        if rc == 64:
            db_log("Banco de destino em processo de inicialização; execução interrompida")
        else:
            db_log(f"Status do banco de destino não reconhecido ({rc}); execução interrompida")
        return False

    context.update({
        "db_status": busy_info["db_status"],
        "db_opts": busy_info["db_opts"],
        "load_db_opts_checked": True,
    })
    if busy_info["busy_state"] == "online":
        db_log("Banco de destino online confirmado")
    else:
        db_log("Banco de destino offline confirmado; comandos Progress seguirão com -1")
    return True


def _catalog_operation_name(catalog):
    steps = catalog.get("catalog", []) if isinstance(catalog, dict) else []
    for step in steps:
        text = " ".join(
            [
                str(step.get("title", "") or ""),
                str(step.get("description", "") or ""),
                str(step.get("command", "") or ""),
            ]
        ).lower()
        if "load" in text:
            return "load"
        if "dump" in text:
            return "dump"
    return "job"


def _busy_status_from_exit_code(exit_code):
    if exit_code == 6:
        return "banco online"

    if exit_code == 0:
        return "banco offline"

    if exit_code == 64:
        return "banco em inicialização"

    return f"status desconhecido ({exit_code})"


def _busy_execution_options(exit_code):
    if exit_code == 6:
        return {"db_status": "", "db_opts": "-b", "busy_state": "online", "valid": True}

    if exit_code == 0:
        return {"db_status": "-1", "db_opts": "-b -1", "busy_state": "offline", "valid": True}

    if exit_code == 64:
        return {"db_status": "", "db_opts": "-b", "busy_state": "starting", "valid": False}

    return {"db_status": "", "db_opts": "-b", "busy_state": "unknown", "valid": False}


def _simulate_mkdir_step(job_id, step_title, target_path):
    path_exists = os.path.isdir(target_path)
    _append_log(job_id, f"[DRY-RUN] Passo 2 - {step_title} comando aplicado: mkdir -p \"{target_path}\"")
    if path_exists:
        _append_log(job_id, f"[DRY-RUN] Passo 2 - {step_title} status do diretório: já existe")
    else:
        _append_log(job_id, f"[DRY-RUN] Passo 2 - {step_title} status do diretório: será criado")


def _resolve_inventory_path(loop_path, loop_file, context):
    candidates = []
    host_root = HOST_ROOT_MOUNT.rstrip("/")

    if loop_path and loop_file:
        candidates.append(os.path.join(loop_path, loop_file))
        candidates.append(os.path.join(host_root, loop_path.lstrip("/"), loop_file))

    db_name = context.get("db_name") or ""
    if db_name and loop_file:
        candidates.append(os.path.join(db_name, loop_file))

        dump_path = context.get("dump_path") or ""
        if dump_path:
            candidates.append(os.path.join(dump_path, loop_file))
            candidates.append(os.path.join(host_root, dump_path.lstrip("/"), loop_file))

        candidates.append(os.path.join(host_root, db_name, loop_file))

    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate

    return candidates[0] if candidates else ""


def _run_dry_job(job_id, config, catalog):
    steps = catalog.get("catalog", []) if isinstance(catalog, dict) else []
    operation = _catalog_operation_name(catalog)
    table_action = operation.upper() if operation in ("dump", "load") else "PROCESSAMENTO"

    try:
        _update_state(job_id, status="running", mode="dry", started_at=_utc_now(), message="Dry-run iniciado")
        _append_log(job_id, "[DRY-RUN] Iniciando simulação da sequência")

        state = _read_state(job_id)
        if not state or not state.get("items"):
            _append_log(job_id, "[DRY-RUN] Nenhum banco selecionado")
            _update_state(job_id, status="failed", message="Nenhum banco selecionado", finished_at=_utc_now(), mode="dry")
            return

        total_items = len(state["items"])
        _append_log(job_id, f"[DRY-RUN] Bancos selecionados: {total_items}")

        for item_index, item in enumerate(state["items"], start=1):
            db_root_path = item.get("dump_db_path") or ""
            mask = item.get("db_mask", "")
            selected_file_path = item.get("selected_file_path") or ""
            matching_files = [selected_file_path] if selected_file_path else _list_matching_files(db_root_path, mask)
            selected_display_path = selected_file_path or db_root_path

            if not matching_files:
                _append_log(job_id, f"[DRY-RUN] Nenhum arquivo encontrado para a máscara {mask} em {db_root_path}")
                _update_item(job_id, item_index - 1, status="skipped", progress=100, message="Nenhum arquivo encontrado")
                continue

            _update_item(job_id, item_index - 1, status="running", progress=0, message="Dry-run em andamento")
            _append_log(job_id, f"[DRY-RUN] Banco {item_index}/{total_items}: {selected_display_path}")

            total_files = len(matching_files)
            for file_index, file_path in enumerate(matching_files, start=1):
                db_exec_path = os.path.splitext(file_path)[0]
                context = _build_preview_context(item, config, db_exec_path, operation=operation)
                _append_log(job_id, f"[DRY-RUN] Database {file_index}/{total_files}: {context['db_path']}")

                for step in steps:
                    if not step.get("enabled", True):
                        continue

                    step_number = step.get("step", "?")
                    step_kind = step.get("kind", "command")
                    command = _safe_format(step.get("command", ""), context)
                    _append_log(job_id, f"[DRY-RUN] Comando: {command}")

                    if step_number == 1 and _is_busy_check_command(command):
                        busy_info = _busy_execution_options(_simulate_exit_code(step_number, command))
                        context.update({
                            "db_status": busy_info["db_status"],
                            "db_opts": busy_info["db_opts"],
                        })
                        _append_log(job_id, f"[DRY-RUN] Status detectado: {_busy_status_from_exit_code(_simulate_exit_code(step_number, command))}")

                    if step_kind == "loop":
                        loop_path = _safe_format(step.get("loop_source_path", ""), context)
                        loop_file = _safe_format(step.get("loop_source_file", ""), context)
                        inventory_path = _resolve_inventory_path(loop_path, loop_file, context)

                        if inventory_path and os.path.exists(inventory_path):
                            with open(inventory_path, "r", encoding="utf-8", errors="replace") as handle:
                                items = [line.strip() for line in handle.read().splitlines() if line.strip()]
                            for table_name in items:
                                loop_context = dict(context)
                                loop_context["table_name"] = table_name
                                loop_command = _safe_format(step.get("command", ""), loop_context)
                                _append_log(job_id, f"[DRY-RUN] {table_action} da tabela {table_name}")
                                _append_log(job_id, f"[DRY-RUN] Comando: {loop_command}")
                        else:
                            _append_log(job_id, "[DRY-RUN] Inventário não encontrado; simulando o loop como concluído sem erro.")

                    # O preview fica reduzido a banco + comando para evitar ruído.

            _update_item(job_id, item_index - 1, status="done", progress=100, message="Dry-run concluído")

        _update_state(job_id, status="completed", finished_at=_utc_now(), message="Dry-run concluído", mode="dry")
    except Exception as exc:
        _append_log(job_id, f"[DRY-RUN] Erro inesperado: {exc}")
        _update_state(job_id, status="failed", message=str(exc), finished_at=_utc_now(), mode="dry")
    finally:
        with _LOCK:
            index = _index_data()
            index["active_job_id"] = None
            index["last_job_id"] = job_id
            _save_index(index)

        _append_log(job_id, "[DRY-RUN] Simulação concluída")


def _run_real_job(job_id, config, catalog):
    steps = catalog.get("catalog", []) if isinstance(catalog, dict) else []
    operation = _catalog_operation_name(catalog)
    table_action = operation.upper() if operation in ("dump", "load") else "PROCESSAMENTO"
    total_action = operation.upper() if operation in ("dump", "load") else "JOB"

    try:
        _update_state(job_id, status="running", mode="real", started_at=_utc_now(), message="Execução iniciada")
        _append_log(job_id, "[EXEC] Iniciando execução real da sequência")

        state = _read_state(job_id)
        if not state or not state.get("items"):
            _append_log(job_id, "[EXEC] Nenhum banco selecionado")
            _update_state(job_id, status="failed", message="Nenhum banco selecionado", finished_at=_utc_now(), mode="real")
            return

        total_items = len(state["items"])
        _append_log(job_id, f"[EXEC] Bancos selecionados: {total_items}")

        for item_index, item in enumerate(state["items"], start=1):
            db_root_path = item.get("dump_db_path") or ""
            mask = item.get("db_mask", "")
            selected_file_path = item.get("selected_file_path") or ""
            matching_files = [selected_file_path] if selected_file_path else _list_matching_files(db_root_path, mask)
            selected_display_path = selected_file_path or db_root_path

            if not matching_files:
                _append_log(job_id, f"[EXEC] Nenhum arquivo encontrado para a máscara {mask} em {db_root_path}")
                _update_item(job_id, item_index - 1, status="skipped", progress=100, message="Nenhum arquivo encontrado")
                continue

            _update_item(job_id, item_index - 1, status="running", progress=0, message="Execução em andamento")
            _append_log(job_id, f"[EXEC] Banco {item_index}/{total_items}: {selected_display_path}")
            _db_start_time = time.time()

            total_files = len(matching_files)
            item_failed = False
            item_offline = False

            for file_index, file_path in enumerate(matching_files, start=1):
                db_exec_path = os.path.splitext(file_path)[0]
                db_name = os.path.basename(db_exec_path)
                db_log = lambda msg, _jid=job_id, _n=db_name: _append_log(_jid, f"[EXEC] {_n} {msg}")
                context = _build_preview_context(item, config, db_exec_path, operation=operation)
                context["load_db_opts_checked"] = False

                if operation == "load":
                    prerequisite_messages = _load_prerequisite_messages(context)
                    if prerequisite_messages:
                        for message in prerequisite_messages:
                            db_log(message)
                        db_log("Criação do banco abortada pelos critérios obrigatórios da fase de load")
                        item_failed = True
                        break

                _ensure_execution_directories(context)
                db_log(f"Database {file_index}/{total_files}: {context['db_path']}")

                for step in steps:
                    if not step.get("enabled", True):
                        continue

                    step_number = step.get("step", "?")
                    step_title = step.get("title", f"Fase {step_number}")
                    step_kind = step.get("kind", "command")
                    command = _safe_format(step.get("command", ""), context)

                    if _is_load_progress_command(operation, command, context) and not context.get("load_db_opts_checked"):
                        busy_transcript_path = _step_transcript_path(item, db_exec_path, step_number, "Verificar_banco_destino", operation=operation)
                        if not _apply_load_busy_context(job_id, context, db_log, busy_transcript_path):
                            item_failed = True
                            break
                        command = _safe_format(step.get("command", ""), context)

                    # Para passos com redirecionamento (> arquivo), extrai e exibe o destino
                    redir_match = re.search(r'>\s*(\S+)', command)
                    if redir_match:
                        generated_file = redir_match.group(1).strip().strip('"\'')
                        db_log(f"Passo {step_number} - {step_title} ({generated_file})")
                    else:
                        db_log(f"Passo {step_number} - {step_title}")

                    if step_kind == "loop":
                        loop_path = _safe_format(step.get("loop_source_path", ""), context)
                        loop_file = _safe_format(step.get("loop_source_file", ""), context)
                        inventory_path = _resolve_inventory_path(loop_path, loop_file, context)

                        if not inventory_path or not os.path.exists(inventory_path):
                            db_log(f"Passo {step_number} - Inventário não encontrado: {inventory_path}")
                            item_failed = True
                            break

                        with open(inventory_path, "r", encoding="utf-8", errors="replace") as handle:
                            tables = [line.strip() for line in handle.read().splitlines() if line.strip()]

                        if not tables:
                            db_log(f"Passo {step_number} - Inventário vazio")
                            item_failed = True
                            break

                        db_log(f"Passo {step_number} - Lendo tables.lst")
                        for table_name in tables:
                            db_log(f"Passo {step_number} - Tabela inventariada: {table_name}")

                        total_tables = len(tables)
                        for table_index, table_name in enumerate(tables, start=1):
                            loop_context = dict(context)
                            loop_context["table_name"] = table_name
                            loop_command = _safe_format(step.get("command", ""), loop_context)
                            table_progress = int(((table_index - 1) * 100) / max(total_tables, 1))
                            _update_item(
                                job_id,
                                item_index - 1,
                                status="running",
                                progress=table_progress,
                                message=f"Tabela atual: {table_name} ({table_index}/{total_tables})",
                            )
                            db_log(f"{table_action} da tabela {table_name} - executando")
                            transcript_path = _step_transcript_path(item, db_exec_path, step_number, f"{step_title}_{table_name}", operation=operation)
                            rc = _run_shell_command(
                                job_id,
                                loop_command,
                                raw_log_path=transcript_path,
                                heartbeat_label=f"{db_name} {table_action} da tabela {table_name} - executando",
                            )
                            if rc != 0:
                                db_log(f"Passo {step_number} - Falhou")
                                item_failed = True
                                break

                            _update_item(
                                job_id,
                                item_index - 1,
                                status="running",
                                progress=int((table_index * 100) / max(total_tables, 1)),
                                message=f"Tabela atual: {table_name} ({table_index}/{total_tables})",
                            )

                        if item_failed:
                            break

                        _update_item(job_id, item_index - 1, status="running", message=f"Passo {step_number} concluído")
                        db_log(f"Passo {step_number} - Concluido...")
                        continue

                    transcript_path = _step_transcript_path(item, db_exec_path, step_number, step_title, operation=operation)
                    if step_number == 1 and _is_busy_check_command(command):
                        # Passo 1: não emite 'Executando...' nem stdout no log principal
                        follow_log_path = _extract_follow_log_path(command)
                        rc = _run_shell_command(
                            job_id,
                            command,
                            follow_log_path=follow_log_path or None,
                            raw_log_path=transcript_path,
                            heartbeat_label=f"{db_name} Passo {step_number} - {step_title}",
                        )
                        busy_info = _busy_execution_options(rc)
                        if busy_info["valid"]:
                            context.update({
                                "db_status": busy_info["db_status"],
                                "db_opts": busy_info["db_opts"],
                            })
                            if busy_info["busy_state"] == "online":
                                db_log("Banco online confirmado")
                            else:
                                db_log("Banco offline confirmado; comandos Progress seguirão com -1")
                            db_log(f"Passo {step_number} - Concluido...")
                        elif rc == 64:
                            db_log("Banco em processo de inicialização; execução interrompida")
                            item_failed = True
                            break
                        else:
                            db_log(f"Status do banco não reconhecido ({rc}); execução interrompida")
                            item_failed = True
                            break
                    else:
                        db_log(f"Passo {step_number} - Executando...")
                        follow_log_path = _extract_follow_log_path(command)
                        rc = _run_shell_command(
                            job_id,
                            command,
                            follow_log_path=follow_log_path or None,
                            raw_log_path=transcript_path,
                            heartbeat_label=f"{db_name} Passo {step_number} - {step_title}",
                        )
                        if rc == 0:
                            db_log(f"Passo {step_number} - Concluido...")
                        else:
                            db_log(f"Passo {step_number} - Falhou")
                            item_failed = True
                            break

                if item_failed or item_offline:
                    break

            if item_failed:
                _update_item(job_id, item_index - 1, status="error", progress=100, message="Execução com erro")
            elif item_offline:
                _update_item(job_id, item_index - 1, status="offline", progress=100, message="Banco offline")
            else:
                _update_item(job_id, item_index - 1, status="done", progress=100, message="Execução concluída")

            _elapsed = int(time.time() - _db_start_time)
            _h, _rem = divmod(_elapsed, 3600)
            _m, _s = divmod(_rem, 60)
            _db_label = os.path.basename(selected_display_path.rstrip('/\\'))
            if _h:
                _elapsed_str = f"{_h}h{_m:02d}m{_s:02d}s"
            else:
                _elapsed_str = f"{_m}m{_s:02d}s"
            _append_log(job_id, f"[EXEC] Tempo total de {total_action} para o banco {_db_label}: {_elapsed_str}")

        _update_state(job_id, status="completed", finished_at=_utc_now(), message="Execução concluída", mode="real")
    except Exception as exc:
        _append_log(job_id, f"[EXEC] Erro inesperado: {exc}")
        _update_state(job_id, status="failed", message=str(exc), finished_at=_utc_now(), mode="real")
    finally:
        with _LOCK:
            index = _index_data()
            index["active_job_id"] = None
            index["last_job_id"] = job_id
            _save_index(index)

        _append_log(job_id, "[EXEC] Execução finalizada")


def _start_job(selected_dbs, config, catalog=None, mode="dry"):
    ensure_runtime_dirs()

    normalized_dbs = [db for db in selected_dbs if db.get("dump_db_path")]
    if not normalized_dbs:
        return None

    selected_file_count = _count_selected_files(normalized_dbs)
    job_id = uuid.uuid4().hex[:12]
    os.makedirs(_job_dir(job_id), exist_ok=True)

    is_dry = mode == "dry"
    operation = _catalog_operation_name(catalog)
    state = {
        "job_id": job_id,
        "status": "queued",
        "operation": operation,
        "mode": mode,
        "message": "Fila criada para dry-run" if is_dry else "Fila criada para execução",
        "created_at": _utc_now(),
        "started_at": None,
        "finished_at": None,
        "updated_at": _utc_now(),
        "items": [_new_item(db) for db in normalized_dbs],
        "log_path": _log_path(job_id),
        "total_dbs": len(normalized_dbs),
        "completed_dbs": 0,
        "running_dbs": 0,
        "offline_dbs": 0,
        "failed_dbs": 0,
        "overall_progress": 0,
        "active_dbs": [],
        "selected_count": len(normalized_dbs),
        "selected_file_count": selected_file_count,
    }

    _save_state(job_id, state)
    _prime_job_logs(normalized_dbs)

    with _LOCK:
        index = _index_data()
        index["active_job_id"] = job_id
        index["last_job_id"] = job_id
        _save_index(index)
        _touch_file(_log_path(job_id))

    if is_dry:
        _append_log(job_id, "[DRY-RUN] Job criado")
        _append_log(job_id, f"[DRY-RUN] Bancos selecionados: {len(normalized_dbs)}")
        worker = threading.Thread(target=_run_dry_job, args=(job_id, config, catalog or load_catalog()))
    else:
        _append_log(job_id, "[EXEC] Job criado")
        _append_log(job_id, f"[EXEC] Bancos selecionados: {len(normalized_dbs)}")
        worker = threading.Thread(target=_run_real_job, args=(job_id, config, catalog or load_catalog()))

    worker.start()
    return get_job_summary(job_id)


def start_dump_job(selected_dbs, config, catalog=None, mode="real"):
    return _start_job(selected_dbs, config, catalog=catalog, mode=mode)


def start_dry_run_job(selected_dbs, config, catalog=None):
    return _start_job(selected_dbs, config, catalog=catalog, mode="dry")


def get_job_summary(job_id, tail_limit=DEFAULT_TAIL_LINES):
    ensure_runtime_dirs()
    state = _read_state(job_id)
    if not state:
        return None

    state["log_tail"] = _tail_log(state.get("log_path", _log_path(job_id)), tail_limit)
    try:
        state["log_size"] = os.path.getsize(state.get("log_path", _log_path(job_id)))
    except OSError:
        state["log_size"] = 0

    live_log_base, live_log_line = _live_progress_line(job_id)
    state["live_log_base"] = live_log_base
    state["live_log_line"] = live_log_line

    state["selected_labels"] = [item.get("dump_db_path", "") for item in state.get("items", [])]
    state["selected_file_count"] = state.get("selected_file_count", len(state.get("selected_labels", [])))
    return state


def get_current_job_summary():
    index = _index_data()
    job_id = index.get("active_job_id")
    if not job_id:
        return None

    summary = get_job_summary(job_id)
    if not summary:
        return None

    summary["is_active"] = job_id == index.get("active_job_id")
    return summary


def get_job_log(job_id):
    state = _read_state(job_id)
    if not state:
        return None

    log_path = state.get("log_path", _log_path(job_id))
    if not os.path.exists(log_path):
        return ""

    with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read()


def get_job_log_chunk(job_id, offset=0):
    """Retorna (chunk, new_offset) a partir de offset em bytes."""
    state = _read_state(job_id)
    if not state:
        return None, 0

    log_path = state.get("log_path", _log_path(job_id))
    if not os.path.exists(log_path):
        return "", 0

    with open(log_path, "rb") as handle:
        handle.seek(0, 2)
        file_size = handle.tell()
        pos = max(0, min(int(offset or 0), file_size))
        handle.seek(pos)
        raw = handle.read()
        new_offset = handle.tell()

    if raw and not raw.endswith(b"\n"):
        last_newline = raw.rfind(b"\n")
        if last_newline >= 0:
            raw = raw[: last_newline + 1]
            new_offset = pos + last_newline + 1
        else:
            raw = b""
            new_offset = pos

    chunk = raw.decode("utf-8", errors="replace")
    return chunk, new_offset


def iter_log_lines(job_id, offset=0, poll_interval=1.0):
    log_path = _log_path(job_id)
    position = max(0, int(offset or 0))
    while True:
        if os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
                handle.seek(position)
                chunk = handle.read()
                position = handle.tell()
                if chunk:
                    for line in chunk.splitlines():
                        yield line

        state = _read_state(job_id)
        if not state:
            return

        if state.get("status") not in ("queued", "running"):
            return

        time.sleep(poll_interval)