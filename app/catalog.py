import os
from html import escape
from urllib.parse import parse_qs

from app.config import load_config, save_config

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CATALOG_PATH = os.path.join(BASE_DIR, "config", "dump_catalog.yaml")



def normalize_catalog(data):
    entries = data.get("catalog", []) if isinstance(data, dict) else []
    normalized = []

    for index, item in enumerate(entries, start=1):
        normalized.append(
            {
                "step": int(item.get("step", index) or index),
                "title": item.get("title", "") or f"Fase {index}",
                "kind": item.get("kind", "command") or "command",
                "enabled": bool(item.get("enabled", True)),
                "command": item.get("command", "") or "",
                "loop_source_path": item.get("loop_source_path", item.get("loop_source", "")) or "",
                "loop_source_file": item.get("loop_source_file", item.get("loop_item", "")) or "",
                "description": item.get("description", "") or "",
            }
        )

    normalized.sort(key=lambda entry: entry.get("step", 0))
    return {"catalog": normalized}


def load_catalog(path=CATALOG_PATH):
    if not os.path.exists(path):
        return {"catalog": []}

    try:
        return normalize_catalog(load_config(path))
    except Exception:
        return {"catalog": []}


def save_catalog(catalog, path=CATALOG_PATH):
    save_config(normalize_catalog(catalog), path)


def parse_catalog_form(body_bytes):
    form = parse_qs(body_bytes.decode("utf-8"), keep_blank_values=True)

    steps = form.get("catalog_step", [])
    titles = form.get("catalog_title", [])
    kinds = form.get("catalog_kind", [])
    commands = form.get("catalog_command", [])
    loop_source_paths = form.get("catalog_loop_source_path", [])
    loop_source_files = form.get("catalog_loop_source_file", [])
    descriptions = form.get("catalog_description", [])
    enabled_indexes = set(form.get("catalog_enabled", []))

    catalog = []
    total = max(len(steps), len(titles), len(kinds), len(commands), len(loop_source_paths), len(loop_source_files), len(descriptions))

    for index in range(total):
        catalog.append(
            {
                "step": int(steps[index] or index + 1) if index < len(steps) and steps[index].strip() else index + 1,
                "title": titles[index].strip() if index < len(titles) else f"Fase {index + 1}",
                "kind": kinds[index].strip() if index < len(kinds) else "command",
                "enabled": str(index) in enabled_indexes,
                "command": commands[index].strip() if index < len(commands) else "",
                "loop_source_path": loop_source_paths[index].strip() if index < len(loop_source_paths) else "",
                "loop_source_file": loop_source_files[index].strip() if index < len(loop_source_files) else "",
                "description": descriptions[index].strip() if index < len(descriptions) else "",
            }
        )

    return {"catalog": catalog}


def build_catalog_page(catalog, message="", page_title="Catálogo de comandos", save_path="/catalogo-comandos/save"):
    catalog = normalize_catalog(catalog)["catalog"]
    rows = []

    for index, item in enumerate(catalog):
        enabled_checked = "checked" if item.get("enabled", True) else ""
        rows.append(
            "<tr>"
            f"<td class='active-cell'><input class='active-checkbox' type='checkbox' name='catalog_enabled' value='{index}' {enabled_checked}></td>"
            f"<td class='phase-cell'><input class='phase-input' name='catalog_step' value='{escape(str(item.get('step', index + 1)))}' readonly></td>"
            f"<td class='title-cell'><input class='title-input' name='catalog_title' value='{escape(item.get('title', ''))}'></td>"
            f"<td class='type-cell'><select class='type-select' name='catalog_kind'><option value='command' {'selected' if item.get('kind') == 'command' else ''}>command</option><option value='loop' {'selected' if item.get('kind') == 'loop' else ''}>loop</option></select></td>"
            f"<td><textarea name='catalog_command' rows='4'>{escape(item.get('command', ''))}</textarea></td>"
            f"<td class='loop-path-cell'><input class='loop-input' name='catalog_loop_source_path' value='{escape(item.get('loop_source_path', ''))}'></td>"
            f"<td class='loop-file-cell'><input class='loop-input' name='catalog_loop_source_file' value='{escape(item.get('loop_source_file', ''))}'></td>"
            f"<td style='display:none'><input type='hidden' name='catalog_description' value='{escape(item.get('description', ''))}'></td>"
            "</tr>"
        )

    notice = f"<div class='notice'>{escape(message)}</div>" if message else ""

    return (
        "<!doctype html>"
        "<html lang='pt-BR'>"
        "<head>"
        "<meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{escape(page_title)}</title>"
        "<style>"
        "body{font-family:Arial,sans-serif;margin:32px;background:#f6f7fb;color:#1f2937}"
        ".page-header{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:8px}"
        ".page-actions{display:flex;align-items:center;gap:10px;flex-wrap:wrap}"
        ".icon-link{display:inline-flex;align-items:center;justify-content:center;height:40px;min-width:40px;padding:0 12px;border-radius:10px;background:#111827;color:#fff !important;text-decoration:none;font-weight:700;line-height:1}"
        ".icon-link.secondary{background:#374151}"
        "h1{margin:0 0 8px}"
        "p{margin:0 0 18px;color:#4b5563;max-width:980px}"
        ".card{background:#fff;border-radius:14px;box-shadow:0 8px 24px rgba(15,23,42,.08);padding:20px;margin-bottom:18px}"
        "table{width:100%;border-collapse:collapse;min-width:1200px}"
        "th,td{padding:10px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top}"
        "th.active-cell,td.active-cell{width:2ch;max-width:2ch}"
        ".active-checkbox{width:auto;min-width:auto;max-width:none;padding:0;display:block;margin:0 auto}"
        "th.phase-cell,td.phase-cell{width:4ch;max-width:4ch}"
        ".phase-input{width:4ch;min-width:4ch;max-width:4ch;box-sizing:border-box;padding:10px 2px;text-align:center}"
        "th.title-cell,td.title-cell{width:12rem;max-width:12rem}"
        "th.type-cell,td.type-cell{width:7rem;max-width:7rem}"
        "th.loop-path-cell,td.loop-path-cell{width:10rem;max-width:10rem}"
        "th.loop-file-cell,td.loop-file-cell{width:10rem;max-width:10rem}"
        ".title-input{width:12rem;min-width:12rem;max-width:12rem;box-sizing:border-box}"
        ".type-select{width:7rem;min-width:7rem;max-width:7rem;box-sizing:border-box}"
        ".loop-input{width:10rem;min-width:10rem;max-width:10rem;box-sizing:border-box}"
        "th{text-align:left;background:#f9fafb;position:sticky;top:0}"
        "input,textarea,select{width:100%;padding:10px 12px;border:1px solid #d1d5db;border-radius:10px;box-sizing:border-box;font:inherit}"
        "textarea{resize:vertical;min-height:92px}"
        ".compact{width:70px}"
        ".tiny{width:120px}"
        ".actions{display:flex;gap:12px;margin-top:18px;align-items:center}"
        "button{padding:10px 14px;border:0;border-radius:10px;cursor:pointer;background:#111827;color:#fff}"
        ".notice{background:#ecfeff;color:#155e75;border:1px solid #a5f3fc;padding:12px 14px;border-radius:10px;margin-bottom:18px}"
        ".hint{margin-top:10px;color:#64748b;font-size:13px}"
        "</style>"
        "</head>"
        "<body>"
        f"<div class='page-header'><div><h1>{escape(page_title)}</h1><p>Edite aqui os comandos e as regras de execução de cada fase. O passo 8 está modelado como loop para iterar sobre o inventário de tabelas.</p></div><div class='page-actions'><a class='icon-link secondary' href='/'>Início</a><a class='icon-link secondary' href='/config'>Configuração</a></div></div>"
        f"{notice}"
        f"<form method='post' action='{escape(save_path)}'>"
        "<div class='card'>"
        "<table>"
        "<thead><tr><th class='active-cell'>Ativo</th><th>Fase</th><th>Título</th><th>Tipo</th><th>Comando</th><th>Loop path</th><th>Loop arquivo</th></tr></thead>"
        "<tbody>"
        + "".join(rows)
        + "</tbody></table>"
        "<div class='hint'>Use as variáveis entre chaves no texto do comando: {db_path}, {dump_path}, {db_name}, {table_name}, {temp_path}, {log_dir}, {dlc_bin} e {db_opts}. Para loop, informe o path da lista em Loop path e o nome do arquivo em Loop arquivo.</div>"
        "</div>"
        "<div class='actions'><button type='submit'>Salvar catálogo</button></div>"
        "</form>"
        "</body></html>"
    )