if __package__ in (None, ""):
    import os
    import sys

    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import argparse
import json
import os
import glob
import time
from html import escape
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, quote, urlparse

from app.config import load_config, save_config
from app.catalog import build_catalog_page, load_catalog, parse_catalog_form, save_catalog
from app.jobs import get_current_job_summary, get_job_log, get_job_log_chunk, get_job_summary, list_job_history, start_dry_run_job, start_dump_job
from app.utils import is_db_online
from app.runner import run_command

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")
CATALOG_PATH = os.path.join(BASE_DIR, "config", "dump_catalog.yaml")
LOAD_CATALOG_PATH = os.path.join(BASE_DIR, "config", "load_catalog.yaml")
HOST_ROOT_MOUNT = os.environ.get("HOST_ROOT_MOUNT", "/hostfs")


try:
    from http.server import ThreadingHTTPServer
except ImportError:
    class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
        daemon_threads = True


DUMP_UI_SCRIPT = r"""
function openExplorer(button){const row=button.closest('tr');const baseInput=row.querySelector('.dump-path-input');const baseCell=row.querySelector('.dump-path-cell');const maskInput=row.querySelector('.mask-input');const base=(baseInput?baseInput.value.trim():(baseCell?baseCell.textContent.trim():''));const mask=(maskInput?maskInput.value.trim():'')||'*.db';const modal=document.getElementById('explorer-modal');const meta=document.getElementById('explorer-meta');const content=document.getElementById('explorer-content');meta.textContent='Carregando '+base+' / '+mask+'...';content.innerHTML='';modal.classList.add('open');fetch('/config/explore?path='+encodeURIComponent(base)+'&mask='+encodeURIComponent(mask)).then(r=>r.json()).then(data=>{meta.textContent=(data.path||base)+' / '+(data.mask||mask);if(data.error){content.innerHTML="<div class='error-box'>"+escapeHtml(data.error)+"</div>";return;}if(!data.files.length){content.innerHTML="<div class='error-box'>Nenhum arquivo encontrado.</div>";return;}content.innerHTML="<ul class='file-list'>"+data.files.map(file=>"<li>"+escapeHtml(file)+"</li>").join('')+"</ul>";}).catch(err=>{content.innerHTML="<div class='error-box'>"+escapeHtml(String(err))+"</div>";});}
function closeExplorer(event){if(event&&event.target&&event.target.id!=='explorer-modal'){return;}document.getElementById('explorer-modal').classList.remove('open');}
function escapeHtml(text){const div=document.createElement('div');div.textContent=String(text);return div.innerHTML;}
function getDatabaseEntries(){return Array.isArray(window.__DATABASE_ENTRIES__)?window.__DATABASE_ENTRIES__.slice():[];}
let startDumpSelectedFiles=new Set((function(){try{var s=localStorage.getItem('dumpSelectedFiles');return s?JSON.parse(s):[];}catch(e){return [];}})());
function renderStartBankItem(entry,filePath,checked){const fileName=(filePath||'').split(/[\\/]/).pop()||filePath;return '<label class=\'start-modal-item\'><input type=\'checkbox\' class=\'start-db-select\' data-dump-db-path=\''+escapeHtml(entry.path)+'\' data-db-mask=\''+escapeHtml(fileName)+'\' data-dump-path=\''+escapeHtml(entry.dumpPath)+'\' data-load-path=\''+escapeHtml(entry.loadPath)+'\' value=\''+escapeHtml(filePath)+'\''+(checked?' checked':'')+'><span><strong>'+escapeHtml(fileName)+'</strong></span></label>';}
function syncStartDumpSelectionFromModal(){startDumpSelectedFiles=new Set(Array.from(document.querySelectorAll('.start-db-select:checked')).map(cb=>cb.value));try{localStorage.setItem('dumpSelectedFiles',JSON.stringify(Array.from(startDumpSelectedFiles)));}catch(e){}}
function openStartDumpModal(){const modal=document.getElementById('start-dump-modal');const content=document.getElementById('start-modal-content');const meta=document.getElementById('start-modal-meta');const entries=getDatabaseEntries();const persistedSelection=startDumpSelectedFiles.size?startDumpSelectedFiles:null;meta.textContent='Carregando bancos filtrados...';content.innerHTML='<div class=\'start-modal-loading\'>Buscando bancos conforme a mascara de cada caminho...</div>';modal.classList.add('open');Promise.all(entries.map(entry=>fetch('/config/explore?path='+encodeURIComponent(entry.path)+'&mask='+encodeURIComponent(entry.mask||'*.db')).then(r=>r.json()).catch(()=>({files:[],error:'Falha ao listar bancos.'}))))
.then(results=>{const bankItems=[];results.forEach((result,index)=>{const entry=entries[index];const files=Array.isArray(result.files)?result.files:[];files.forEach(filePath=>bankItems.push({entry:entry,filePath:filePath}));});meta.textContent=bankItems.length+' banco(s) encontrado(s)';if(!bankItems.length){content.innerHTML='<div class=\'start-modal-loading\'>Nenhum banco encontrado para a mascara informada.</div>';startDumpSelectedFiles=new Set();return;}content.innerHTML='<div class=\'start-modal-list\'>'+bankItems.map(item=>renderStartBankItem(item.entry,item.filePath,persistedSelection?persistedSelection.has(item.filePath):true)).join('')+'</div>';syncStartDumpSelectionFromModal();content.querySelectorAll('.start-db-select').forEach(cb=>cb.addEventListener('change',syncStartDumpSelectionFromModal));});}
function closeStartDumpModal(event){if(event&&event.target&&event.target.id!=='start-dump-modal'){return;}const modal=document.getElementById('start-dump-modal');if(modal){modal.classList.remove('open');}}
function _isElapsedOnlyLine(line){return /^\d{2}:\d{2}:\d{2}$/.test((line||'').trim());}
function syncTableSelectionFromModal(selected){document.querySelectorAll('.job-select').forEach(cb=>{cb.checked=selected.has(cb.value);});refreshSelectionSummary();}
function getSelectedDumpMode(){const checkbox=document.getElementById('simulation-mode');return checkbox&&checkbox.checked?'dry':'real';}
function syncModeState(){const checkbox=document.getElementById('simulation-mode');const state=document.getElementById('mode-state');if(state){state.textContent=checkbox&&checkbox.checked?'':'';}}
function startSelectedDumpWithMode(selectedOverride,mode){if(!Array.isArray(selectedOverride)){openStartDumpModal();return;}const selected=selectedOverride;if(!selected.length){alert('Selecione pelo menos um banco.');return;}const button=document.getElementById('start-dump-button');button.disabled=true;fetch('/dump/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({databases:selected,mode:mode||getSelectedDumpMode()})}).then(r=>r.json()).then(job=>{button.disabled=false;if(!job||!job.job_id){alert('Não foi possível iniciar o dump.');return;}activeJob=job;updateJobBadge(job, job.selected_file_count||selectedFileCount);appendJobLog(job.log_tail||['Job iniciado.'], true);connectJobStream(job.job_id, job.log_size||0);}).catch(err=>{button.disabled=false;alert(String(err));});}
function confirmStartDumpModal(){const selected=Array.from(document.querySelectorAll('.start-db-select:checked')).map(cb=>({dump_db_path:cb.dataset.dumpDbPath||'',db_mask:cb.dataset.dbMask||'',dump_path:cb.dataset.dumpPath||'',load_path:cb.dataset.loadPath||'',selected_file_path:cb.value||''})).filter(item=>item.dump_db_path&&item.db_mask);if(!selected.length){alert('Selecione pelo menos um banco.');return;}syncStartDumpSelectionFromModal();closeStartDumpModal();syncTableSelectionFromModal(new Set(selected.map(item=>item.dump_db_path)));startSelectedDumpWithMode(selected,getSelectedDumpMode());}
let jobSource=null;let activeJob=null;let selectedFileCount=0;let _liveLogBase='';let _liveLogLine='';function selectedDatabases(){return [];}function selectedRows(){return [];}function syncCheckboxState(){return {total:0,checked:0,all:false};}function setSelectionHint(extra){const hint=document.getElementById('selection-hint');if(hint){hint.textContent=extra||'Pronto para selecionar nos modais';}}function countFilesForEntry(entry){const base=(entry&&entry.path)||'';const mask=(entry&&entry.mask)||'*.db';if(!base){return Promise.resolve(0);}return fetch('/config/explore?path='+encodeURIComponent(base)+'&mask='+encodeURIComponent(mask)).then(r=>r.json()).then(data=>Array.isArray(data.files)?data.files.length:0).catch(()=>0);}function refreshSelectionSummary(){const entries=getDatabaseEntries();if(!entries.length){selectedFileCount=0;setSelectionHint('Nenhum ambiente configurado');if(!activeJob){updateJobBadge({status:'idle',operation:'job',total_dbs:0,completed_dbs:0,running_dbs:0,overall_progress:0,active_dbs:[],active_labels:[]},0);}return Promise.resolve(0);}return Promise.all(entries.map(countFilesForEntry)).then(counts=>{selectedFileCount=counts.reduce((sum,value)=>sum+value,0);setSelectionHint(selectedFileCount+' arquivos disponíveis para seleção');if(!activeJob){updateJobBadge({status:'idle',operation:'job',total_dbs:0,completed_dbs:0,running_dbs:0,overall_progress:0,active_dbs:[],active_labels:[]},selectedFileCount);}else{updateJobBadge(activeJob,activeJob.selected_file_count||selectedFileCount);}return selectedFileCount;});}function toggleAllJobs(checked){return checked;}function scrollJobLogToBottom(){const jobLog=document.getElementById('job-log');if(!jobLog){return;}requestAnimationFrame(()=>{jobLog.scrollTop=jobLog.scrollHeight;requestAnimationFrame(()=>{jobLog.scrollTop=jobLog.scrollHeight;});});}function _renderLogDOM(){const jobLog=document.getElementById('job-log');if(!jobLog){return;}var displayLines=_logLines.slice();if(_liveLogBase&&_liveLogLine&&displayLines.length&&displayLines[displayLines.length-1]===_liveLogBase){displayLines[displayLines.length-1]=_liveLogLine;}jobLog.textContent=displayLines.join('\n');scrollJobLogToBottom();}function _setLiveLogLine(baseLine,liveLine){_liveLogBase=baseLine||'';_liveLogLine=liveLine||'';_renderLogDOM();}function appendJobLog(lines,reset){if(reset){_logLines=[];}if(Array.isArray(lines)){_logLines=lines.slice();}else if(lines){_logLines=_logLines.concat(String(lines).split(/\r?\n/).filter(function(line){return line!=='';}));}_renderLogDOM();}function isTerminalJob(job){return job&&['failed','completed','completed_with_warnings'].includes(job.status);}function refreshFinalJobLog(job){if(!job||!job.job_id){return Promise.resolve();}return fetch('/dump/log?job_id='+encodeURIComponent(job.job_id)).then(r=>r.text()).then(text=>{appendJobLog(text.split(/\r?\n/).filter(function(line){return line!=='';}),true);});}function updateJobBadge(job,banksCount){const badge=document.getElementById('job-badge');const meta=document.getElementById('job-meta');const total=typeof banksCount==='number'?banksCount:(job.selected_file_count||selectedFileCount||0);const done=job.completed_dbs||0;const running=job.running_dbs||0;const progress=job.overall_progress||0;const current=((job.active_labels&&job.active_labels.length)?job.active_labels:(job.active_dbs||[])).join(', ')||'Nenhum';const operation=(job.operation||'job');const noun=operation==='load'?'Load':operation==='dump'?'Dump':'Job';const stateClass=job.status==='running'||job.status==='queued'?'running':job.status==='failed'?'failed':job.status==='completed_with_warnings'?'warning':job.status==='completed'?'completed':'idle';const label=job.status==='running'?noun+' em andamento':job.status==='queued'?noun+' na fila':job.status==='failed'?noun+' com falha':job.status==='completed_with_warnings'?noun+' finalizado com alertas':job.status==='completed'?noun+' finalizado':'Nenhuma atividade em andamento';if(badge){badge.className='job-badge '+stateClass;badge.textContent=label;}meta.innerHTML='<div class="job-meta-item"><span>Status</span><strong>'+escapeHtml(label)+'</strong></div><div class="job-meta-item"><span>Ambientes</span><strong>'+total+'</strong></div><div class="job-meta-item"><span>Concluídos</span><strong>'+done+'</strong></div><div class="job-meta-item"><span>Em execução</span><strong>'+running+'</strong></div><div class="job-meta-item"><span>Progresso</span><strong>'+progress+'%</strong></div><div class="job-meta-item"><span>Ativos</span><strong>'+escapeHtml(current)+'</strong></div>';}let _pollTimer=null;var _logOffset=0;var _logLines=[];var _MAX_LOG_LINES=2000;var _POLL_DELAY_MS=500;var _FETCH_TIMEOUT_MS=5000;function _appendToLogDOM(chunk){if(!chunk)return;var newLines=chunk.replace(/\n+$/,'').split('\n');if(!newLines.length||(newLines.length===1&&!newLines[0]))return;for(var i=0;i<newLines.length;i++){var line=newLines[i];if(_isElapsedOnlyLine(line)&&_logLines.length){_logLines[_logLines.length-1]=(_logLines[_logLines.length-1].replace(/\s+$/,'')+' '+line.trim()).trim();continue;}if(line&&/^\-\s+/.test(line)&&_logLines.length){_logLines[_logLines.length-1]=(_logLines[_logLines.length-1].replace(/\s+$/,'')+' '+line.replace(/^\-\s+/,'- ')).trim();continue;}_logLines.push(line);}if(_logLines.length>_MAX_LOG_LINES){_logLines=_logLines.slice(-_MAX_LOG_LINES);}_renderLogDOM();}function _readCurrentLogLines(){var el=document.getElementById('job-log');if(!el||!el.textContent)return [];var lines=el.textContent.split(/\r?\n/).filter(function(line){return line!=='';});if(_liveLogBase&&_liveLogLine&&lines.length&&lines[lines.length-1]===_liveLogLine){lines[lines.length-1]=_liveLogBase;}return lines;}function _fetchWithTimeout(url,parseResponse){var controller=new AbortController();var timeoutId=setTimeout(function(){controller.abort();},_FETCH_TIMEOUT_MS);return fetch(url,{signal:controller.signal,cache:'no-store'}).then(function(response){return parseResponse(response);}).finally(function(){clearTimeout(timeoutId);});}function connectJobStream(jobId,offset){if(jobSource){try{jobSource.close();}catch(e){}jobSource=null;}if(_pollTimer){clearTimeout(_pollTimer);_pollTimer=null;}if(!jobId)return;_logOffset=Math.max(0,Number(offset)||0);_logLines=_readCurrentLogLines();var _terminal=false;var _terminalPolls=0;function scheduleNext(){if(_terminal&&_terminalPolls>=5)return;_pollTimer=setTimeout(poll,_POLL_DELAY_MS);}function poll(){var logUrl='/dump/log?job_id='+encodeURIComponent(jobId)+'&offset='+_logOffset;var p1=_fetchWithTimeout(logUrl,function(r){if(!r.ok)return '';return r.text();}).then(function(chunk){if(chunk){_logOffset+=new Blob([chunk]).size;_appendToLogDOM(chunk);}}).catch(function(e){console.error('poll-log',e);});var p2=_fetchWithTimeout('/dump/current',function(r){return r.status===204?null:r.json();}).then(function(job){if(!job||job.job_id!==jobId)return;activeJob=job;updateJobBadge(job,job.selected_file_count||selectedFileCount);_setLiveLogLine(job.live_log_base,job.live_log_line);if(!_terminal&&isTerminalJob(job)){_terminal=true;_terminalPolls=0;_setLiveLogLine('','');refreshFinalJobLog(job).catch(function(e){console.error('final-log',e);});}if(_terminal)_terminalPolls++;}).catch(function(e){console.error('poll-current',e);});Promise.allSettled([p1,p2]).finally(scheduleNext);}poll();}function _restoreJobSelection(job){return job;}function _hydrateActiveJob(){return _fetchWithTimeout('/dump/current',function(r){return r.status===204?null:r.json();}).then(function(job){if(!job||!job.job_id){return refreshSelectionSummary();}activeJob=job;selectedFileCount=job.selected_file_count||0;setSelectionHint((job.selected_file_count||0)+' arquivos encontrados');updateJobBadge(job,job.selected_file_count||selectedFileCount);return fetch('/dump/log?job_id='+encodeURIComponent(job.job_id),{cache:'no-store'}).then(function(r){return r.ok?r.text():'';}).then(function(text){appendJobLog((text||'').split(/\r?\n/).filter(function(line){return line!=='';}),true);_setLiveLogLine(job.live_log_base,job.live_log_line);connectJobStream(job.job_id,job.log_size||new Blob([text||'']).size||0);});}).catch(function(e){console.error('hydrate-job',e);return refreshSelectionSummary();});}function startSelectedDump(selectedOverride){if(!Array.isArray(selectedOverride)){openStartDumpModal();return;}const selected=selectedOverride;if(!selected.length){alert('Selecione pelo menos um banco.');return;}const button=document.getElementById('start-dump-button');button.disabled=true;fetch('/dump/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({databases:selected})}).then(r=>r.json()).then(job=>{button.disabled=false;if(!job||!job.job_id){alert('Não foi possível iniciar o dump.');return;}activeJob=job;updateJobBadge(job,job.selected_file_count||selectedFileCount);appendJobLog(job.log_tail||['Job iniciado.'],true);_setLiveLogLine(job.live_log_base,job.live_log_line);connectJobStream(job.job_id,job.log_size||0);}).catch(err=>{button.disabled=false;alert(String(err));});}const simulationModeCheckbox=document.getElementById('simulation-mode');if(simulationModeCheckbox){simulationModeCheckbox.addEventListener('change',syncModeState);syncModeState();}refreshSelectionSummary();_hydrateActiveJob();
"""

DUMP_UI_FALLBACK_SCRIPT = r"""
let startLoadSelectedFiles=new Set((function(){try{var s=localStorage.getItem('loadSelectedFiles');return s?JSON.parse(s):[];}catch(e){return [];}})());
function renderStartLoadBankItem(entry,filePath,checked){const fileName=(filePath||'').split(/[\\/]/).pop()||filePath;return '<label class=\'start-modal-item\'><input type=\'checkbox\' class=\'start-load-db-select\' data-dump-db-path=\''+escapeHtml(entry.path)+'\' data-db-mask=\''+escapeHtml(fileName)+'\' data-dump-path=\''+escapeHtml(entry.dumpPath)+'\' data-load-path=\''+escapeHtml(entry.loadPath)+'\' value=\''+escapeHtml(filePath)+'\''+(checked?' checked':'')+'><span><strong>'+escapeHtml(fileName)+'</strong></span></label>';}
function syncStartLoadSelectionFromModal(){startLoadSelectedFiles=new Set(Array.from(document.querySelectorAll('.start-load-db-select:checked')).map(cb=>cb.value));try{localStorage.setItem('loadSelectedFiles',JSON.stringify(Array.from(startLoadSelectedFiles)));}catch(e){}}
function openStartLoadModal(){const modal=document.getElementById('start-load-modal');const content=document.getElementById('start-load-modal-content');const meta=document.getElementById('start-load-modal-meta');const entries=getDatabaseEntries();const persistedSelection=startLoadSelectedFiles.size?startLoadSelectedFiles:null;meta.textContent='Carregando bancos filtrados...';content.innerHTML='<div class=\'start-modal-loading\'>Buscando bancos conforme a mascara de cada caminho...</div>';modal.classList.add('open');Promise.all(entries.map(entry=>fetch('/config/explore?path='+encodeURIComponent(entry.path)+'&mask='+encodeURIComponent(entry.mask||'*.db')).then(r=>r.json()).catch(()=>({files:[],error:'Falha ao listar bancos.'}))))
.then(results=>{const bankItems=[];results.forEach((result,index)=>{const entry=entries[index];const files=Array.isArray(result.files)?result.files:[];files.forEach(filePath=>bankItems.push({entry:entry,filePath:filePath}));});meta.textContent=bankItems.length+' banco(s) encontrado(s)';if(!bankItems.length){content.innerHTML='<div class=\'start-modal-loading\'>Nenhum banco encontrado para a mascara informada.</div>';startLoadSelectedFiles=new Set();return;}content.innerHTML='<div class=\'start-modal-list\'>'+bankItems.map(item=>renderStartLoadBankItem(item.entry,item.filePath,persistedSelection?persistedSelection.has(item.filePath):true)).join('')+'</div>';syncStartLoadSelectionFromModal();content.querySelectorAll('.start-load-db-select').forEach(cb=>cb.addEventListener('change',syncStartLoadSelectionFromModal));});}
function closeStartLoadModal(event){if(event&&event.target&&event.target.id!=='start-load-modal'){return;}const modal=document.getElementById('start-load-modal');if(modal){modal.classList.remove('open');}}
function startSelectedLoadWithMode(selectedOverride,mode){if(!Array.isArray(selectedOverride)){openStartLoadModal();return;}const selected=selectedOverride;if(!selected.length){alert('Selecione pelo menos um banco.');return;}const button=document.getElementById('start-load-button');if(button){button.disabled=true;}fetch('/load/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({databases:selected,mode:mode||getSelectedDumpMode()})}).then(r=>r.json()).then(job=>{if(button){button.disabled=false;}if(!job||!job.job_id){alert('Não foi possível iniciar o load.');return;}activeJob=job;updateJobBadge(job,job.selected_file_count||selectedFileCount);appendJobLog(job.log_tail||['Job iniciado.'],true);_setLiveLogLine(job.live_log_base,job.live_log_line);connectJobStream(job.job_id,job.log_size||0);}).catch(err=>{if(button){button.disabled=false;}alert(String(err));});}
function startSelectedLoad(selectedOverride){if(!Array.isArray(selectedOverride)){openStartLoadModal();return;}return startSelectedLoadWithMode(selectedOverride,getSelectedDumpMode());}
function confirmStartLoadModal(){const selected=Array.from(document.querySelectorAll('.start-load-db-select:checked')).map(cb=>({dump_db_path:cb.dataset.dumpDbPath||'',db_mask:cb.dataset.dbMask||'',dump_path:cb.dataset.dumpPath||'',load_path:cb.dataset.loadPath||'',selected_file_path:cb.value||''})).filter(item=>item.dump_db_path&&item.db_mask);if(!selected.length){alert('Selecione pelo menos um banco.');return;}syncStartLoadSelectionFromModal();closeStartLoadModal();syncTableSelectionFromModal(new Set(selected.map(item=>item.dump_db_path)));startSelectedLoadWithMode(selected,getSelectedDumpMode());}
"""

HISTORY_UI_SCRIPT = r"""
let historyJobs=[];
function closeHistoryModal(event){if(event&&event.target&&event.target.id!=='history-modal'){return;}const modal=document.getElementById('history-modal');if(modal){modal.classList.remove('open');}}
function _historyStatusLabel(job){if(!job)return 'Desconhecido';if(job.is_active)return 'Em andamento';if(job.status==='completed')return 'Concluido';if(job.status==='completed_with_warnings')return 'Concluido com alertas';if(job.status==='failed'||job.status==='error')return 'Falhou';if(job.status==='queued')return 'Na fila';return String(job.status||'Desconhecido');}
function _historyOperationLabel(job){const op=(job&&job.operation)||'job';return op==='load'?'LOAD':op==='dump'?'DUMP':'JOB';}
function _historyTimeLabel(job){return (job&&job.finished_at)||(job&&job.updated_at)||(job&&job.started_at)||(job&&job.created_at)||'';}
function _historyPreview(job){const tail=Array.isArray(job&&job.log_tail)?job.log_tail.filter(Boolean):[];if(!tail.length)return 'Sem linhas recentes no log.';return tail.slice(-2).join(' | ');}
function _renderHistoryList(items){const content=document.getElementById('history-content');const meta=document.getElementById('history-meta');if(!content||!meta)return;historyJobs=Array.isArray(items)?items:[];meta.textContent=historyJobs.length+' job(s) encontrados';if(!historyJobs.length){content.innerHTML='<div class="start-modal-loading">Nenhum job anterior encontrado.</div>';return;}content.innerHTML='<div class="history-list">'+historyJobs.map(function(job){const status=_historyStatusLabel(job);const title=((job.operation||'job').toUpperCase()+' '+(job.job_id||'' )).trim();const activeTag=job.is_active?'<span class="history-chip active">ativo</span>':'';const lastTag=job.is_last?'<span class="history-chip">ultimo</span>':'';return '<button type="button" class="history-item" onclick="openHistoryJob(\''+escapeHtml(job.job_id||'')+'\')"><span class="history-item-top"><strong>'+escapeHtml(title)+'</strong><span class="history-item-tags">'+activeTag+lastTag+'</span></span><span class="history-item-meta">'+escapeHtml(_historyOperationLabel(job))+' • '+escapeHtml(status)+' • '+escapeHtml(_historyTimeLabel(job))+'</span><span class="history-item-preview">'+escapeHtml(_historyPreview(job))+'</span></button>';}).join('')+'</div>';}
function openHistoryModal(){const modal=document.getElementById('history-modal');const content=document.getElementById('history-content');const meta=document.getElementById('history-meta');if(!modal||!content||!meta)return;modal.classList.add('open');meta.textContent='Carregando historico de jobs...';content.innerHTML='<div class="start-modal-loading">Lendo jobs anteriores...</div>';fetch('/jobs/history',{cache:'no-store'}).then(function(r){return r.ok?r.json():[];}).then(function(items){_renderHistoryList(items);}).catch(function(err){content.innerHTML='<div class="error-box">'+escapeHtml(String(err))+'</div>';});}
function openHistoryJob(jobId){const job=historyJobs.find(function(item){return item&&item.job_id===jobId;});if(!job||!job.job_id){alert('Job nao encontrado no historico.');return;}if(jobSource){try{jobSource.close();}catch(e){}jobSource=null;}if(_pollTimer){clearTimeout(_pollTimer);_pollTimer=null;}activeJob=job;selectedFileCount=job.selected_file_count||selectedFileCount||0;setSelectionHint((job.selected_file_count||0)+' arquivos encontrados');updateJobBadge(job,job.selected_file_count||selectedFileCount);fetch('/dump/log?job_id='+encodeURIComponent(job.job_id),{cache:'no-store'}).then(function(r){return r.ok?r.text():'';}).then(function(text){appendJobLog((text||'').split(/\r?\n/).filter(function(line){return line!=='';}),true);_setLiveLogLine(job.live_log_base,job.live_log_line);closeHistoryModal();if(job.is_active){connectJobStream(job.job_id,job.log_size||new Blob([text||'']).size||0);}}).catch(function(err){alert(String(err));});}
"""

def simulate_dump(db_path):
    cmd = f"echo 'Simulando dump do banco {db_path}'"
    return run_command(cmd)


def list_matching_files(base_path, mask):
    if not base_path:
        return {"files": [], "error": "Informe o path do banco para DUMP."}

    resolved_base_path = base_path if os.path.isdir(base_path) else os.path.join(HOST_ROOT_MOUNT, base_path.lstrip("/"))

    if not os.path.isdir(resolved_base_path):
        return {"files": [], "error": f"Diretório não encontrado: {base_path}"}

    pattern = os.path.join(resolved_base_path, mask or "*.db")
    files = [path for path in sorted(glob.glob(pattern)) if os.path.isfile(path)]

    return {"files": files, "error": ""}


def normalize_config(config):
    return {
        "progress": {
            "dlc": config.get("progress", {}).get("dlc", ""),
            "proenv": config.get("progress", {}).get("proenv", ""),
        },
        "databases": [
            {
                "dump_db_path": db.get("dump_db_path", ""),
                "db_mask": db.get("db_mask", ""),
                "dump_path": db.get("dump_path", ""),
                "load_path": db.get("load_path", ""),
            }
            for db in config.get("databases", [])
        ],
        "dump": {
            "output_dir": config.get("dump", {}).get("output_dir", ""),
            "threads_per_db": config.get("dump", {}).get("threads_per_db", 1),
        },
        "execution": {
            "max_parallel_dbs": config.get("execution", {}).get("max_parallel_dbs", 1),
        },
    }


def build_config_form(config, message=""):
    databases = config["databases"] or [{"dump_db_path": "", "db_mask": "", "dump_path": "", "load_path": ""}]
    db_rows = []

    for index, db in enumerate(databases):
        db_rows.append(
            "<tr>"
            f"<td><input class='dump-path-input' name='databases_dump_db_path' value='{escape(db.get('dump_db_path', ''))}'></td>"
            "<td>"
            f"<div class='mask-with-explorer'><input class='mask-input' name='databases_db_mask' value='{escape(db.get('db_mask', ''))}'><button type='button' class='icon-button' title='Explorar arquivos' onclick='openExplorer(this)'>⌕</button></div>"
            "</td>"
            f"<td><input name='databases_dump_path' value='{escape(db.get('dump_path', ''))}'></td>"
            f"<td><input name='databases_load_path' value='{escape(db.get('load_path', ''))}'></td>"
            f"<td><button type='button' onclick='removeRow(this)'>Remover</button></td>"
            "</tr>"
        )

    notice = f"<div class='notice'>{escape(message)}</div>" if message else ""

    return (
        "<!doctype html>"
        "<html lang='pt-BR'>"
        "<head>"
        "<meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<title>Configuração do console</title>"
        "<style>"
        "body{font-family:Arial,sans-serif;margin:32px;background:#f6f7fb;color:#1f2937}"
        ".page-header{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:8px}"
        ".page-actions{display:flex;align-items:center;gap:10px}"
        ".icon-link{display:inline-flex;align-items:center;justify-content:center;height:40px;min-width:40px;padding:0 12px;border-radius:10px;background:#111827;color:#fff !important;text-decoration:none;font-weight:700;line-height:1}"
        ".icon-link.symbol{font-size:20px;padding:0;width:40px}"
        "h1{margin:0 0 8px}"
        "p{margin:0 0 18px;color:#4b5563}"
        ".card{background:#fff;border-radius:14px;box-shadow:0 8px 24px rgba(15,23,42,.08);padding:20px;margin-bottom:18px}"
        "label{display:block;font-weight:700;margin-bottom:6px}"
        "input{width:100%;padding:10px 12px;border:1px solid #d1d5db;border-radius:10px;box-sizing:border-box}"
        ".mask-with-explorer{display:flex;gap:8px;align-items:center}"
        ".mask-with-explorer input{flex:1}"
        ".icon-button{width:40px;min-width:40px;height:40px;padding:0;border-radius:10px;border:1px solid #d1d5db;background:#fff;color:#111827;cursor:pointer;font-size:18px;line-height:1}"
        "table{width:100%;border-collapse:collapse}"
        "th,td{padding:10px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top}"
        "th{text-align:left;background:#f9fafb}"
        ".grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}"
        ".actions{display:flex;gap:12px;margin-top:18px;align-items:center}"
        "button{padding:10px 14px;border:0;border-radius:10px;cursor:pointer;background:#111827;color:#fff}"
        ".secondary{background:#374151}"
        ".notice{background:#ecfeff;color:#155e75;border:1px solid #a5f3fc;padding:12px 14px;border-radius:10px;margin-bottom:18px}"
        ".danger{background:#b91c1c}"
        ".modal{display:none;position:fixed;inset:0;background:rgba(15,23,42,.55);align-items:center;justify-content:center;padding:20px;z-index:50}"
        ".modal.open{display:flex}"
        ".modal-card{width:min(760px,100%);max-height:80vh;overflow:auto;background:#fff;border-radius:16px;box-shadow:0 24px 60px rgba(0,0,0,.22);padding:20px}"
        ".modal-header{display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:12px}"
        ".modal-title{font-size:18px;font-weight:700}"
        ".close-button{background:#374151}"
        ".file-list{list-style:none;margin:0;padding:0;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden}"
        ".file-list li{padding:10px 12px;border-bottom:1px solid #e5e7eb;font-family:monospace;word-break:break-all}"
        ".file-list li:last-child{border-bottom:0}"
        ".error-box{background:#fef2f2;color:#991b1b;border:1px solid #fecaca;padding:12px 14px;border-radius:10px}"
        "</style>"
        "</head>"
        "<body>"
        "<div class='page-header'><h1>Configuração do console</h1><div class='page-actions'><a class='icon-link' href='/'>Inicio</a></div></div>"
        "<p>Edite as regras de descoberta, paths e salve o arquivo YAML usado pelo console.</p>"
        f"{notice}"
        "<form method='post' action='/config/save'>"
        "<div class='card'><div class='grid'>"
        f"<div><label>Path do DLC</label><input name='progress_dlc' value='{escape(config['progress'].get('dlc', ''))}'></div>"
        f"<div><label>Path do proenv</label><input name='progress_proenv' value='{escape(config['progress'].get('proenv', ''))}'></div>"
        f"<div><label>Diretório do dump</label><input name='dump_output_dir' value='{escape(config['dump'].get('output_dir', ''))}'></div>"
        f"<div><label>Threads por banco(no dump)</label><input name='dump_threads_per_db' type='number' min='1' value='{escape(str(config['dump'].get('threads_per_db', 1)))}'></div>"
        f"<div><label>Paralelismo de bancos(no load)</label><input name='execution_max_parallel_dbs' type='number' min='1' value='{escape(str(config['execution'].get('max_parallel_dbs', 1)))}'></div>"
        "</div></div>"
        "<div class='card'><h2>Regras de descoberta</h2>"
        "<table id='db-table'><thead><tr><th>Path do banco para DUMP</th><th>Máscara de bancos</th><th>Path do Dump</th><th>Path do Load</th><th>Ação</th></tr></thead><tbody>"
        + "".join(db_rows)
        + "</tbody></table>"
        "</div>"
        "<div class='actions'><button type='submit'>Salvar YAML</button></div>"
        "</form>"
        "<div id='explorer-modal' class='modal' onclick='closeExplorer(event)'>"
        "<div class='modal-card' onclick='event.stopPropagation()'>"
        "<div class='modal-header'><div class='modal-title'>Arquivos encontrados</div><button type='button' class='close-button' onclick='closeExplorer()'>Fechar</button></div>"
        "<div id='explorer-meta' style='margin-bottom:12px;color:#4b5563'></div>"
        "<div id='explorer-content'></div>"
        "</div>"
        "</div>"
        "<script>"
        "function removeRow(button){const row=button.closest('tr');if(row&&row.parentNode){row.parentNode.removeChild(row);}}"
        "function openExplorer(button){const row=button.closest('tr');const base=row.querySelector('.dump-path-input').value.trim();const mask=row.querySelector('.mask-input').value.trim()||'*.db';const modal=document.getElementById('explorer-modal');const meta=document.getElementById('explorer-meta');const content=document.getElementById('explorer-content');meta.textContent='Carregando '+base+' / '+mask+'...';content.innerHTML='';modal.classList.add('open');fetch('/config/explore?path='+encodeURIComponent(base)+'&mask='+encodeURIComponent(mask)).then(r=>r.json()).then(data=>{meta.textContent=(data.path||base)+' / '+(data.mask||mask);if(data.error){content.innerHTML=\"<div class='error-box'>\"+escapeHtml(data.error)+\"</div>\";return;}if(!data.files.length){content.innerHTML=\"<div class='error-box'>Nenhum arquivo encontrado.</div>\";return;}content.innerHTML=\"<ul class='file-list'>\"+data.files.map(file=>\"<li>\"+escapeHtml(file)+\"</li>\").join('')+\"</ul>\";}).catch(err=>{content.innerHTML=\"<div class='error-box'>\"+escapeHtml(String(err))+\"</div>\";});}"
        "function closeExplorer(event){if(event&&event.target&&event.target.id!=='explorer-modal'){return;}document.getElementById('explorer-modal').classList.remove('open');}"
        "function escapeHtml(text){const div=document.createElement('div');div.textContent=String(text);return div.innerHTML;}"
        "</script>"
        "</body></html>"
    )


def build_home_page_button_row():
    return (
        "<div class='hero-side'>"
        "<button type='button' class='icon-link secondary' onclick='openHistoryModal()' title='Historico de jobs' aria-label='Historico de jobs'>Historico</button>"
        "<a class='icon-link secondary' href='/catalogo-comandos' title='Editar catálogo Dump' aria-label='Editar catálogo Dump'>Editar catalogo Dump</a>"
        "<a class='icon-link secondary' href='/catalogo-comandos-load' title='Editar catálogo Load' aria-label='Editar catálogo Load'>Editar catalogo Load</a>"
        "<a class='icon-link symbol' href='/config' title='Configuração' aria-label='Configuração'>⚙</a>"
        "</div>"
    )


def parse_config_form(body_bytes):
    form = parse_qs(body_bytes.decode("utf-8"), keep_blank_values=True)
    dump_db_paths = form.get("databases_dump_db_path", [])
    db_masks = form.get("databases_db_mask", [])
    dump_paths = form.get("databases_dump_path", [])
    load_paths = form.get("databases_load_path", [])
    databases = []

    for index in range(max(len(dump_db_paths), len(db_masks), len(dump_paths), len(load_paths))):
        dump_db_path = dump_db_paths[index].strip() if index < len(dump_db_paths) else ""
        db_mask = db_masks[index].strip() if index < len(db_masks) else ""
        dump_path = dump_paths[index].strip() if index < len(dump_paths) else ""
        load_path = load_paths[index].strip() if index < len(load_paths) else ""
        if dump_db_path or dump_path or load_path:
            databases.append({
                "dump_db_path": dump_db_path,
                "db_mask": db_mask,
                "dump_path": dump_path,
                "load_path": load_path,
            })

    return {
        "progress": {
            "dlc": form.get("progress_dlc", [""])[0].strip(),
            "proenv": form.get("progress_proenv", [""])[0].strip(),
        },
        "databases": databases,
        "dump": {
            "output_dir": form.get("dump_output_dir", [""])[0].strip(),
            "threads_per_db": int(form.get("dump_threads_per_db", ["1"])[0] or 1),
        },
        "execution": {
            "max_parallel_dbs": int(form.get("execution_max_parallel_dbs", ["1"])[0] or 1),
        },
    }


def build_status_report():
    config = load_config(CONFIG_PATH)
    databases = []

    for db in config["databases"]:
        status = is_db_online(db["dump_db_path"])

        databases.append({
            "dump_db_path": db["dump_db_path"],
            "db_mask": db.get("db_mask", ""),
            "dump_path": db.get("dump_path", ""),
            "load_path": db.get("load_path", ""),
            "online": status,
        })

    return {
        "databases": databases,
    }


def render_html(report):
    database_entries = []

    for db in report["databases"]:
        database_entries.append({
            "path": db["dump_db_path"],
            "mask": db.get("db_mask", ""),
            "dumpPath": db.get("dump_path", ""),
            "loadPath": db.get("load_path", ""),
            "checked": False,
        })

    database_entries_script = (
        "window.__DATABASE_ENTRIES__ = "
        + json.dumps(database_entries, ensure_ascii=False).replace("</", "<\\/")
        + ";"
    )

    return (
        "<!doctype html>"
        "<html lang='pt-BR'>"
        "<head>"
        "<meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<meta name='color-scheme' content='light'>"
        "<title>MIR - Tecnologia com eficiência</title>"
        "<style>"
        "*{box-sizing:border-box}"
        "body{margin:0;min-height:100vh;font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e5eef7;background:radial-gradient(circle at top left,#16324f 0%,#0b1220 40%,#050816 100%)}"
        "body:before{content:'';position:fixed;inset:0;background-image:linear-gradient(rgba(255,255,255,.03) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.03) 1px,transparent 1px);background-size:48px 48px;pointer-events:none;opacity:.32}"
        ".shell{position:relative;z-index:1;max-width:1440px;margin:0 auto;padding:22px}"
        ".hero{position:relative;overflow:hidden;border:1px solid rgba(148,163,184,.2);border-radius:22px;padding:18px 20px 16px;background:linear-gradient(135deg,rgba(15,23,42,.94),rgba(30,41,59,.85));box-shadow:0 18px 52px rgba(0,0,0,.38)}"
        ".hero:before,.hero:after{content:'';position:absolute;border-radius:999px;filter:blur(12px);opacity:.65;pointer-events:none}"
        ".hero:before{width:220px;height:220px;right:-70px;top:-70px;background:radial-gradient(circle,rgba(56,189,248,.42) 0%,rgba(56,189,248,0) 70%)}"
        ".hero:after{width:180px;height:180px;left:-40px;bottom:-50px;background:radial-gradient(circle,rgba(244,114,182,.28) 0%,rgba(244,114,182,0) 70%)}"
        ".hero-top{display:flex;align-items:flex-start;justify-content:space-between;gap:14px;position:relative;z-index:1}"
        ".eyebrow{display:inline-flex;align-items:center;gap:6px;padding:6px 10px;border-radius:999px;background:rgba(255,255,255,.08);color:#cbd5e1;font-size:10px;letter-spacing:.1em;text-transform:uppercase}"
        ".hero h1{margin:10px 0 4px;font-size:clamp(22px,3vw,34px);line-height:1.04;letter-spacing:-.03em;color:#f8fafc}"
        ".hero p{margin:0;max-width:720px;font-size:17px;line-height:1.6;color:#cbd5e1}"
        ".hero-side{display:flex;align-items:center;gap:8px}"
        ".icon-link{display:inline-flex;align-items:center;justify-content:center;height:36px;min-width:36px;padding:0 12px;border-radius:11px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.12);color:#fff !important;text-decoration:none;font-weight:700;font-size:12px;backdrop-filter:blur(8px)}"
        ".icon-link.symbol{font-size:16px;padding:0;width:36px}"
        ".content{margin-top:16px;display:grid;grid-template-columns:minmax(0,1fr);gap:20px}"
        ".panel{background:rgba(255,255,255,.94);border:1px solid rgba(148,163,184,.18);border-radius:24px;box-shadow:0 24px 60px rgba(2,6,23,.18);overflow:hidden;color:#0f172a}"
        ".panel-head{display:flex;justify-content:space-between;align-items:center;gap:16px;padding:18px 20px 0}"
        ".panel-title{font-size:18px;font-weight:800;letter-spacing:-.02em}"
        ".panel-subtitle{margin-top:4px;color:#475569;font-size:14px}"
        ".panel-toolbar{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:18px 20px 0;flex-wrap:wrap}"
        ".toolbar-group{display:flex;align-items:center;gap:12px;flex-wrap:wrap}"
        ".job-button{background:linear-gradient(135deg,#2563eb,#1d4ed8);color:#fff;border:0;border-radius:14px;padding:12px 16px;font-weight:800;cursor:pointer;box-shadow:0 12px 28px rgba(37,99,235,.28)}"
        ".job-button:disabled{opacity:.55;cursor:not-allowed}"
        ".selection-hint{color:#475569;font-size:13px}"
        ".mode-toggle{display:inline-flex;align-items:center;gap:8px;padding:10px 12px;border:1px solid rgba(148,163,184,.24);border-radius:12px;background:rgba(255,255,255,.68);color:#0f172a;font-size:13px;font-weight:700}"
        ".mode-toggle input{width:16px;height:16px;margin:0}"
        ".mode-state{font-size:12px;font-weight:800;color:#1d4ed8;text-transform:uppercase;letter-spacing:.06em}"
        ".env-placeholder{padding:18px 20px 22px;color:#64748b;font-size:14px}"
        ".mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;font-size:13px;word-break:break-all}"
        ".mask-with-explorer{display:flex;align-items:center;gap:8px}"
        ".mask-with-explorer input{flex:1;min-width:0}"
        ".mask-readonly{background:#fff;color:#111827;padding:10px 12px;border:1px solid #cbd5e1;border-radius:12px}"
        ".icon-button{width:42px;min-width:42px;height:42px;padding:0;border-radius:12px;border:1px solid #cbd5e1;background:linear-gradient(180deg,#fff,#f8fafc);color:#0f172a;cursor:pointer;font-size:18px;line-height:1;box-shadow:0 4px 14px rgba(15,23,42,.08)}"
        ".footer-note{padding:0 20px 20px;color:#64748b;font-size:13px}"
        ".modal{display:none;position:fixed;inset:0;background:rgba(2,6,23,.86);backdrop-filter:blur(10px);align-items:center;justify-content:center;padding:20px;z-index:50}"
        ".modal.open{display:flex}"
        ".modal-card{width:min(840px,100%);max-height:80vh;overflow:auto;background:linear-gradient(180deg,#0f172a,#111827);border-radius:22px;box-shadow:0 32px 80px rgba(0,0,0,.5);padding:20px;border:1px solid rgba(148,163,184,.28);color:#e5eef7}"
        ".modal-header{display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:12px}"
        ".modal-title{font-size:18px;font-weight:800;color:#f8fafc}"
        ".modal-subtitle{margin-top:4px;color:#cbd5e1;font-size:13px}"
        ".close-button{background:#f8fafc;color:#0f172a;border:0;border-radius:12px;padding:10px 14px;font-weight:800}"
        ".close-button:hover{background:#e2e8f0}"
        ".modal-actions{display:flex;justify-content:flex-end;gap:10px;margin-top:16px}"
        ".modal-primary{background:linear-gradient(135deg,#2563eb,#1d4ed8);color:#fff;border:0;border-radius:12px;padding:12px 16px;font-weight:800;cursor:pointer}"
        ".modal-secondary{background:#334155;color:#f8fafc;border:0;border-radius:12px;padding:12px 16px;font-weight:800;cursor:pointer}"
        ".start-modal-list{display:flex;flex-direction:column;gap:10px}"
        ".start-modal-item{display:flex;align-items:flex-start;gap:12px;padding:14px;border:1px solid rgba(148,163,184,.2);border-radius:16px;background:rgba(15,23,42,.72);cursor:pointer}"
        ".start-modal-item input{margin-top:4px;width:18px;height:18px;flex:0 0 auto}"
        ".start-modal-item strong{display:block;color:#f8fafc;font-size:14px;word-break:break-all}"
        ".start-modal-item small{display:block;margin-top:4px;color:#cbd5e1;font-size:12px;line-height:1.4;word-break:break-all}"
        ".start-modal-loading,.start-bank-empty{padding:12px 14px;border-radius:12px;background:rgba(255,255,255,.06);color:#cbd5e1;font-size:13px}"
        ".start-bank-files{margin:10px 0 0;padding-left:18px;color:#e2e8f0;font-size:12px;line-height:1.45;word-break:break-all}"
        ".history-list{display:flex;flex-direction:column;gap:10px}"
        ".history-item{display:flex;flex-direction:column;gap:8px;align-items:flex-start;width:100%;padding:14px;border:1px solid rgba(148,163,184,.2);border-radius:16px;background:rgba(15,23,42,.72);cursor:pointer;color:#f8fafc;text-align:left}"
        ".history-item-top{display:flex;justify-content:space-between;align-items:center;gap:10px;width:100%}"
        ".history-item-tags{display:flex;gap:8px;flex-wrap:wrap}"
        ".history-chip{display:inline-flex;align-items:center;padding:4px 8px;border-radius:999px;background:rgba(255,255,255,.08);border:1px solid rgba(148,163,184,.22);font-size:11px;font-weight:700;color:#cbd5e1;text-transform:uppercase}"
        ".history-chip.active{background:#1d4ed8;color:#eff6ff;border-color:#60a5fa}"
        ".history-item-meta{font-size:12px;color:#cbd5e1}"
        ".history-item-preview{font-size:12px;color:#e2e8f0;line-height:1.5}"
        ".explorer-meta{margin-bottom:12px;color:#cbd5e1;font-size:13px}"
        ".file-list{list-style:none;margin:0;padding:0;border:1px solid rgba(148,163,184,.28);border-radius:14px;overflow:hidden;background:#0b1220}"
        ".file-list li{padding:12px 14px;border-bottom:1px solid rgba(148,163,184,.18);font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;word-break:break-all;background:rgba(15,23,42,.72);color:#f8fafc}"
        ".file-list li:nth-child(even){background:rgba(30,41,59,.82)}"
        ".file-list li:last-child{border-bottom:0}"
        ".error-box{background:#450a0a;color:#fee2e2;border:1px solid #fca5a5;padding:12px 14px;border-radius:12px}"
        ".job-panel{margin-top:20px;background:#0f172a;border:1px solid rgba(148,163,184,.24);border-radius:24px;box-shadow:0 24px 60px rgba(2,6,23,.18);overflow:hidden;display:flex;flex-direction:column}"
        ".job-alert{margin:14px 20px 0;padding:12px 14px;border-radius:14px;border:1px solid transparent;font-weight:700}"
        ".job-alert.hidden{display:none}"
        ".job-alert.failed{background:#450a0a;border-color:#fca5a5;color:#fee2e2}"
        ".job-alert.warning{background:#78350f;border-color:#fdba74;color:#ffedd5}"
        ".job-alert.success{background:#064e3b;border-color:#6ee7b7;color:#d1fae5}"
        ".job-meta{display:flex;gap:10px;flex-wrap:wrap;padding:14px 20px 0}"
        ".job-meta-item{background:rgba(255,255,255,.06);border:1px solid rgba(148,163,184,.18);border-radius:16px;padding:10px 12px;color:#e5eef7;min-width:160px}"
        ".job-meta-item span{display:block;font-size:11px;text-transform:uppercase;letter-spacing:.08em;color:#94a3b8}"
        ".job-meta-item strong{display:block;margin-top:6px;font-size:16px;color:#f8fafc}"
        ".job-log{margin:18px 20px 20px;background:#050816;border:1px solid rgba(148,163,184,.18);border-radius:18px;padding:16px;height:360px;max-height:360px;overflow-y:scroll;overflow-x:auto;scrollbar-gutter:stable;color:#dbeafe;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;white-space:pre;word-break:normal;overflow-wrap:normal;line-height:1.5;flex:0 0 auto}"
        "@media (max-width: 980px){.hero-top{flex-direction:column}.hero-side{width:100%;justify-content:flex-start}}"
        "@media (max-width: 640px){.shell{padding:14px}.panel-head{flex-direction:column;align-items:flex-start}}"
        "</style>"
        "</head>"
        "<body>"
        "<div class='shell'>"
        "<section class='hero'>"
        "<div class='hero-top'>"
        "<div>"
        "<div class='eyebrow'>OpenEdge dump / load console</div>"
        "<h1>MIR - Tecnologia com eficiência</h1>"
        "</div>"
        f"{build_home_page_button_row()}"
        "</div>"
        "</section>"
        "<div class='content'>"
        "<section class='panel'>"
        "<div class='panel-head'>"
        "<div><div class='panel-title'>Ambientes</div></div>"
        "</div>"
        "<div class='panel-toolbar'><div class='toolbar-group'><button type='button' class='job-button' id='start-dump-button' onclick='startSelectedDump()'>Executar dump selecionados</button><button type='button' class='job-button' id='start-load-button' onclick='startSelectedLoad()'>Executar load selecionados</button><label class='mode-toggle'><input type='checkbox' id='simulation-mode'><span>Simulação</span><span class='mode-state' id='mode-state'></span></label><span class='selection-hint' id='selection-hint'>0 bancos selecionados</span></div></div>"
        "<section class='job-panel'>"
        "<div id='job-alert' class='job-alert hidden'></div>"
        "<div class='job-meta' id='job-meta'></div>"
        "<pre class='job-log' id='job-log'>Nenhum job foi iniciado ainda.</pre>"
        "</section>"
        "</section>"
        "</div>"
        "<div id='start-dump-modal' class='modal' onclick='closeStartDumpModal(event)'>"
        "<div class='modal-card' onclick='event.stopPropagation()'>"
        "<div class='modal-header'><div><div class='modal-title'>Bancos encontrados para o dump</div><div class='modal-subtitle' id='start-modal-meta'></div></div><button type='button' class='close-button' onclick='closeStartDumpModal()'>Fechar</button></div>"
        "<div id='start-modal-content'></div>"
        "<div class='modal-actions'><button type='button' class='modal-secondary' onclick='closeStartDumpModal()'>Cancelar</button><button type='button' class='modal-primary' onclick='confirmStartDumpModal()'>Executar apenas os selecionados</button></div>"
        "</div>"
        "</div>"
        "<div id='start-load-modal' class='modal' onclick='closeStartLoadModal(event)'>"
        "<div class='modal-card' onclick='event.stopPropagation()'>"
        "<div class='modal-header'><div><div class='modal-title'>Bancos encontrados para o load</div><div class='modal-subtitle' id='start-load-modal-meta'></div></div><button type='button' class='close-button' onclick='closeStartLoadModal()'>Fechar</button></div>"
        "<div id='start-load-modal-content'></div>"
        "<div class='modal-actions'><button type='button' class='modal-secondary' onclick='closeStartLoadModal()'>Cancelar</button><button type='button' class='modal-primary' onclick='confirmStartLoadModal()'>Executar apenas os selecionados</button></div>"
        "</div>"
        "</div>"
        "<div id='history-modal' class='modal' onclick='closeHistoryModal(event)'>"
        "<div class='modal-card' onclick='event.stopPropagation()'>"
        "<div class='modal-header'><div><div class='modal-title'>Historico de jobs</div><div class='modal-subtitle' id='history-meta'></div></div><button type='button' class='close-button' onclick='closeHistoryModal()'>Fechar</button></div>"
        "<div id='history-content'></div>"
        "</div>"
        "</div>"
        "<div id='explorer-modal' class='modal' onclick='closeExplorer(event)'>"
        "<div class='modal-card' onclick='event.stopPropagation()'>"
        "<div class='modal-header'><div class='modal-title'>Arquivos encontrados</div><button type='button' class='close-button' onclick='closeExplorer()'>Fechar</button></div>"
        "<div id='explorer-meta' class='explorer-meta'></div>"
        "<div id='explorer-content'></div>"
        "</div>"
        "</div>"
        "<script>"
        + database_entries_script
        + DUMP_UI_SCRIPT +
        DUMP_UI_FALLBACK_SCRIPT +
        HISTORY_UI_SCRIPT +
        "</script>"
        "</div></body></html>"
    )


def serve(port):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed_url = urlparse(self.path)

            if parsed_url.path == "/dump/current":
                job = get_current_job_summary()
                if not job:
                    self.send_response(204)
                    self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                    self.send_header("Pragma", "no-cache")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return

                body = json.dumps(job, ensure_ascii=False, indent=2).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if parsed_url.path == "/jobs/history":
                body = json.dumps(list_job_history(limit=50, tail_limit=5), ensure_ascii=False, indent=2).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if parsed_url.path in ("/catalogo-comandos", "/comandos"):
                catalog = load_catalog(CATALOG_PATH)
                message = parse_qs(parsed_url.query).get("message", [""])[0]
                html = build_catalog_page(catalog, message).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Content-Length", str(len(html)))
                self.end_headers()
                self.wfile.write(html)
                return

            if parsed_url.path in ("/catalogo-comandos-load", "/comandos-load"):
                catalog = load_catalog(LOAD_CATALOG_PATH)
                message = parse_qs(parsed_url.query).get("message", [""])[0]
                html = build_catalog_page(
                    catalog,
                    message,
                    page_title="Catálogo de comandos do load",
                    save_path="/catalogo-comandos-load/save",
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Content-Length", str(len(html)))
                self.end_headers()
                self.wfile.write(html)
                return

            if parsed_url.path == "/dump/log":
                query = parse_qs(parsed_url.query)
                job_id = query.get("job_id", [""])[0]
                offset_str = query.get("offset", [""])[0]

                if offset_str:
                    # Incremental mode: return chunk from offset
                    offset = int(offset_str)
                    chunk, new_offset = get_job_log_chunk(job_id, offset)
                    if chunk is None:
                        self.send_response(404)
                        self.end_headers()
                        return

                    body = chunk.encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                    self.send_header("Pragma", "no-cache")
                    self.send_header("X-Log-Offset", str(new_offset))
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                # Full log mode (legacy/initial load)
                log_text = get_job_log(job_id)
                if log_text is None:
                    self.send_response(404)
                    self.end_headers()
                    return

                body = log_text.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("X-Log-Offset", str(len(body)))
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if parsed_url.path.startswith("/dump/") and parsed_url.path.endswith("/events"):
                parts = [part for part in parsed_url.path.split("/") if part]
                if len(parts) != 3:
                    self.send_response(404)
                    self.end_headers()
                    return

                job_id = parts[1]
                query = parse_qs(parsed_url.query)
                offset = int(query.get("offset", ["0"])[0] or 0)
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.end_headers()

                def send_event(event_name, payload):
                    message = json.dumps(payload, ensure_ascii=False)
                    self.wfile.write(f"event: {event_name}\n".encode("utf-8"))
                    self.wfile.write(f"data: {message}\n\n".encode("utf-8"))
                    self.wfile.flush()

                try:
                    while True:
                        job = get_job_summary(job_id)
                        if not job:
                            break

                        log_path = job.get("log_path")
                        if log_path and os.path.exists(log_path):
                            with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
                                handle.seek(offset)
                                chunk = handle.read()
                                offset = handle.tell()

                            if chunk:
                                for line in chunk.splitlines():
                                    send_event("log", {"job_id": job_id, "line": line})

                        send_event("state", job)

                        if job.get("status") not in ("queued", "running"):
                            if log_path and os.path.exists(log_path):
                                stable_size = None
                                stable_rounds = 0
                                for _ in range(60):
                                    try:
                                        current_size = os.path.getsize(log_path)
                                    except OSError:
                                        current_size = -1

                                    if current_size == stable_size:
                                        stable_rounds += 1
                                        if stable_rounds >= 3:
                                            break
                                    else:
                                        stable_size = current_size
                                        stable_rounds = 0

                                    time.sleep(0.25)

                                with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
                                    for line in handle.read().splitlines():
                                        if line.strip():
                                            send_event("log", {"job_id": job_id, "line": line})
                            send_event("done", job)
                            break

                        time.sleep(1)
                except (BrokenPipeError, ConnectionResetError):
                    return
                return

            if self.path.startswith("/config/explore"):
                query = parse_qs(urlparse(self.path).query)
                base_path = query.get("path", [""])[0]
                mask = query.get("mask", [""])[0]
                payload = {
                    "path": base_path,
                    "mask": mask,
                }
                payload.update(list_matching_files(base_path, mask))
                body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if self.path == "/json":
                report = build_status_report()
                payload = json.dumps(report, indent=2, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if self.path == "/favicon.ico":
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            if self.path in ("/config", "/config/"):
                config = normalize_config(load_config(CONFIG_PATH))
                html = build_config_form(config).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html)))
                self.end_headers()
                self.wfile.write(html)
                return

            if self.path != "/":
                self.send_response(404)
                self.end_headers()
                return

            report = build_status_report()

            html = render_html(report).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)

        def do_POST(self):
            parsed_url = urlparse(self.path)

            if parsed_url.path == "/dump/start":
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                try:
                    payload = json.loads(body.decode("utf-8") or "{}")
                except json.JSONDecodeError:
                    payload = {}

                selected_payload = payload.get("databases", [])
                config = normalize_config(load_config(CONFIG_PATH))

                if selected_payload and isinstance(selected_payload[0], dict):
                    selected_databases = [
                        {
                            "dump_db_path": db.get("dump_db_path", ""),
                            "db_mask": db.get("db_mask", ""),
                            "dump_path": db.get("dump_path", ""),
                            "load_path": db.get("load_path", ""),
                            "selected_file_path": db.get("selected_file_path", ""),
                        }
                        for db in selected_payload
                        if db.get("dump_db_path") and db.get("db_mask")
                    ]
                else:
                    selected_paths = [path for path in selected_payload if isinstance(path, str)]
                    selected_databases = [db for db in config["databases"] if db.get("dump_db_path") in selected_paths]

                mode = str(payload.get("mode", "real") or "real").strip().lower()
                if mode == "dry":
                    job = start_dry_run_job(selected_databases, config, load_catalog(CATALOG_PATH))
                else:
                    job = start_dump_job(selected_databases, config, load_catalog(CATALOG_PATH), mode="real")

                if not job:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    body = json.dumps({"error": "Selecione ao menos um banco válido para o dry-run."}, ensure_ascii=False).encode("utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                body = json.dumps(job, ensure_ascii=False, indent=2).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if parsed_url.path == "/load/start":
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                try:
                    payload = json.loads(body.decode("utf-8") or "{}")
                except json.JSONDecodeError:
                    payload = {}

                selected_payload = payload.get("databases", [])
                config = normalize_config(load_config(CONFIG_PATH))

                if selected_payload and isinstance(selected_payload[0], dict):
                    selected_databases = [
                        {
                            "dump_db_path": db.get("dump_db_path", ""),
                            "db_mask": db.get("db_mask", ""),
                            "dump_path": db.get("dump_path", ""),
                            "load_path": db.get("load_path", ""),
                            "selected_file_path": db.get("selected_file_path", ""),
                        }
                        for db in selected_payload
                        if db.get("dump_db_path") and db.get("db_mask")
                    ]
                else:
                    selected_paths = [path for path in selected_payload if isinstance(path, str)]
                    selected_databases = [db for db in config["databases"] if db.get("dump_db_path") in selected_paths]

                mode = str(payload.get("mode", "real") or "real").strip().lower()
                if mode == "dry":
                    job = start_dry_run_job(selected_databases, config, load_catalog(LOAD_CATALOG_PATH))
                else:
                    job = start_dump_job(selected_databases, config, load_catalog(LOAD_CATALOG_PATH), mode="real")

                if not job:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    body = json.dumps({"error": "Selecione ao menos um banco válido para o load."}, ensure_ascii=False).encode("utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                body = json.dumps(job, ensure_ascii=False, indent=2).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if parsed_url.path in ("/catalogo-comandos/save", "/comandos/save"):
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                catalog = parse_catalog_form(body)
                save_catalog(catalog, CATALOG_PATH)

                self.send_response(303)
                self.send_header("Location", f"/catalogo-comandos?message={quote('Catálogo salvo com sucesso.')}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            if parsed_url.path in ("/catalogo-comandos-load/save", "/comandos-load/save"):
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                catalog = parse_catalog_form(body)
                save_catalog(catalog, LOAD_CATALOG_PATH)

                self.send_response(303)
                self.send_header("Location", f"/catalogo-comandos-load?message={quote('Catálogo salvo com sucesso.')}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            if self.path != "/config/save":
                self.send_response(404)
                self.end_headers()
                return

            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            config = parse_config_form(body)
            save_config(config, CONFIG_PATH)

            html = build_config_form(normalize_config(config), "Configuração salva com sucesso.").encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Servidor ativo em http://0.0.0.0:{port}")
    print("Abra / no navegador ou /json para a resposta estruturada.")
    server.serve_forever()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--serve", action="store_true", help="Exibe o status em um servidor HTTP")
    parser.add_argument("--port", type=int, default=8000, help="Porta do servidor HTTP")
    args = parser.parse_args()

    if args.serve:
        serve(args.port)
        return

    config = load_config(CONFIG_PATH)

    for db in config["databases"]:
        status = is_db_online(db["dump_db_path"])
        print(f"{db['dump_db_path']} -> {'ONLINE' if status else 'OFFLINE'}")

        if not status:
            print(f"Pulando dump de {db['dump_db_path']} porque o banco está offline.")
            continue

        simulate_dump(db["dump_db_path"])

if __name__ == "__main__":
    main()
