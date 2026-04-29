import unittest
import tempfile
from pathlib import Path
from unittest.mock import mock_open
from unittest.mock import patch

import app.catalog as catalog_module
import app.jobs as jobs


class JobsRegressionTests(unittest.TestCase):
    def test_get_current_job_summary_ignores_last_job_when_no_active_job(self):
        with patch.object(
            jobs,
            "_index_data",
            return_value={"active_job_id": None, "last_job_id": "old-job"},
        ), patch.object(
            jobs,
            "get_job_summary",
            side_effect=AssertionError("get_job_summary should not be called when there is no active job"),
        ):
            self.assertIsNone(jobs.get_current_job_summary())

    def test_run_dry_job_emits_step_two_and_continues_after_busy(self):
        logs = []
        state = {
            "job_id": "job-1",
            "status": "queued",
            "mode": "dry",
            "message": "Fila criada para dry-run",
            "created_at": "2026-04-28T00:00:00+00:00",
            "started_at": None,
            "finished_at": None,
            "updated_at": "2026-04-28T00:00:00+00:00",
            "items": [
                {
                    "dump_db_path": "/totvs/database/CON",
                    "db_mask": "*.db",
                    "dump_path": "/totvs/database/dump/CON",
                    "load_path": "/totvs/database/load/CON",
                    "status": "pending",
                    "progress": 0,
                    "message": "Aguardando início",
                }
            ],
            "log_path": "/tmp/job-1.log",
            "total_dbs": 1,
            "completed_dbs": 0,
            "running_dbs": 0,
            "offline_dbs": 0,
            "failed_dbs": 0,
            "overall_progress": 0,
            "active_dbs": [],
            "selected_count": 1,
            "selected_file_count": 1,
        }
        catalog = {
            "catalog": [
                {
                    "step": 1,
                    "title": "Verificar banco origem",
                    "kind": "command",
                    "enabled": True,
                    "command": '{dlc_bin}/proutil "{db_path}" -C busy',
                    "loop_source_path": "",
                    "loop_source_file": "",
                    "description": "",
                },
                {
                    "step": 2,
                    "title": "Garantir destino do dump",
                    "kind": "command",
                    "enabled": True,
                    "command": 'mkdir -p "{dump_path}/{db_name}"',
                    "loop_source_path": "",
                    "loop_source_file": "",
                    "description": "",
                },
            ]
        }
        config = {
            "progress": {"dlc": "/totvs/dba/progress/dlc12", "proenv": "/totvs/dba/progress/dlc12/bin/proenv"},
            "dump": {"output_dir": "/totvs/temp", "threads_per_db": 2},
        }

        def noop(*args, **kwargs):
            return None

        original_isdir = jobs.os.path.isdir

        def fake_isdir(path):
            if path == "/totvs/database/dump/CON/ems2adt":
                return False
            return original_isdir(path)

        with patch.object(jobs, "_read_state", return_value=state), patch.object(
            jobs,
            "_list_matching_files",
            return_value=["/hostfs/totvs/database/CON/ems2adt"],
        ), patch.object(jobs, "_update_state", side_effect=noop), patch.object(
            jobs,
            "_update_item",
            side_effect=noop,
        ), patch.object(jobs, "_append_log", side_effect=lambda job_id, line: logs.append(line)), patch.object(
            jobs,
            "_prime_job_logs",
            side_effect=noop,
        ), patch.object(jobs, "_save_index", side_effect=noop), patch.object(
            jobs,
            "_index_data",
            return_value={"active_job_id": "job-1", "last_job_id": "job-1"},
        ), patch.object(jobs.os.path, "isdir", side_effect=fake_isdir), patch.object(
            jobs.time,
            "sleep",
            side_effect=noop,
        ):
            jobs._run_dry_job("job-1", config, catalog)

        self.assertIn('[DRY-RUN] Comando: /totvs/dba/progress/dlc12/bin/proutil "/hostfs/totvs/database/CON/ems2adt" -C busy', logs)
        self.assertIn('[DRY-RUN] Comando: mkdir -p "/hostfs/totvs/database/dump/CON/ems2adt"', logs)
        self.assertIn("[DRY-RUN] Simulação concluída", logs)
        self.assertLess(
            logs.index('[DRY-RUN] Comando: /totvs/dba/progress/dlc12/bin/proutil "/hostfs/totvs/database/CON/ems2adt" -C busy'),
            logs.index('[DRY-RUN] Comando: mkdir -p "/hostfs/totvs/database/dump/CON/ems2adt"'),
        )

    def test_append_command_output_mirrors_secondary_errors(self):
        logs = []

        with patch.object(jobs, "_append_log", side_effect=lambda job_id, line: logs.append(line)):
            jobs._append_command_output(
                "job-1",
                "/tmp/raw.log",
                "linha ok\nPermission denied attaching to shm seg 32880\noutra linha",
            )

        self.assertIn("linha ok", logs)
        self.assertIn("Permission denied attaching to shm seg 32880", logs)
        self.assertIn("[SECONDARY-ERROR] Permission denied attaching to shm seg 32880", logs)
        self.assertIn("outra linha", logs)

    def test_run_real_job_announces_each_table_in_loop_step(self):
        logs = []
        state = {
            "job_id": "job-1",
            "status": "queued",
            "mode": "real",
            "message": "Fila criada para execução",
            "created_at": "2026-04-28T00:00:00+00:00",
            "started_at": None,
            "finished_at": None,
            "updated_at": "2026-04-28T00:00:00+00:00",
            "items": [
                {
                    "dump_db_path": "/totvs/database/CON",
                    "db_mask": "*.db",
                    "selected_file_path": "/hostfs/totvs/database/CON/ems2adt.db",
                    "dump_path": "/totvs/database/dump/CON",
                    "load_path": "/totvs/database/load/CON",
                    "status": "pending",
                    "progress": 0,
                    "message": "Aguardando início",
                }
            ],
            "log_path": "/tmp/job-1.log",
            "total_dbs": 1,
            "completed_dbs": 0,
            "running_dbs": 0,
            "offline_dbs": 0,
            "failed_dbs": 0,
            "overall_progress": 0,
            "active_dbs": [],
            "selected_count": 1,
            "selected_file_count": 1,
        }
        catalog = {
            "catalog": [
                {
                    "step": 8,
                    "title": "Dump por tabela",
                    "kind": "loop",
                    "enabled": True,
                    "command": 'echo dumping {table_name}',
                    "loop_source_path": "{db_path}",
                    "loop_source_file": "tables.lst",
                    "description": "",
                }
            ]
        }
        config = {
            "progress": {"dlc": "/totvs/dba/progress/dlc12", "proenv": "/totvs/dba/progress/dlc12/bin/proenv"},
            "dump": {"output_dir": "/totvs/temp", "threads_per_db": 2},
        }

        def noop(*args, **kwargs):
            return None

        def fake_exists(path):
            return path == "/tmp/tables.lst"

        with patch.object(jobs, "_read_state", return_value=state), patch.object(jobs, "_update_state", side_effect=noop), patch.object(
            jobs,
            "_update_item",
            side_effect=noop,
        ), patch.object(jobs, "_append_log", side_effect=lambda job_id, line: logs.append(line)), patch.object(
            jobs,
            "_prime_job_logs",
            side_effect=noop,
        ), patch.object(jobs, "_save_index", side_effect=noop), patch.object(jobs, "_index_data", return_value={"active_job_id": "job-1", "last_job_id": "job-1"}), patch.object(
            jobs,
            "_ensure_execution_directories",
            side_effect=noop,
        ), patch.object(jobs, "_resolve_inventory_path", return_value="/tmp/tables.lst"), patch.object(jobs.os.path, "exists", side_effect=fake_exists), patch.object(
            jobs,
            "_run_shell_command",
            return_value=0,
        ), patch("builtins.open", mock_open(read_data="TAB_A\nTAB_B\n")):
            jobs._run_real_job("job-1", config, catalog)

        self.assertIn("[EXEC] Passo 8 - Dump por tabela", logs)
        self.assertIn("[EXEC] Passo 8 - Lendo tables.lst", logs)
        self.assertIn("[EXEC] Passo 8 - Tabela inventariada: TAB_A", logs)
        self.assertIn("[EXEC] Passo 8 - Tabela inventariada: TAB_B", logs)
        self.assertIn("[EXEC] DUMP da tabela TAB_A", logs)
        self.assertIn("[EXEC] DUMP da tabela TAB_B", logs)
        self.assertIn("[EXEC] Passo 8 - Concluido...", logs)
        self.assertLess(logs.index("[EXEC] Passo 8 - Lendo tables.lst"), logs.index("[EXEC] DUMP da tabela TAB_A"))
        self.assertLess(logs.index("[EXEC] DUMP da tabela TAB_A"), logs.index("[EXEC] DUMP da tabela TAB_B"))

    def test_run_real_job_uses_status_messages_for_normal_steps(self):
        logs = []
        state = {
            "job_id": "job-1",
            "status": "queued",
            "mode": "real",
            "message": "Fila criada para execução",
            "created_at": "2026-04-28T00:00:00+00:00",
            "started_at": None,
            "finished_at": None,
            "updated_at": "2026-04-28T00:00:00+00:00",
            "items": [
                {
                    "dump_db_path": "/totvs/database/CON",
                    "db_mask": "*.db",
                    "selected_file_path": "/hostfs/totvs/database/CON/ems2adt.db",
                    "dump_path": "/totvs/database/dump/CON",
                    "load_path": "/totvs/database/load/CON",
                    "status": "pending",
                    "progress": 0,
                    "message": "Aguardando início",
                }
            ],
            "log_path": "/tmp/job-1.log",
            "total_dbs": 1,
            "completed_dbs": 0,
            "running_dbs": 0,
            "offline_dbs": 0,
            "failed_dbs": 0,
            "overall_progress": 0,
            "active_dbs": [],
            "selected_count": 1,
            "selected_file_count": 1,
        }
        catalog = {
            "catalog": [
                {
                    "step": 4,
                    "title": "Dump das sequences",
                    "kind": "command",
                    "enabled": True,
                    "command": 'echo sequence dump',
                    "loop_source_path": "",
                    "loop_source_file": "",
                    "description": "",
                }
            ]
        }
        config = {
            "progress": {"dlc": "/totvs/dba/progress/dlc12", "proenv": "/totvs/dba/progress/dlc12/bin/proenv"},
            "dump": {"output_dir": "/totvs/temp", "threads_per_db": 2},
        }

        def noop(*args, **kwargs):
            return None

        with patch.object(jobs, "_read_state", return_value=state), patch.object(jobs, "_update_state", side_effect=noop), patch.object(
            jobs,
            "_update_item",
            side_effect=noop,
        ), patch.object(jobs, "_append_log", side_effect=lambda job_id, line: logs.append(line)), patch.object(
            jobs,
            "_prime_job_logs",
            side_effect=noop,
        ), patch.object(jobs, "_save_index", side_effect=noop), patch.object(jobs, "_index_data", return_value={"active_job_id": "job-1", "last_job_id": "job-1"}), patch.object(
            jobs,
            "_ensure_execution_directories",
            side_effect=noop,
        ), patch.object(jobs, "_run_shell_command", return_value=0):
            jobs._run_real_job("job-1", config, catalog)

        self.assertIn("[EXEC] Passo 4 - Dump das sequences", logs)
        self.assertIn("[EXEC] Passo 4 - Executando...", logs)
        self.assertIn("[EXEC] Passo 4 - Concluido...", logs)
        self.assertNotIn("[EXEC] Comando:", "\n".join(logs))
        self.assertNotIn("[EXEC] Retorno:", "\n".join(logs))

    def test_save_catalog_roundtrip_preserves_custom_step_nine_command(self):
        catalog = catalog_module.default_catalog()
        catalog["catalog"][8]["command"] = '{dlc_bin}/proutil {db_path} -C tabanalys > {load_path}/{db_name}_tab.ini'

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir) / "dump_catalog.yaml"
            catalog_module.save_catalog(catalog, str(temp_path))

            loaded = catalog_module.load_catalog(str(temp_path))

        self.assertEqual(
            loaded["catalog"][8]["command"],
            '{dlc_bin}/proutil {db_path} -C tabanalys > {load_path}/{db_name}_tab.ini',
        )


if __name__ == "__main__":
    unittest.main()
