import os
import shutil
import subprocess


def _resolve_progress_cfg():
    candidates = [
        os.environ.get("PROCFG"),
        "/app/progress.cfg",
        "progress.cfg",
        "/totvs/database/dump/py/progress.cfg",
    ]

    for candidate in candidates:
        if candidate and (os.path.isfile(candidate) or candidate == "progress.cfg"):
            return candidate

    return "progress.cfg"

def _resolve_progress_dlc():
    candidates = [
        os.environ.get("DLC"),
        os.environ.get("PROGRESS_DLC"),
        "/totvs/dba/progress/dlc12",
        "/usr/dlc",
    ]

    for candidate in candidates:
        if candidate and os.path.isdir(candidate):
            return candidate

    return ""


def _build_progress_env(extra_env=None):
    env = os.environ.copy()
    dlc_path = _resolve_progress_dlc()
    procfg = _resolve_progress_cfg()

    if dlc_path:
        env["DLC"] = dlc_path
        env["PROMSGS"] = os.path.join(dlc_path, "promsgs")
        env["PATH"] = os.path.join(dlc_path, "bin") + os.pathsep + env.get("PATH", "")

    env["PROCFG"] = procfg

    if extra_env:
        env.update(extra_env)

    return env


def _prepare_progress_command(cmd):
    command = (cmd or "").strip()

    if not command:
        return command

    if command.startswith("export TERM=xterm;"):
        return command

    return f"export TERM=xterm; {command}"


def _should_use_sudo():
    if os.geteuid() == 0:
        return False

    return shutil.which("sudo") is not None


def _prepare_progress_command_for_sudo(cmd):
    command = _prepare_progress_command(cmd)

    if not command:
        return command

    env_assignments = []
    dlc_path = _resolve_progress_dlc()
    procfg = _resolve_progress_cfg()

    if dlc_path:
        env_assignments.append(f"DLC={dlc_path}")
        env_assignments.append(f"PROMSGS={os.path.join(dlc_path, 'promsgs')}")
        env_assignments.append(f"PATH={os.path.join(dlc_path, 'bin')}:$PATH")

    env_assignments.append(f"PROCFG={procfg}")

    env_assignments.insert(0, "TERM=xterm")

    export_prefix = " ".join(env_assignments)
    if _should_use_sudo():
        return f"sudo -n env {export_prefix} /bin/sh -lc {subprocess.list2cmdline([command])}"

    return f"env {export_prefix} /bin/sh -lc {subprocess.list2cmdline([command])}"


def run_command(cmd, extra_env=None):
    prepared_command = _prepare_progress_command_for_sudo(cmd)
    print(f"[EXEC] {prepared_command}")

    process = subprocess.Popen(
        prepared_command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=_build_progress_env(extra_env),
    )

    stdout, stderr = process.communicate()

    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8", errors="replace")

    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", errors="replace")

    return {
        "returncode": process.returncode,
        "stdout": stdout,
        "stderr": stderr
    }
