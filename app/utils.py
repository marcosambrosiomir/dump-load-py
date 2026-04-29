from app.runner import run_command

def is_db_online(db_path):
    result = run_command(f"proutil {db_path} -C busy")

    if "in use" in result["stdout"].lower():
        return True

    if result["returncode"] != 0:
        return False

    return False
