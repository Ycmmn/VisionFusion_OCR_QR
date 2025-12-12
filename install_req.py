import os
import sys
import subprocess
import venv

VENV_DIR = "venv_auto"
PY = sys.executable

def create_venv():
    print("Creating virtual environment...")
    venv.EnvBuilder(with_pip=True).create(VENV_DIR)
    pip = os.path.join(VENV_DIR, "Scripts", "pip.exe")
    subprocess.check_call([pip, "install", "-r", "requirements.txt"])

def run_app():
    python = os.path.join(VENV_DIR, "Scripts", "python.exe")
    subprocess.call([python, "-m", "streamlit", "run", "app2.py"])

if not os.path.exists(VENV_DIR):
    create_venv()

run_app()
