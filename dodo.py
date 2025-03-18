"""
Run or update the project. This file uses the `doit` Python package.
It works like a Makefile, but is Python-based
"""

#######################################
## Configuration and Helpers for PyDoit
#######################################
## Make sure the src folder is in the path
import sys

sys.path.insert(1, "./src/")

import shutil
from os import environ, getcwd, path
from pathlib import Path

from colorama import Fore, Style, init
from doit.reporter import ConsoleReporter

try:
    in_slurm = environ["SLURM_JOB_ID"] is not None
except:
    in_slurm = False

class GreenReporter(ConsoleReporter):
    def write(self, stuff, **kwargs):
        doit_mark = stuff.split(" ")[0].ljust(2)
        task = " ".join(stuff.split(" ")[1:]).strip() + "\n"
        output = (
            Fore.GREEN
            + doit_mark
            + f" {path.basename(getcwd())}: "
            + task
            + Style.RESET_ALL
        )
        self.outstream.write(output)

if not in_slurm:
    DOIT_CONFIG = {
        "reporter": GreenReporter,
        "backend": "sqlite3",
        "dep_file": "./.doit-db.sqlite",
    }
else:
    DOIT_CONFIG = {
        "backend": "sqlite3",
        "dep_file": "./.doit-db.sqlite",
    }

init(autoreset=True)

##########################################
## Base Directories (if you have them)
##########################################
# Example if you have a settings.py or config file. Adjust or remove as needed.
try:
    from settings import config
    BASE_DIR = config("BASE_DIR")
    DATA_DIR = config("DATA_DIR")
    MANUAL_DATA_DIR = config("MANUAL_DATA_DIR")
    OUTPUT_DIR = Path(config("OUTPUT_DIR"))
    PUBLISH_DIR = config("PUBLISH_DIR")
    USER = config("USER")
except ImportError:
    # If you don't have a settings.py or config, fallback
    BASE_DIR = Path(".")
    DATA_DIR = Path("./data")
    OUTPUT_DIR = Path("./_output")

##########################################
## Notebook Helper Functions
##########################################
def jupyter_execute_notebook(notebook):
    return (
        f"jupyter nbconvert --execute --to notebook "
        f"--ClearMetadataPreprocessor.enabled=True --log-level WARN --inplace "
        f"./src/{notebook}.ipynb"
    )

def jupyter_to_html(notebook, output_dir=OUTPUT_DIR):
    return (
        f"jupyter nbconvert --to html --log-level WARN "
        f"--output-dir={output_dir} ./src/{notebook}.ipynb"
    )

def jupyter_clear_output(notebook):
    return (
        f"jupyter nbconvert --log-level WARN "
        f"--ClearOutputPreprocessor.enabled=True "
        f"--ClearMetadataPreprocessor.enabled=True "
        f"--inplace ./src/{notebook}.ipynb"
    )

def jupyter_to_python(notebook, build_dir=OUTPUT_DIR):
    """Convert a notebook to a Python script in build_dir."""
    return (
        f"jupyter nbconvert --log-level WARN --to python "
        f"./src/{notebook}.ipynb --output _{notebook}.py "
        f"--output-dir {build_dir}"
    )

##########################################
## Simple helper for copying a file
##########################################
def copy_file(origin_path, destination_path, mkdir=True):
    """Create a Python action for copying a file."""
    def _copy_file():
        origin = Path(origin_path)
        dest = Path(destination_path)
        if mkdir:
            dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origin, dest)
    return _copy_file

##########################################
## 1) Convert & Run the Seven Notebooks
##########################################
# The exact 7 notebooks from your screenshot, in correct order:
notebooks_in_order = [
    "1.dataPullAndMakeDataTidy",
    "2.briefTourOfData",
    "3.summaryStats",
    "4.latexSummaryStats",
    "5.Table1",
    "6.TableA1FigureA1",
    "7.latexOverview",
]

# Minimal dictionary if you need to attach file_dep or targets individually:
notebook_tasks = {nb + ".ipynb": {"file_dep": [], "targets": []} for nb in notebooks_in_order}

def task_convert_notebooks_to_scripts():
    """
    Convert each of the 7 notebooks to a Python script
    so doit can detect real code changes (rather than ephemeral metadata).
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for notebook_name in notebooks_in_order:
        notebook_ipynb = notebook_name + ".ipynb"
        yield {
            "name": notebook_ipynb,
            "actions": [
                jupyter_clear_output(notebook_name),
                jupyter_to_python(notebook_name, OUTPUT_DIR),
            ],
            "file_dep": [Path("./src") / notebook_ipynb],
            "targets": [OUTPUT_DIR / f"_{notebook_name}.py"],
            "clean": True,
            "verbosity": 0,
        }

def task_run_notebooks():
    """
    Execute the notebooks in strict sequence: #2 depends on #1, #3 depends on #2, etc.
    Then convert them to HTML and copy them to OUTPUT_DIR.
    """
    for i, notebook_name in enumerate(notebooks_in_order):
        notebook_ipynb = notebook_name + ".ipynb"
        if i == 0:
            deps = []
        else:
            # The previous notebook in the chain
            prev_notebook = notebooks_in_order[i - 1] + ".ipynb"
            deps = [f"run_notebooks:{prev_notebook}"]

        yield {
            "name": notebook_ipynb,
            "actions": [
                """python -c "import sys; from datetime import datetime; print(f'Start """
                + notebook_ipynb
                + """: {datetime.now()}', file=sys.stderr)" """,
                jupyter_execute_notebook(notebook_name),
                jupyter_to_html(notebook_name, OUTPUT_DIR),
                copy_file(
                    Path("./src") / notebook_ipynb,
                    OUTPUT_DIR / notebook_ipynb,
                    mkdir=True,
                ),
                jupyter_clear_output(notebook_name),
                """python -c "import sys; from datetime import datetime; print(f'End """
                + notebook_ipynb
                + """: {datetime.now()}', file=sys.stderr)" """,
            ],
            "file_dep": [
                OUTPUT_DIR / f"_{notebook_name}.py",
                *notebook_tasks[notebook_ipynb]["file_dep"],
            ],
            "targets": [
                OUTPUT_DIR / f"{notebook_name}.html",
                OUTPUT_DIR / notebook_ipynb,
                *notebook_tasks[notebook_ipynb]["targets"],
            ],
            # Must also wait for the script conversion step
            "task_dep": deps + ["convert_notebooks_to_scripts"],
            "clean": True,
        }

##########################################
## Example: Other tasks if needed
##########################################
""""
def task_summary_stats():
    file_dep = ["./src/example_table.py"]
    file_output = ["example_table.tex", "pandas_to_latex_simple_table1.tex"]
    targets = [OUTPUT_DIR / file for file in file_output]
    return {
        "actions": [
            "ipython ./src/example_table.py",
            "ipython ./src/pandas_to_latex_demo.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
    }

def task_example_plot():
    file_dep = [Path("./src") / f for f in ["example_plot.py", "pull_fred.py"]]
    file_output = ["example_plot.png"]
    targets = [OUTPUT_DIR / f for f in file_output]
    return {
        "actions": [
            "ipython ./src/example_plot.py",
        ],
        "targets": targets,
        "file_dep": file_dep,
        "clean": True,
    }
"""

# End of dodo.py"
