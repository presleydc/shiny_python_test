from shiny import App, reactive, render, ui
import subprocess
import time
import os
from pathlib import Path

# Simple SLURM script template
slurm_script_template = """#!/bin/bash
#SBATCH --job-name=sleepy
#SBATCH --output={output}
#SBATCH --ntasks=1
#SBATCH --time=00:01:00

hostname
sleep 30
"""

# Temporary directory for slurm scripts and output
slurm_dir = Path("/tmp/shiny_slurm")
slurm_dir.mkdir(parents=True, exist_ok=True)

# Store job ID and output file
current_job = {"job_id": None, "output_file": None}


def submit_slurm_job():
    output_file = slurm_dir / f"slurm_output_{int(time.time())}.txt"
    script_file = slurm_dir / f"slurm_script_{int(time.time())}.sh"

    script_content = slurm_script_template.format(output=output_file)
    script_file.write_text(script_content)

    result = subprocess.run(["sbatch", script_file], capture_output=True, text=True)
    if result.returncode == 0:
        job_id = result.stdout.strip().split()[-1]
        current_job["job_id"] = job_id
        current_job["output_file"] = output_file
        return job_id
    else:
        return None


def get_job_status(job_id):
    try:
        result = subprocess.run(
            ["sacct", "-j", job_id, "--format=JobID,State", "--parsable2", "--noheader"],
            capture_output=True,
            text=True
        )
        lines = result.stdout.strip().splitlines()
        if lines:
            # Return state of the main job (not child steps)
            return lines[0].split("|")[1]
        else:
            return "UNKNOWN"
    except Exception:
        return "ERROR"


def get_hostname(output_file):
    if output_file.exists():
        with open(output_file) as f:
            return f.readline().strip()
    return "(not yet available)"


app_ui = ui.page_fluid(
    ui.h2("Launch SLURM Sleep Job"),
    ui.input_action_button("launch", "Launch Job"),
    ui.output_text_verbatim("job_id"),
    ui.output_text_verbatim("job_status"),
    ui.output_text_verbatim("hostname"),
)


def server(input, output, session):
    job_status = reactive.Value("No job yet")

    @reactive.effect
    @reactive.event(input.launch)
    def _():
        job_id = submit_slurm_job()
        if job_id:
            job_status.set("PENDING")
        else:
            job_status.set("Failed to submit")

    @reactive.effect(interval=reactive.timer(2.0))
    def _():
        if current_job["job_id"]:
            status = get_job_status(current_job["job_id"])
            job_status.set(status)

    @output
    @render.text
    def job_id():
        return f"Job ID: {current_job['job_id']}" if current_job["job_id"] else "No job submitted."

    @output
    @render.text
    def job_status():
        return f"Status: {job_status()}" if current_job["job_id"] else ""

    @output
    @render.text
    def hostname():
        if current_job["output_file"]:
            return f"Hostname: {get_hostname(current_job['output_file'])}"
        return ""

app = App(app_ui, server)
