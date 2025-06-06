import os
import subprocess
import asyncio
import time
from datetime import datetime

from shiny import App, ui, render, reactive

# --- Helper function to read Slurm output/error files ---
async def read_slurm_logs(job_id):
    """
    Reads the .out and .err files for a given Slurm job ID and returns their content.
    Assumes files are in the current directory and named shiny_sleep_job_{job_id}.out/err.
    """
    stdout_file = f"shiny_sleep_job_{job_id}.out"
    stderr_file = f"shiny_sleep_job_{job_id}.err"
    
    output_content = ""
    error_content = ""

    try:
        if os.path.exists(stdout_file):
            with open(stdout_file, "r") as f:
                output_content = f.read()
        else:
            output_content = f"(Output file '{stdout_file}' not found.)"
    except Exception as e:
        output_content = f"(Could not read output file '{stdout_file}': {e})"

    try:
        if os.path.exists(stderr_file):
            with open(stderr_file, "r") as f:
                error_content = f.read()
        else:
            error_content = f"(Error file '{stderr_file}' not found.)"
    except Exception as e:
        error_content = f"(Could not read error file '{stderr_file}': {e})"

    return (
        f"--- Slurm Job {job_id} Standard Output ---\n"
        f"{output_content.strip()}\n\n" # .strip() to remove trailing newlines if present
        f"--- Slurm Job {job_id} Standard Error ---\n"
        f"{error_content.strip()}\n"
        f"-----------------------------------------"
    )

# --- Shiny UI ---
app_ui = ui.page_fluid(
    ui.h2("Slurm Job Launcher"),
    ui.layout_sidebar(
        ui.sidebar(
            ui.input_action_button("launch_job", "Launch Slurm Job", class_="btn-primary"),
            ui.hr(),
            ui.output_text_verbatim("job_status_display"),
        ),
        ui.h3("Slurm Job Details"),
        ui.output_text_verbatim("job_output"),
    ),
)

# --- Shiny Server Logic ---
def server(input, output, session):
    job_info = reactive.Value({
        "status": "No job launched", # This will be one of "No job launched", "Job running", "Job completed"
        "start_time": None,
        "end_time": None,
        "job_id": None,
        "hostname": "N/A" # Initialize hostname
    })
    job_output_content = reactive.Value("")

    # Define the Slurm job script content directly in Python
    # IMPORTANT: The --output and --error paths here must match what read_slurm_logs expects.
    # We are setting them to the current directory here.
    job_script_template = """#!/bin/bash
#SBATCH --job-name=ShinyEmbeddedJob
#SBATCH --output=shiny_sleep_job_%j.out   # Output to current directory
#SBATCH --error=shiny_sleep_job_%j.err    # Error to current directory
#SBATCH --time=0-00:01:00  # 1 minute max run time
#SBATCH --ntasks=1
#SBATCH --nodes=1

echo "Slurm job started on $(hostname) at $(date)"
sleep 30
echo "Slurm job finished on $(hostname) at $(date)"
"""

    @output
    @render.text
    def job_status_display():
        info = job_info.get()
        status = info["status"]
        hostname = info["hostname"]
        
        if status == "Job running":
            return f"Job running on host: {hostname}"
        elif status == "Job completed":
            duration = info["end_time"] - info["start_time"]
            return f"Job Completed in {duration:.2f} seconds."
        # For all other states (No job launched, Launching, Pending, Error),
        # always display "No job launched" in the status bar.
        # Detailed messages are in job_output.
        return "No job launched"


    @output
    @render.text
    def job_output():
        return job_output_content.get()

    @reactive.Effect
    @reactive.event(input.launch_job)
    async def _():
        # Reset job_info to "No job launched" before starting
        job_info.set({"status": "No job launched", "start_time": None, "end_time": None, "job_id": None, "hostname": "N/A"})
        job_output_content.set("Attempting to launch Slurm job...")

        script_dir = os.path.dirname(os.path.abspath(__file__))
        job_script_path = os.path.join(script_dir, "shiny_generated_job.sh") # New name for the generated script
        
        current_job_id = None # Initialize to None

        try:
            # Write the embedded script content to a file
            with open(job_script_path, "w") as f:
                f.write(job_script_template)
            
            # Make the script executable
            os.chmod(job_script_path, 0o755)

            sbatch_command = ["sbatch", "--parsable", job_script_path]
            print(f"Executing: {' '.join(sbatch_command)}")

            process = await asyncio.create_subprocess_exec(
                *sbatch_command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                current_job_id = stdout.decode().strip() # Store the job ID
                print(f"Slurm job submitted with ID: {current_job_id}")

                # Update job_info with the current job ID
                job_info.set({
                    "status": "No job launched", # job_status_display remains "No job launched"
                    "start_time": datetime.now(),
                    "end_time": None,
                    "job_id": current_job_id,
                    "hostname": "N/A" # Still N/A until it runs
                })
                job_output_content.set(f"Slurm job ID: {current_job_id}\nJob status: Pending.\nPolling for job completion and node information...")

                # Polling for job completion and node info
                last_known_hostname = "N/A"
                while True:
                    # Check job state and assigned node
                    check_command = ["squeue", "-h", "-j", current_job_id, "-o", "%T %N"] # Get state and node
                    check_process = await asyncio.create_subprocess_exec(
                        *check_command,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    check_stdout, check_stderr = await check_process.communicate()
                    squeue_output = check_stdout.decode().strip()

                    job_state = ""
                    current_hostname = "N/A"

                    if squeue_output:
                        parts = squeue_output.split(maxsplit=1)
                        job_state = parts[0]
                        if len(parts) > 1:
                            current_hostname = parts[1].strip()
                            last_known_hostname = current_hostname # Update last known
                    
                    if job_state == "RUNNING":
                        job_info.set({
                            "status": "Job running", # Updates job_status_display
                            "start_time": job_info.get()["start_time"] if job_info.get()["start_time"] else datetime.now(), # Ensure start_time is set
                            "end_time": None,
                            "job_id": current_job_id,
                            "hostname": last_known_hostname if last_known_hostname != "N/A" else "Unknown Node"
                        })
                        job_output_content.set(f"Slurm job ID: {current_job_id}\nCurrently running on: {job_info.get()['hostname']}\nMonitoring job status and output...")
                    elif job_state == "PENDING":
                        # job_status_display stays "No job launched"
                        job_output_content.set(f"Slurm job ID: {current_job_id}\nJob status: Pending. Waiting for allocation...")
                    elif not job_state: # Job no longer in squeue, check sacct for final state
                        sacct_command = ["sacct", "-j", current_job_id, "--format=State", "-n", "-P"]
                        sacct_process = await asyncio.create_subprocess_exec(
                            *sacct_command,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        )
                        sacct_stdout, sacct_stderr = await sacct_process.communicate()
                        
                        final_state_raw = sacct_stdout.decode().strip()
                        # Extract the first pipe-separated field and clean it thoroughly
                        final_state = final_state_raw.split('|')[0].strip().upper() if final_state_raw else "UNKNOWN"

                        print(f"DEBUG: Job {current_job_id} final_state extracted: '{final_state}' (repr: {repr(final_state)})") # For debugging

                        # --- Read log files for final display (both .out and .err) ---
                        logs_combined_content = await read_slurm_logs(current_job_id)

                        if final_state == "COMPLETED":
                            job_info.set({
                                "status": "Job completed", # Updates job_status_display
                                "start_time": job_info.get()["start_time"],
                                "end_time": datetime.now(),
                                "job_id": current_job_id,
                                "hostname": last_known_hostname
                            })
                            job_output_content.set(f"Slurm job {current_job_id} completed successfully.\n\n{logs_combined_content}")
                        # Check for known failure/termination states
                        elif final_state in ["FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "PREEMPTED", "OUT_OF_MEMORY"]:
                            job_info.set({
                                "status": "No job launched", # Resets job_status_display to default
                                "start_time": job_info.get()["start_time"],
                                "end_time": datetime.now(),
                                "job_id": current_job_id,
                                "hostname": last_known_hostname
                            })
                            job_output_content.set(f"Slurm job {current_job_id} ended with state: {final_state}.\n\n{logs_combined_content}")
                        else: # Any other truly unexpected or unknown state
                            job_info.set({
                                "status": "No job launched", # Resets job_status_display to default
                                "start_time": job_info.get()["start_time"],
                                "end_time": datetime.now(),
                                "job_id": current_job_id,
                                "hostname": last_known_hostname
                            })
                            job_output_content.set(f"Slurm job {current_job_id} ended in unexpected state: {final_state}. "
                                                  f"Please check Slurm logs directly on the cluster for job ID {current_job_id} and consult `sacct -j {current_job_id}`.\n\n{logs_combined_content}")
                        break # Exit polling loop

                    # If not in a final state, wait a bit and re-check.
                    await asyncio.sleep(5) # Poll every 5 seconds

            else:
                error_message = stderr.decode().strip()
                job_info.set({"status": "No job launched", "start_time": None, "end_time": None, "job_id": None, "hostname": "N/A"})
                job_output_content.set(f"Failed to submit Slurm job:\n{error_message}")

        except Exception as e:
            job_info.set({"status": "No job launched", "start_time": None, "end_time": None, "job_id": None, "hostname": "N/A"})
            job_output_content.set(f"An unexpected error occurred: {e}")
        finally:
            # Clean up the generated job script file
            if os.path.exists(job_script_path):
                try:
                    os.remove(job_script_path)
                    print(f"Cleaned up generated job script: {job_script_path}")
                except Exception as e:
                    print(f"Error cleaning up {job_script_path}: {e}")
            
            # Clean up the .out and .err files if a job ID was successfully obtained
            if current_job_id:
                output_file = f"shiny_sleep_job_{current_job_id}.out"
                error_file = f"shiny_sleep_job_{current_job_id}.err"
                
                for f in [output_file, error_file]:
                    if os.path.exists(f):
                        try:
                            os.remove(f)
                            print(f"Cleaned up job log file: {f}")
                        except Exception as e:
                            print(f"Error cleaning up {f}: {e}")


# --- Create the Shiny App instance ---
app = App(app_ui, server)