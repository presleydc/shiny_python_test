#!/bin/bash
#SBATCH --job-name=ShinySleepJob
#SBATCH --output=shiny_sleep_job_%j.out   # Changed: Output to current directory
#SBATCH --error=shiny_sleep_job_%j.err    # Changed: Error to current directory
#SBATCH --time=0-00:01:00  # 1 minute max run time
#SBATCH --ntasks=1
#SBATCH --nodes=1

echo "Slurm job started on $(hostname) at $(date)"
sleep 10
echo "Slurm job finished on $(hostname) at $(date)"