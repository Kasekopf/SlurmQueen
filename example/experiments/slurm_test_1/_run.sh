#!/bin/bash
#SBATCH --time=5:00
#SBATCH --job-name=slurm_test_1
#SBATCH --partition=scavenge
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
module load Anaconda3/5.0.0

for file in `cat _tasks.txt | awk "(NR - 1) % ${1:-1} == ${SLURM_ARRAY_TASK_ID:-0}"`; do
  ./$file
done
