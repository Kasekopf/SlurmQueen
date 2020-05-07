#!/bin/bash
#SBATCH --time=5:00
#SBATCH --job-name=slurm_test_1
#SBATCH --partition=scavenge
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
module load Anaconda3/5.0.0

for file in `find *.in | awk "(NR - 1) % $1 == $SLURM_ARRAY_TASK_ID"`; do
  ./$file
done
