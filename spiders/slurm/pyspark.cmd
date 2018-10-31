#!/bin/bash
#SBATCH -N 3
#SBATCH --ntasks-per-node 8
#SBATCH --cpus-per-task 4
#SBATCH -p viz
#SBATCH -t 30
#SBATCH -J pyspark
#SBATCH --mail-type=ALL
#SBATCH --mail-user=$USER@mpe.mpg.de

# First add to path:

# Run in the following way:
# sbatch -a 1-1%1 -p <partition> -N <nodes> -t <timelimit> \
# pyspark.cmd <conda env> "path/to/<script>.py args -foo -bar"

export PROJECT="$(pwd)" && echo "PROJECT=$PROJECT"

# Load Draco modules required for spark
module load jdk
module load spark

# Load libraries required for Astromatic software
module load intel/18.0
module load mkl
echo "MKL_ROOT=$MKLROOT"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$MKLROOT/lib/intel64_lin"
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"

#TODO: Note this requires exporting conda's bin dir in ~/.bashrc
# Load conda env
PROJECT_ENV="$1" && echo "PROJECT_ENV $PROJECT_ENV" && source activate $PROJECT_ENV

# Append PYTHONPATH in order to find all modules in the project directory.
export PYTHONPATH="${PYTHONPATH}:$PROJECT" && export PYTHONUNBUFFERED=1

# Ensure random numbers and hashes are reproducible.
export PYTHONSEED=0 && export PYTHONHASHSEED=0

# Directory to use for "scratch" space in Spark, including map output files and
# RDDs that get stored on disk. This should be on a fast, local disk.
export SPARK_LOCAL_DIRS=/ptmp/jacobic

# Ensure you have executable permissions on the program.
ARGS="$2" && echo "ARGS $ARGS" && SCRIPT=$(echo $ARGS | awk '{print $1;}')
echo "SCRIPT $SCRIPT" && chmod u+x $SCRIPT

# These are not accessible as master node is not directly accessible by ssh.
export SPARK_MASTER_WEBUI_PORT=8080 && export SPARK_WORKER_WEBUI_PORT=8081

# For click to work (this should not be necessary as it is in ~/.bashrc)
export LC_ALL=C.UTF-8 && export LANG=C.UTF-8

spark-start
echo $MASTER && DIRECTORY=`dirname $0` && echo $DIRECTORY

# standalone mode does not support cluster mode (--deploy-mode cluster) for Python applications.
# leave --master as default (local) otherwise you will not be able to access the webUI

# Spark parameters from SLURM env vars. See Job Environments section of:
# https://www.mpcdf.mpg.de/services/computing/linux/migration-from-sge-to-slurm.
# Memory is in MB. Executor mem must be int so use bc (no dp) instead of bc -l.

echo "SLURM_JOB_NUM_NODES ${SLURM_JOB_NUM_NODES}"
echo "SLURM_NTASKS_PER_NODE ${SLURM_NTASKS_PER_NODE}"
echo "SLURM_CPUS_PER_TASK ${SLURM_CPUS_PER_TASK}"
echo "SLURM_MEM_PER_NODE ${SLURM_MEM_PER_NODE}"

TOTAL_EXECUTOR_CORES=$(echo "$SLURM_JOB_NUM_NODES * $SLURM_NTASKS_PER_NODE * $SLURM_CPUS_PER_TASK" | bc -l)
DRIVER_MEMORY=20000
EXECUTOR_MEMORY=$(echo "(($SLURM_JOB_NUM_NODES * $SLURM_MEM_PER_NODE) - $DRIVER_MEMORY) / $TOTAL_EXECUTOR_CORES" | bc)

# Spark job submission.
SUBMIT="spark-submit --total-executor-cores $TOTAL_EXECUTOR_CORES \
--executor-memory ${EXECUTOR_MEMORY}m --driver-memory ${DRIVER_MEMORY}m $ARGS"

rm "$PROJECT/pyspark.log" && touch "$PROJECT/pyspark.log"
echo "$SUBMIT" && eval $SUBMIT

# Send email with attached output
mail -s $SLURM_JOB_NAME $USER@mpe.mpg.de < slurm-${SLURM_JOB_ID}_${I}.out
mail -s $SLURM_JOB_NAME $USER@mpe.mpg.de < $PROJECT/pyspark.log
