#!/bin/bash
#SBATCH -N 3
#SBATCH --ntasks-per-node 8
#SBATCH --cpus-per-task 8
#SBATCH -p viz
#SBATCH -t 30
#SBATCH -J pyspark
#SBATCH --mail-type=ALL
#SBATCH --mail-user=$USER@mpe.mpg.de


# Run in the following way:
# sbatch -a 1-1%1 src/slurm/pyspark.cmd path/to/<script>.py

export PROJECT="$(pwd)"
echo "PROJECT=$PROJECT"

# Load Draco modules required for spark
module load jdk
module load spark

# Load libraries required for Astromatic software
module load intel/18.0
module load mkl
echo "MKL_ROOT=$MKLROOT"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$MKLROOT/lib/intel64_lin"
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"


#TODO: please note this requires exporting conda's bin dir in ~/.bashrc
# Load conda env
source activate ps1

# Append PYTHONPATH in order to find all modules in the project directory.
export PYTHONPATH="${PYTHONPATH}:$PROJECT"
export PYTHONUNBUFFERED=1

# Ensure random numbers and hashes are reproducible.
export PYTHONSEED=0
export PYTHONHASHSEED=0

# Directory to use for "scratch" space in Spark, including map output files and
# RDDs that get stored on disk. This should be on a fast, local disk in your system.
# It can also be a comma-separated list of multiple directories on different disks.
export SPARK_LOCAL_DIRS=/ptmp/jacobic

# Ensure you have executable permissions on the program.
SCRIPT=$1
chmod u+x $SCRIPT

# These are not accessible as master node is not directly accessible by ssh.
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_WEBUI_PORT=8081

# For click to work (this should not be necessary as it is in ~/.bashrc)
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

spark-start
echo $MASTER
DIRECTORY=`dirname $0`
echo $DIRECTORY

# standalone mode does not support cluster mode (--deploy-mode cluster) for Python applications.
# leave --master as default (local) otherwise you will not be able to access the webUI

# Spark parameters from SLURM environment variables.
# See Job Environments section of https://www.mpcdf.mpg.de/services/computing/linux/migration-from-sge-to-slurm.
# All memory is expressed in MegaBytes.
# Executor memory must be an integer so use bc (no dp) rather than bc -l.

echo "SLURM_JOB_NUM_NODES ${SLURM_JOB_NUM_NODES}"
echo "SLURM_NTASKS_PER_NODE ${SLURM_NTASKS_PER_NODE}"
echo "SLURM_CPUS_PER_TASK ${SLURM_CPUS_PER_TASK}"

TOTAL_EXECUTOR_CORES=$(echo "$SLURM_JOB_NUM_NODES * $SLURM_NTASKS_PER_NODE * $SLURM_CPUS_PER_TASK" | bc -l)
DRIVER_MEMORY=10000
EXECUTOR_MEMORY=$(echo "(($SLURM_JOB_NUM_NODES * $SLURM_MEM_PER_NODE) - $DRIVER_MEMORY) / $TOTAL_EXECUTOR_CORES" | bc)

echo "TOTAL_EXECUTOR_CORES $TOTAL_EXECUTOR_CORES"
echo "DRIVER_MEMORY ${DRIVER_MEMORY}m"
echo "EXECUTOR_MEMORY ${EXECUTOR_MEMORY}m"

# Spark job submission.
spark-submit \
--total-executor-cores $TOTAL_EXECUTOR_CORES \
--executor-memory ${EXECUTOR_MEMORY}m \
--driver-memory ${DRIVER_MEMORY}m \
$SCRIPT

# Send email with attached output
mail -s $SLURM_JOB_NAME $USER@mpe.mpg.de < slurm-${SLURM_JOB_ID}_${I}.out
mail -s $SLURM_JOB_NAME $USER@mpe.mpg.de < $PROJECT/pyspark.log

