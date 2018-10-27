#!/bin/bash
#SBATCH -N 3
#SBATCH --ntasks-per-node 8
#SBATCH --cpus-per-task 8
#SBATCH -p express
#SBATCH -t 30
#SBATCH -J PanPipes
#SBATCH --mail-type=ALL
#SBATCH --mail-user=$USER@mpe.mpg.de

# FOR x
# -N 3
# --ntasks-per-node 8
# --cpus-per-task 8
# -t 240
# -J PanPipes

# general nodes are useful for Source Extraction
# Fat noes are useful for identification / richness (convolve_fft is memory greedy) or ask for a large number of nodes as mem limits are on a per node basis.

export PROJECT="PanPipes"

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
export PYTHONPATH="${PYTHONPATH}:$HOME/$PROJECT"
export PYTHONUNBUFFERED=1

# Ensure random numbers and hashes are reproducible.
export PYTHONSEED=0
export PYTHONHASHSEED=0

# Directory to use for "scratch" space in Spark, including map output files and
# RDDs that get stored on disk. This should be on a fast, local disk in your system.
# It can also be a comma-separated list of multiple directories on different disks.
export SPARK_LOCAL_DIRS=/ptmp/jacobic

# Ensure you have executable permissions on the program.
SCRIPT="/u/${USER}/$PROJECT/src/data/make_dataset.py"
chmod u+x $SCRIPT

# These are not accessible as master node is not directly accessible by ssh.
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_WEBUI_PORT=8081

# For click to work (this should not be necessary as it is in ~/.bashrc)
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

CHUNK=$1
I=$SLURM_ARRAY_TASK_ID
START=$(echo "(($I - 1) * $CHUNK)" | bc -l)
END=$(echo "$START + $CHUNK" | bc -l)

#TODO: get rid of shift stuff after source extraction is completed.
#N=1
#BATCH_SIZE=1880
#SHIFT=$(echo "$N * $BATCH_SIZE" | bc -l)
#START=$(echo "(($I - 1) * $CHUNK) + $SHIFT" | bc -l)
#END=$(echo "$START + $CHUNK + $SHIFT" | bc -l)

echo "Running script"
echo "I $I"
echo "CHUNK $CHUNK"
echo "START $START"
echo "END $END"
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
$SCRIPT -rc panpipe -d random -s $START $END --hpc

# -id panpipe -d new -s $START $END --hpc

# Send email with attached output
mail -s $SLURM_JOB_NAME $USER@mpe.mpg.de < slurm-${SLURM_JOB_ID}_${I}.out
mail -s $SLURM_JOB_NAME $USER@mpe.mpg.de < $HOME/$PROJECT/pyspark.log


################################################################################

# Notes:
# --mem default units are MegaBytes, defaulted is unlimited --mem 120000

# The job run limit on Draco is set to 8.
# The default time limit is the partition's default time limit.
# Use the table below to select the sbatch partition.
# Partition   Processor  Max. CPUs per node  Max. Memory  Max. Nr.  Max. Run
#               type                           per node   of Nodes    Time
# --------------------------------------------------------------------------
# small       Haswell    16                     60 GB      0.5      24:00:00
# express     Haswell    32 / 64 in HT mode    120 GB       32         30:00
# short       Haswell    32 / 64 in HT mode    120 GB       32      04:00:00
# general     Haswell    32 / 64 in HT mode    120 GB       32      24:00:00
# fat         Haswell    32 / 64 in HT mode    500 GB        4      24:00:00
# fat01       Haswell    32 / 64 in HT mode    248 GB        1      24:00:00
# gpu         Haswell    32 / 64 (host cpus)   120 GB       32      24:00:00
# viz         Haswell    32 / 64 in HT mode    248 GB        4      24:00:00

#Job Size Factor
#The job size factor correlates to the number of nodes or CPUs the job has requested. This factor can be configured to favor larger jobs or smaller jobs based on the state of the PriorityFavorSmall boolean in the slurm.conf file. When PriorityFavorSmall is NO, the larger the job, the greater its job size factor will be. A job that requests all the nodes on the machine will get a job size factor of 1.0. When the PriorityFavorSmall Boolean is YES, the single node job will receive the 1.0 job size factor.
# Run scontrol show config to see the details.

#Basically massive jobs for short times get the highest priority, especially if you request all the cores on each node!
# Cores per node: 32 (each with 2 hyperthreads, thus 64 logical CPUs per node).

# 773 general compute nodes + 106 GPU nodes
# Processor type: Intel 'Haswell' Xeon E5-2698v3
# Processor clock: 2.3 GHz
# Peak performance per core: 26.5 GFlop/s
# Cores per node: 32 (each with 2 hyperthreads, thus 64 logical CPUs per node)
# Main memory:
# general nodes: 768 x 128 GB, 1 x 256 GB, 4 x 512 GB
# GPU nodes:     102 x 128 GB, 4 x 256 GB
# 64 general compute nodes of processor type 'Broadwell'
# Processor type: Intel 'Broadwell' Xeon E5-2698v4
# Processor clock: 2.2 GHz
# Cores per node: 40 (each with 2 hyperthreads, thus 80 logical CPUs per node)
# Main memory: 256 GB

# ABOUT THE EXTENSION CLUSTER OF THE HPC SYSTEM AT MPCDF
# The extension cluster DRACO of the HPC system HYDRA was installed in May 2016 at the MPCDF with Intel 'Haswell' Xeon E5-2698 processors (~ 880 nodes with 32 cores @ 2.3 GHz each).
# 106 of the nodes are equipped with accelerator cards (2 x PNY GTX980 GPUs each).
# Most of the compute nodes have a main memory of 128 GB, 4 nodes have 512 GB, 1 has 256 GB, 4 of the GPU nodes have a main memory of 256 GB.
# In January 2017, the DRACO cluster was expanded by 64 Intel 'Broadwell' nodes that were purchased by the Fritz-Haber Institute.
# The 'Broadwell' nodes have 40 cores each and a main memory of 256 GB.
# In total there are 30.688 cores with a main memory of 128 TB and a peak performance of 1.12 PetaFlop/s.
# In addition to the compute nodes there are 4 login nodes and 8 I/O nodes that serve the 1.5 PetaByte of disk storage.
# The common interconnect is a fast InfiniBand FDR14 network.
# The compute nodes and GPU nodes are bundled into 30 domains.
# Within one domain, the InfiniBand network topology is a 'fat tree' topology for high efficient communication.
# The InfiniBand connection between the domains is much weaker, so batch jobs are restricted to a single domain, that is 32 nodes.
