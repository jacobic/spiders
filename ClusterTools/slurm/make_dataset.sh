#!/bin/bash

# Add the following lines to your crontab file to run this script every hour:
# MAILTO="jacobic@mpe.mpg.de"
# 0 * * * * $HOME/PanPipes/src/slurm/make_dataset.sh
# The crontab file can be accessed for editing with the following command:
# crontab -e
# Remember to edit and remove the entry after the goal is completed.
# The following crontab does not work! jobs are submitted but they finish without outputs
# My suspicion is that $USER variables etc in make_dataset.cmd are not set.
# */30 * * * * /bin/bash /u/jacobic/PanPipes/src/slurm/make_dataset.sh

# Number of spark clusters, Maximum simultaneous number is 8.
#FOR X
#SIZE=250
#CONCURRENT_JOBS=8


SIZE=80
CONCURRENT_JOBS=8


#15 galaxy clusters per spark cluster (each with 512 cores, i.e. 8 Nodes x 64 cores) with time x_lim of 4 hours is doable.

# Number of Galaxy Cluster Candidate objects.
# Quick way to determine this number:
#'fitsinfo file.fits' or 'cat file.txt | wc -l', but remember to -1 for the txt header

#DATASET="old"
#LEN=150

#DATASET="new"
#LEN=935
#
#DATASET="CODEX"
##LEN=1000
#TOTAL=10382
#LEN=10382

DATASET="Merged"
##LEN=1287
LEN=1300

#DATASET="random"
#LEN=1287
#LEN=1000

#FOR X
#REDUCED_TOTAL=$(echo "( $SIZE / 1380)" | bc -l)
#LEN=$(echo "scale=0; ($TOTAL * $REDUCED_TOTAL)/1" | bc)
#echo "TOTAL $TOTAL, FOO $REDUCED_TOTAL, LEN $LEN"

#DATASET="COSMOS"
#LEN=183

# Split Candidates into chunks over the spark clusters.
CHUNK=$( echo "((($LEN / $SIZE) + 0.5)/1)" | bc)
echo "CHUNK $CHUNK"

# Reinitialise log file
LOG=$HOME/PanPipes/pyspark.log
rm $LOG
touch $LOG

# Limit the number of jobs with %MAX_NUM after declaring the size of the array.
sbatch --array 1-"${SIZE}"%"${CONCURRENT_JOBS}" $HOME/PanPipes/src/slurm/make_dataset.cmd "${CHUNK}"
#sbatch $HOME/PanPipes/src/slurm/make_dataset.cmd "${CHUNK}"

# This takes approx 48 hours with 8 clusters x (4 nodes x 64 cores)
#TOTAL 10382, FOO .18115942028985507246, LEN 1880
#Submitted batch job 5915599

#ssh://jacobic@localhost:2224:bash src/slurm/make_dataset.sh
#TOTAL 10382, FOO .18115942028985507246, LEN 1880
#CHUNK 7
#Submitted batch job 5966411
