#!/bin/bash
#REMOTE=`dirname $0`
# In order to write error and output to correct dir
cd /u/${USER}/PanPipes/src/data/remote/jobs
llsubmit "../mpcdf.batch"
