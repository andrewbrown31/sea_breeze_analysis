#!/bin/bash

#PBS -P ng72
#PBS -q normal
#PBS -l walltime=06:00:00,mem=64GB 
#PBS -l ncpus=1
#PBS -l jobfs=16gb
#PBS -o /home/548/ab4502/working/ExtremeWind/jobs/messages/obs_sp.o 
#PBS -e /home/548/ab4502/working/ExtremeWind/jobs/messages/obs_sp.e
#PBS -l storage=gdata/ng72+gdata/w40+gdata/xp65
 
#Set up conda/shell environments 
module use /g/data/xp65/public/modules
module load conda/analysis3-24.07

python /home/548/ab4502/working/sea_breeze_analysis/obs/obs_sp.py
