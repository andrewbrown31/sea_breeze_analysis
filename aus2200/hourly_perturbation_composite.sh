#!/bin/bash

#PBS -P ng72 
#PBS -q normal
#PBS -l walltime=12:00:00,mem=190GB 
#PBS -l ncpus=48
#PBS -l jobfs=190gb
#PBS -o /home/548/ab4502/working/ExtremeWind/jobs/messages/aus2200_hourly_pert.o 
#PBS -e /home/548/ab4502/working/ExtremeWind/jobs/messages/aus2200_hourly_pert.e
#PBS -l storage=gdata/ng72+gdata/hh5+gdata/ua8+gdata/ng72+gdata/bs94+gdata/xp65+gdata/dk92+gdata/ob53

#Set up conda/shell environments 
module use /g/data/xp65/public/modules
module load conda/analysis3-24.07

module use /g/data/dk92/apps/Modules/modulefiles
module load gadi_jupyterlab/23.02
jupyter.ini.sh -D

#python /home/548/ab4502/working/sea_breeze_analysis/aus2200/hourly_perturbation_composite.py "2013-01-01 00:00" "2013-01-31 23:00" --exp_id "mjo-neutral2013" --u_var "uas" --v_var "vas"
#python /home/548/ab4502/working/sea_breeze_analysis/aus2200/hourly_perturbation_composite.py "2013-02-01 00:00" "2013-02-28 23:00" --exp_id "mjo-neutral2013" --u_var "uas" --v_var "vas"

#python /home/548/ab4502/working/sea_breeze_analysis/aus2200/hourly_perturbation_composite.py "2016-01-01 00:00" "2016-01-31 23:00" --exp_id "mjo-elnino2016" --u_var "uas" --v_var "vas"
#python /home/548/ab4502/working/sea_breeze_analysis/aus2200/hourly_perturbation_composite.py "2016-02-01 00:00" "2016-02-29 23:00" --exp_id "mjo-elnino2016" --u_var "uas" --v_var "vas"

python /home/548/ab4502/working/sea_breeze_analysis/aus2200/hourly_perturbation_composite.py "2018-01-01 00:00" "2018-01-31 23:00" --exp_id "mjo-lanina2018" --u_var "uas" --v_var "vas"
python /home/548/ab4502/working/sea_breeze_analysis/aus2200/hourly_perturbation_composite.py "2018-02-01 00:00" "2018-02-28 23:00" --exp_id "mjo-lanina2018" --u_var "uas" --v_var "vas"