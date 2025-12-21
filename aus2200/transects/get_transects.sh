#!/bin/bash

#PBS -P ng72 
#PBS -q normal
#PBS -l walltime=6:00:00,mem=128GB 
#PBS -l ncpus=48
#PBS -l jobfs=32gb
#PBS -o /home/548/ab4502/working/ExtremeWind/jobs/messages/get_transects.o 
#PBS -e /home/548/ab4502/working/ExtremeWind/jobs/messages/get_transects.e
#PBS -l storage=gdata/ng72+gdata/hh5+gdata/ua8+gdata/ng72+gdata/bs94+gdata/xp65+gdata/dk92
 
#Set up conda/shell environments 
module use /g/data/xp65/public/modules
module load conda/analysis3-25.06

module use /g/data/dk92/apps/Modules/modulefiles
module load gadi_jupyterlab/23.02
jupyter.ini.sh -D

python /home/548/ab4502/working/sea_breeze_analysis/aus2200/transects/transect_calc.py illawara --start_date "2013-01-01 00:00" --end_date "2013-01-31 23:00"
python /home/548/ab4502/working/sea_breeze_analysis/aus2200/transects/transect_calc.py illawara --start_date "2013-02-01 00:00" --end_date "2013-02-28 23:00"
python /home/548/ab4502/working/sea_breeze_analysis/aus2200/transects/transect_calc.py illawara --start_date "2016-01-01 00:00" --end_date "2016-01-31 23:00"
python /home/548/ab4502/working/sea_breeze_analysis/aus2200/transects/transect_calc.py illawara --start_date "2016-02-01 00:00" --end_date "2016-02-29 23:00"
python /home/548/ab4502/working/sea_breeze_analysis/aus2200/transects/transect_calc.py illawara --start_date "2018-01-01 00:00" --end_date "2018-01-31 23:00"
python /home/548/ab4502/working/sea_breeze_analysis/aus2200/transects/transect_calc.py illawara --start_date "2018-02-01 00:00" --end_date "2018-02-28 23:00"
