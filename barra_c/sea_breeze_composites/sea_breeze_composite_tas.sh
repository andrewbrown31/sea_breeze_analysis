#!/bin/bash

#PBS -P ng72 
#PBS -q normal
#PBS -l walltime=12:00:00,mem=190GB 
#PBS -l ncpus=48
#PBS -l jobfs=32gb
#PBS -o /home/548/ab4502/working/ExtremeWind/jobs/messages/barra_c_sb_composite_tas.o 
#PBS -e /home/548/ab4502/working/ExtremeWind/jobs/messages/barra_c_sb_composite_tas.e
#PBS -l storage=gdata/ng72+gdata/hh5+gdata/ua8+gdata/ng72+gdata/bs94+gdata/xp65+gdata/dk92+gdata/ob53

#Set up conda/shell environments 
module use /g/data/xp65/public/modules
module load conda/analysis3-24.07

module use /g/data/dk92/apps/Modules/modulefiles
module load gadi_jupyterlab/23.02
jupyter.ini.sh -D

#Set the start and end dates, and the current date as the start date
start_date="2000-01-01"
end_date="2024-12-31"
current_date="$start_date 00:00:00"

#Set an optional interval for the time range
#This is the interval that will be used to calculate the end time for each iteration
#For monthly data, this should be 31 days or greater. It is a maximum of monthly intervals
optional_interval="31 days"

while [[ "$current_date" < "$end_date" ]]; do
    
    #Create a datetime object for the current date
    start_time=$(date -d "$current_date" +"%Y-%m-%d %H:%M")

    #Calculate the current month and next month
    start_month=$(date -d "$current_date" +"%Y-%m-01")
    next_month=$(date -d "$start_month +1 month" +"%Y-%m-01")

    #Set the end time based on the specified interval, minus one hour
    end_time=$(date -ud "$start_time UTC + $optional_interval" +"%Y-%m-%d %H:%M")
    end_time=$(date -ud "$end_time UTC - 1 hour" +"%Y-%m-%d %H:%M")

    #If the end time is greater than the next month, set the end time to the last hour of the month
    if [[ $(date -d "$end_time" +"%s") -gt $(date -d "$next_month" +"%s") ]]; then
        end_time=$(date -d "$next_month -1 hour" +"%Y-%m-%d %H:%M")
        new_month=1
    fi

    python /home/548/ab4502/working/sea_breeze_analysis/barra_c/sea_breeze_composites/sea_breeze_composite.py "$start_time" "$end_time" --region tas --lat_start "-45" --lat_end "-30" --lon_start 135 --lon_end 155

    #If we are into the next month, advance to the first of the next month
    #Otherwise, advance by the specified interval
    if [[ "$new_month" == 1 ]]; then
        current_date=$(date -d "$next_month" +"%Y-%m-%d %H:%M")
        new_month=0
    else
        current_date=$(date -ud "$current_date UTC + $optional_interval" +"%Y-%m-%d %H:%M")
    fi

done    

#Combine the output files into seasonal averages and delete the monthly files
python /home/548/ab4502/working/sea_breeze_analysis/barra_c/sea_breeze_composites/sea_breeze_composite_sum.py --region tas