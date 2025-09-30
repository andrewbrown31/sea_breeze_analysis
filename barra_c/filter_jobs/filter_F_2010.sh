#!/bin/bash

#PBS -P ng72
#PBS -q normal
#PBS -l walltime=06:00:00,mem=128GB 
#PBS -l ncpus=48
#PBS -l jobfs=32gb
#PBS -o /home/548/ab4502/working/ExtremeWind/jobs/messages/barra_c_filter_F_2010.o 
#PBS -e /home/548/ab4502/working/ExtremeWind/jobs/messages/barra_c_filter_F_2010.e
#PBS -l storage=gdata/ng72+gdata/hh5+gdata/ua8+gdata/rt52+gdata/ob53+gdata/xp65+gdata/dk92
 
#Set up conda/shell environments 
module use /g/data/dk92/apps/Modules/modulefiles
module use /g/data/xp65/public/modules

module load gadi_jupyterlab/23.02
module load conda/analysis3-24.07

jupyter.ini.sh -D

#Threshold based on Jan-Feb period for BARRA-C
f_threshold="16.1"	

#Set the start and end dates, and the current date as the start date
start_date="2010-01-01"
end_date="2011-01-01"
current_date="$start_date 00:00:00"

#Set domain
lat1=-45.7
lat2=-6.9
lon1=108.0
lon2=158.5

#Set an optional interval for the time range
# This is the interval that will be used to calculate the end time for each iteration
#For monthly data, this should be 31 days or greater. It is a maximum of monthly intervals
optional_interval="10 days"

while [[ "$current_date" < "$end_date" ]]; do
    
    #Create a datetime object for the current date
    start_time=$(date -d "$current_date" +"%Y-%m-%d %H:%M")

    #Adjust this back one hour for calculating F_hourly (as it is a time difference)
    start_time_f_hourly=$(date -ud "$start_time UTC - 1 hour" +"%Y-%m-%d %H:%M")

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

    #Print the date interval
    echo "$start_time" "$end_time" 

    #Calculate the F diagnostic field for the specified time range
    python /home/548/ab4502/working/sea_breeze_analysis/barra_c/barra_c_spatial_sb.py "$start_time" "$end_time" --model barra_c_smooth_s2 --smooth --sigma 2 --lat1 "$lat1" --lat2 "$lat2" --lon1 "$lon1" --lon2 "$lon2"

    #Calculate the F_hourly diagnostic field for the specified time range
    python /home/548/ab4502/working/sea_breeze_analysis/barra_c/barra_c_temporal_sb.py "$start_time_f_hourly" "$end_time" --model barra_c_smooth_s2 --smooth --sigma 2 --lat1 "$lat1" --lat2 "$lat2" --lon1 "$lon1" --lon2 "$lon2"

    #Run the filter script for each field with the specified parameters
    python /home/548/ab4502/working/sea_breeze/filter.py --model barra_c_smooth_s2 --filter_name standard --field_name F --t1 "$start_time" --t2 "$end_time" --threshold fixed --threshold_value $f_threshold --lat1 "$lat1" --lat2 "$lat2" --lon1 "$lon1" --lon2 "$lon2" --humidity_change_filter 

    #Remove the F field for the specified time range
    rm -rf /g/data/ng72/ab4502/sea_breeze_detection/barra_c_smooth_s2/F_$(date -d "$start_time" +"%Y%m%d%H%M")_$(date -d "$end_time" +"%Y%m%d%H%M").zarr

    #Remove the F_hourly field for the specified time range
    rm -rf /g/data/ng72/ab4502/sea_breeze_detection/barra_c_smooth_s2/F_hourly_$(date -d "$start_time_f_hourly" +"%Y%m%d%H%M")_$(date -d "$end_time" +"%Y%m%d%H%M").zarr

    #If we are into the next month, advance to the first of the next month
    #Otherwise, advance by the specified interval
    if [[ "$new_month" == 1 ]]; then
        current_date=$(date -d "$next_month" +"%Y-%m-%d %H:%M")
        new_month=0
    else
        current_date=$(date -ud "$current_date UTC + $optional_interval" +"%Y-%m-%d %H:%M")
    fi

done
