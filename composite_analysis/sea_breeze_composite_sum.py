import xarray as xr
import numpy as np
from dask.distributed import Client
import glob
import pandas as pd
import os
import argparse

def preprocess_sb(ds):
    ds = ds.expand_dims("month")
    prop_sb_days = xr.DataArray([ds.attrs["prop_sb_days"]],dims=["month"])
    ds["prop_sb_days"] = prop_sb_days
    return ds

def preprocess_nonsb(ds):
    ds = ds.expand_dims("month")
    prop_nonsb_days = xr.DataArray([ds.attrs["prop_nonsb_days"]],dims=["month"])
    ds["prop_nonsb_days"] = prop_nonsb_days
    return ds

if __name__ == "__main__":

    client = Client(scheduler_file=os.environ["DASK_PBS_SCHEDULER"])

    # Argument parser for the script
    parser = argparse.ArgumentParser(
        prog="Sea Breeze Composite summarise",
    )
    parser.add_argument("--region", type=str, help="Region to define sea breeze days")
    args = parser.parse_args()
    region = args.region

    #Load monthly files for sea breeze and non-sea breeze composites, created by the sea_breeze_composite.sh script
    sb_files = np.sort(glob.glob("/scratch/ng72/ab4502/hourly_composites/barra_c/barra_c_sb_"+region+"_????????????_????????????.nc"))
    file_dates = [pd.to_datetime(f.split("/")[-1].split("_")[-2]) for f in sb_files]
    sb = xr.open_mfdataset(sb_files,
                        combine="nested",
                        concat_dim=xr.DataArray(np.array(file_dates),dims="month"),
                        preprocess=preprocess_sb)

    nonsb_files = np.sort(glob.glob("/scratch/ng72/ab4502/hourly_composites/barra_c/barra_c_nonsb_"+region+"_????????????_????????????.nc"))
    file_dates = [pd.to_datetime(f.split("/")[-1].split("_")[-2]) for f in nonsb_files]
    nonsb = xr.open_mfdataset(nonsb_files,
                        combine="nested",
                        concat_dim=xr.DataArray(np.array(file_dates),dims="month"),
                        preprocess=preprocess_nonsb)

    #For each season, calculate the weighted average sea breeze and non-sea breeze composites, and save as netcdf (weighted by the proportion of sea breeze and non-sea breeze days for each month)
    for season in ["ANN","DJF","MAM","JJA","SON"]:
        print(season)
        if season == "ANN":
            #Do all months
            sb_avg = ((sb * sb.prop_sb_days).sum("month") / (sb.prop_sb_days.sum())).persist()
            nonsb_avg = ((nonsb * nonsb.prop_nonsb_days).sum("month") / (nonsb.prop_nonsb_days.sum())).persist()  

            sb_avg.attrs["months"] = list(pd.to_datetime(sb.month.values).strftime("%Y-%m-%d"))
            nonsb_avg.attrs["months"] = list(pd.to_datetime(nonsb.month.values).strftime("%Y-%m-%d"))              
        else:
            temp_sb = sb.sel(month=np.in1d(sb.month.dt.season,season))
            temp_nonsb = nonsb.sel(month=np.in1d(nonsb.month.dt.season,season))
            sb_avg = ((temp_sb * temp_sb.prop_sb_days).sum("month") / (temp_sb.prop_sb_days.sum())).persist()
            nonsb_avg = ((temp_nonsb * temp_nonsb.prop_nonsb_days).sum("month") / (temp_nonsb.prop_nonsb_days.sum())).persist()        

            sb_avg.attrs["months"] = list(pd.to_datetime(temp_sb.month.values).strftime("%Y-%m-%d"))
            nonsb_avg.attrs["months"] = list(pd.to_datetime(temp_nonsb.month.values).strftime("%Y-%m-%d"))

        sb_avg.to_netcdf("/g/data/ng72/ab4502/hourly_composites/barra_c/barra_c_hourly_sb_composite_"+region+"_"+season+".nc")
        nonsb_avg.to_netcdf("/g/data/ng72/ab4502/hourly_composites/barra_c/barra_c_hourly_nonsb_composite_"+region+"_"+season+".nc")

    #Remove the monthly files from disk
    for f in sb_files:
        os.remove(f)
    for f in nonsb_files:
        os.remove(f)

    #Concatenate the .csv land-sea temperature files for the region, and delete the monthly files. Potentially resample to daily (either mean diff, max diff, or integrated positive diff)