import xarray as xr
from dask.distributed import Client
import pandas as pd
import numpy as np
import os
import datetime as dt

def preprocess_aest(ds):
    
    """
    Preprocess the hourly sea breeze object dataset by converting time to AEST and filtering to 9am-9pm local time.
    Output is daily occurrences of a sea breeze.
    """

    ds["time"] = pd.to_datetime(ds["time"]) + dt.timedelta(hours=10)
    ds = ds.sel(time=(ds.time.dt.hour >= 9) & (ds.time.dt.hour <= 21))
    ds.attrs["time_zone"] = "AEST"
    ds.attrs["local_time_restriction"] = "0900_2100"
    
    return ds.groupby("time.date").max().rename({"date":"time"})

def preprocess_awst(ds):
    
    """
    Preprocess the hourly sea breeze object dataset by converting time to AWST and filtering to 9am-9pm local time.
    Output is daily occurrences of a sea breeze.
    """

    ds["time"] = pd.to_datetime(ds["time"]) + dt.timedelta(hours=8)
    ds = ds.sel(time=(ds.time.dt.hour >= 9) & (ds.time.dt.hour <= 21))
    ds.attrs["time_zone"] = "AWST"
    ds.attrs["local_time_restriction"] = "0900_2100"
    
    return ds.groupby("time.date").max().rename({"date":"time"})    

def preprocess_utc(ds):
    
    """
    Preprocess the hourly sea breeze object dataset, output is daily occurrences of a sea breeze.
    """
    ds.attrs["time_zone"] = "UTC"
    ds.attrs["local_time_restriction"] = "None"
    return ds.groupby("time.date").max().rename({"date":"time"})    

if __name__ == "__main__":
    
    client = Client(scheduler_file=os.environ["DASK_PBS_SCHEDULER"])

    model = "barra_c_smooth_s2"
    name = "standard"
    variable = "F"
    base_path = "/g/data/ng72/ab4502/sea_breeze_detection/"

    #Load in the hourly sea breeze object dataset and preprocess to daily occurrences in AEST
    ds_aest = xr.open_mfdataset(f"{base_path}/{model}/filters/filtered_mask_{name}_{variable}_*.zarr",
                            engine="zarr",
                            preprocess=preprocess_aest)
    ds_aest["time"] = pd.to_datetime(ds_aest["time"])
    t1 = pd.to_datetime(ds_aest.time.min().values).strftime("%Y%m%d")
    t2 = pd.to_datetime(ds_aest.time.max().values).strftime("%Y%m%d")
    ds_aest.chunk({"time":101}).to_zarr(f"{base_path}/{model}/filters/daily_filtered_{name}_{variable}_aest_{t1}_{t2}.zarr",
                                        mode="w")

    #Same but for AWST 
    ds_awst = xr.open_mfdataset(f"{base_path}/{model}/filters/filtered_mask_{name}_{variable}_*.zarr",
                            engine="zarr",
                            preprocess=preprocess_awst)
    ds_awst["time"] = pd.to_datetime(ds_awst["time"])
    t1 = pd.to_datetime(ds_awst.time.min().values).strftime("%Y%m%d")
    t2 = pd.to_datetime(ds_awst.time.max().values).strftime("%Y%m%d")
    ds_awst.chunk({"time":101}).to_zarr(f"{base_path}/{model}/filters/daily_filtered_{name}_{variable}_awst_{t1}_{t2}.zarr",
                                        mode="w")

    #Same but for UTC
    ds_utc = xr.open_mfdataset(f"{base_path}/{model}/filters/filtered_mask_{name}_{variable}_*.zarr",
                            engine="zarr",
                            preprocess=preprocess_utc)
    ds_utc["time"] = pd.to_datetime(ds_utc["time"])
    t1 = pd.to_datetime(ds_utc.time.min().values).strftime("%Y%m%d")
    t2 = pd.to_datetime(ds_utc.time.max().values).strftime("%Y%m%d")
    ds_utc.chunk({"time":101}).to_zarr(f"{base_path}/{model}/filters/daily_filtered_{name}_{variable}_utc_{t1}_{t2}.zarr",
                                        mode="w")

