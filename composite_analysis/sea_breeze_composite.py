from sea_breeze import load_model_data, sea_breeze_funcs, utils
from dask.distributed import Client
import os
import pandas as pd
import argparse
import xarray as xr
import datetime as dt
import numpy as np
from sea_breeze_analysis.wind_turbine_power_curve import capacity_factor, iec_class2

if __name__ == "__main__":

    # Set up the Dask client
    client = Client(scheduler_file=os.environ["DASK_PBS_SCHEDULER"])

    # Argument parser for the script
    parser = argparse.ArgumentParser(
        prog="Sea Breeze Composite",
    )
    parser.add_argument("t1", type=str, help="Start time (Y-m-d H:M)")
    parser.add_argument("t2", type=str, help="End time (Y-m-d H:M)")
    parser.add_argument("--lat_start", type=float, default=-45, help="Start latitude")
    parser.add_argument("--lat_end", type=float, default=-30, help="End latitude")
    parser.add_argument("--lon_start", type=float, default=135, help="Start longitude")
    parser.add_argument("--lon_end", type=float, default=155, help="End longitude")
    parser.add_argument("--region", type=str, help="Region to define sea breeze days")
    args = parser.parse_args()
    t1 = args.t1
    t2 = args.t2
    lat_slice = slice(args.lat_start, args.lat_end)
    lon_slice = slice(args.lon_start, args.lon_end)
    region = args.region

    print(f"Running sea breeze composite for {region} from {t1} to {t2}")

    # Load U wind and V wind components
    u = load_model_data.load_barra_variable(
        "ua100m",
        t1,
        t2,
        "AUST-04",
        "1hr",
        lat_slice,
        lon_slice,
        chunks={}).chunk({"lat":100,"lon":100,"time":-1})
    v = load_model_data.load_barra_variable(
        "va100m",
        t1,
        t2,
        "AUST-04",
        "1hr",
        lat_slice,
        lon_slice,
        chunks={}).chunk({"lat":100,"lon":100,"time":-1})
    tas = load_model_data.load_barra_variable(
        "tas",
        t1,
        t2,
        "AUST-04",
        "1hr",
        lat_slice,
        lon_slice,
        chunks={}).chunk({"lat":100,"lon":100,"time":-1}).drop_vars("height")         
    ws = np.sqrt(u**2 + v**2)
    cf = (xr.apply_ufunc(iec_class2,ws,dask="parallelized") / 3450.)
    
    if region in ["bunbury"]:
        #For western australia, convert to AWST (UTC+8)
        h = 8
    elif region in  ["gipps","illawara","newcastle","tas","southern","sa"]:
        #For victoria, new south wales, tasmania and south australia convert to AEST (UTC+10)
        h = 10
    else:
        raise ValueError("Region not recognised")

    #Convert the UTC times to AEST (Australian Eastern Standard Time - UTC+10)
    u["time"] = pd.to_datetime(u["time"]) + dt.timedelta(hours=h)
    v["time"] = pd.to_datetime(v["time"]) + dt.timedelta(hours=h)
    ws["time"] = pd.to_datetime(ws["time"]) + dt.timedelta(hours=h)
    cf["time"] = pd.to_datetime(cf["time"]) + dt.timedelta(hours=h)
    tas["time"] = pd.to_datetime(tas["time"]) + dt.timedelta(hours=h)

    # Load the sea breeze days (created by sea_breeze_analysis/analysis_notebooks/diurnal_rez.ipynb)
    sb_days = pd.read_csv("/g/data/ng72/ab4502/sea_breeze_detection/barra_c_smooth_s2/sb_days/"+region+".csv",index_col=0,parse_dates=True)
    sb_days = sb_days.loc[t1:t2]

    #Load the coastal shapes
    shapes = xr.open_dataset("/g/data/ng72/ab4502/coastline_data/rez_coastal_shapes.nc").sel(lat=tas.lat,lon=tas.lon)

    #Get a time series of land and ocean air temps
    land_tas = xr.where(
        (shapes[f"{region}_landsea"]==1) & (shapes["lsm"]==1),tas,np.nan
        ).mean(("lat","lon")).to_dataframe(name="land_tas").drop(columns=["height","crs","region","abbrevs","names"])
    ocean_tas = xr.where(
        (shapes[f"{region}_landsea"]==1) & (shapes["lsm"]==0),tas,np.nan
        ).mean(("lat","lon")).to_dataframe(name="ocean_tas").drop(columns=["height","crs","region","abbrevs","names"])
    t1_str = pd.to_datetime(t1).strftime("%Y%m%d%H%M")
    t2_str = pd.to_datetime(t2).strftime("%Y%m%d%H%M")
    tas_df = pd.concat([land_tas,ocean_tas],axis=1).to_csv(f"/scratch/ng72/ab4502/land_sea_temp_diff/land_ocean_tas_{region}_{t1_str}_{t2_str}.csv")

    # Convert the sea breeze days to datetime
    sb_times = pd.to_datetime(sb_days[sb_days.sb==1].index)
    nonsb_times = pd.to_datetime(sb_days[sb_days.sb==0].index)

    #Subset the data for sea breeze and non-sea breeze days
    sb_u = u.sel(time=np.in1d(u.time.dt.date,sb_times.date))
    sb_v = v.sel(time=np.in1d(v.time.dt.date,sb_times.date))
    sb_ws = ws.sel(time=np.in1d(ws.time.dt.date,sb_times.date))
    sb_cf = cf.sel(time=np.in1d(cf.time.dt.date,sb_times.date))
    sb_tas = tas.sel(time=np.in1d(tas.time.dt.date,sb_times.date))
    nonsb_u = u.sel(time=np.in1d(u.time.dt.date,nonsb_times.date))
    nonsb_v = v.sel(time=np.in1d(v.time.dt.date,nonsb_times.date))
    nonsb_ws = ws.sel(time=np.in1d(ws.time.dt.date,nonsb_times.date))
    nonsb_cf = cf.sel(time=np.in1d(cf.time.dt.date,nonsb_times.date))
    nonsb_tas = tas.sel(time=np.in1d(tas.time.dt.date,nonsb_times.date))

    #Check if the data is empty for non-sea breeze days. Otherwise, calculate the hourly composites
    if (sb_days.sb==0).sum() == 0:
        print(f"No non-sea breeze days found for {region} from {t1} to {t2}")
        exit(0)
    
    else:
        #Group non-sea breeze and sea breeze data by hour, create datasets
        nonsb_u = nonsb_u.groupby("time.hour").mean().persist()
        nonsb_v = nonsb_v.groupby("time.hour").mean().persist()
        nonsb_ws = nonsb_ws.groupby("time.hour").mean().persist()
        nonsb_cf = nonsb_cf.groupby("time.hour").mean().persist()
        nonsb_tas = nonsb_tas.groupby("time.hour").mean().persist()
        nonsb_ds = xr.Dataset({"u":nonsb_u, "v":nonsb_v, "ws":nonsb_ws, "cf":nonsb_cf, "tas":nonsb_tas}).chunk({"lat":-1,"lon":-1})

        #Fix up some attributes
        for v in nonsb_ds.data_vars:
            nonsb_ds[v].attrs["smoothed"] = "False"
        nonsb_ds.attrs["region"] = region
        nonsb_ds.attrs["tz"] = f"UTC+{h}"
        nonsb_ds.attrs["prop_nonsb_days"] = len(nonsb_times) / len(sb_days)

        # Save the result to a nc file
        encoding = {var: {"zlib":False,"least_significant_digit":3, "dtype":np.float32} for var in nonsb_ds.data_vars}
        output_path_nonsb = "/scratch/ng72/ab4502/hourly_composites/barra_c/barra_c_nonsb_"+\
            region+"_"+\
            pd.to_datetime(t1).strftime("%Y%m%d%H%M")+"_"+\
            (pd.to_datetime(t2).strftime("%Y%m%d%H%M"))
        if os.path.isdir(os.path.dirname(output_path_nonsb)):
            pass
        else:
            os.makedirs(os.path.dirname(output_path_nonsb))
        nonsb_ds.to_netcdf(output_path_nonsb+".nc", encoding=encoding, mode="w")    

    #Check if the data is empty for SB days, it not then do the same for sea breeze days
    if sb_days.sb.sum() == 0:
        print(f"No sea breeze days found for {region} from {t1} to {t2}")
        exit(0)
    else:
        #Group by hour
        sb_u = sb_u.groupby("time.hour").mean().persist()
        sb_v = sb_v.groupby("time.hour").mean().persist()
        sb_ws = sb_ws.groupby("time.hour").mean().persist()
        sb_cf = sb_cf.groupby("time.hour").mean().persist()
        sb_tas = sb_tas.groupby("time.hour").mean().persist()

        #Combine into one dataset
        sb_ds = xr.Dataset({"u":sb_u, "v":sb_v, "ws":sb_ws, "cf":sb_cf, "tas":sb_tas}).chunk({"lat":-1,"lon":-1})

        #Fix up some attributes
        for v in sb_ds.data_vars:
            sb_ds[v].attrs["smoothed"] = "False"

        #Add attributes for the proportion of sea breeze and non-sea breeze days. Can be used to calculate the average sea breeze and non-sea breeze days
        sb_ds.attrs["region"] = region
        sb_ds.attrs["prop_sb_days"] = len(sb_times) / len(sb_days)
        sb_ds.attrs["tz"] = f"UTC+{h}"

        # Save the result to a nc file
        encoding = {var: {"zlib":False,"least_significant_digit":3, "dtype":np.float32} for var in sb_ds.data_vars}
        output_path_sb = "/scratch/ng72/ab4502/hourly_composites/barra_c/barra_c_sb_"+\
            region+"_"+\
            pd.to_datetime(t1).strftime("%Y%m%d%H%M")+"_"+\
            (pd.to_datetime(t2).strftime("%Y%m%d%H%M"))
        if os.path.isdir(os.path.dirname(output_path_sb)):
            pass
        else:
            os.makedirs(os.path.dirname(output_path_sb))
        sb_ds.to_netcdf(output_path_sb+".nc", encoding=encoding, mode="w")

        #Close the Dask client
        client.close()
