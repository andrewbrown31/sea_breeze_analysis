from sea_breeze import load_model_data, sea_breeze_funcs, utils
from dask.distributed import Client
import os
import pandas as pd
import argparse
import xarray as xr
import datetime as dt
import numpy as np
from sea_breeze_analysis.wind_turbine_power_curve import capacity_factor, iea_ref_10mw

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
    cf = (xr.apply_ufunc(iea_ref_10mw,ws,dask="parallelized") / 10638.301)

    #Load the coastline angles
    theta = load_model_data.get_coastline_angle_kernel(
        compute=False,path_to_load="/g/data/ng72/ab4502/coastline_data/barra_c.nc",lon_slice=lon_slice,lat_slice=lat_slice)["angle"]
    
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

    #Load the coastal shapes
    shapes = xr.open_dataset("/g/data/ng72/ab4502/coastline_data/rez_coastal_shapes.nc").sel(lat=tas.lat,lon=tas.lon)

    #Calculate u' and v' (alongshore and offshore wind components)
    up,vp = sea_breeze_funcs.rotate_wind(u,v,theta)

    #Define list of offshore, onshore, alongshore (left) and alongshore (right) days
    shape_vp = xr.where(shapes[region],vp,np.nan).mean(("lat","lon")).sel(time=vp.time.dt.hour==5)
    shape_up = xr.where(shapes[region],up,np.nan).mean(("lat","lon")).sel(time=up.time.dt.hour==5)
    onshore_dates = [pd.to_datetime(t).date() for t in shape_vp.where(shape_vp>0).dropna("time",how="all")["time"].values]
    offshore_dates = [pd.to_datetime(t).date() for t in shape_vp.where(shape_vp<=0).dropna("time",how="all")["time"].values]
    right_dates = [pd.to_datetime(t).date() for t in shape_up.where(shape_up>0).dropna("time",how="all")["time"].values]
    left_dates = [pd.to_datetime(t).date() for t in shape_up.where(shape_up<=0).dropna("time",how="all")["time"].values]

    #Calculate hourly time series of cf for the onshore, offshore, alongshore left and alongshore right days
    shape_cf = xr.where(shapes[region],cf,np.nan).mean(("lat","lon")).to_dataframe(name="cf")[["cf"]]
    onshore_cf = shape_cf[np.in1d(shape_cf.index.date,onshore_dates)]
    offshore_cf = shape_cf[np.in1d(shape_cf.index.date,offshore_dates)]
    right_cf = shape_cf[np.in1d(shape_cf.index.date,right_dates)]
    left_cf = shape_cf[np.in1d(shape_cf.index.date,left_dates)]
    onshore_right_cf = shape_cf[np.in1d(shape_cf.index.date,onshore_dates) & np.in1d(shape_cf.index.date,right_dates)]
    onshore_left_cf = shape_cf[np.in1d(shape_cf.index.date,onshore_dates) & np.in1d(shape_cf.index.date,left_dates)]
    offshore_right_cf = shape_cf[np.in1d(shape_cf.index.date,offshore_dates) & np.in1d(shape_cf.index.date,right_dates)]
    offshore_left_cf = shape_cf[np.in1d(shape_cf.index.date,offshore_dates) & np.in1d(shape_cf.index.date,left_dates)]

    #Save a daily time series of 5 am u' and v'

    #Save a daily time series of max, min, mean cf

    #Close the Dask client
    client.close()
