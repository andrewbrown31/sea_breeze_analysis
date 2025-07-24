from sea_breeze import load_model_data, sea_breeze_funcs, utils
from dask.distributed import Client
import os
import pandas as pd
import argparse
import xarray as xr
import numpy as np
from sea_breeze_analysis.wind_turbine_power_curve import capacity_factor

if __name__ == "__main__":

    # Set up the Dask client
    client = Client(scheduler_file=os.environ["DASK_PBS_SCHEDULER"])
    #client = Client()

    #Set the domain bounds
    parser = argparse.ArgumentParser(
        prog="AUS2200 hourly perturbation composite",
        description="This program calculates the average hourly perturbation of the rotated wind components from a rolling daily mean for AUS2200 data."
    )
    parser.add_argument("t1",type=str,help="Start time (Y-m-d H:M)")
    parser.add_argument("t2",type=str,help="End time (Y-m-d H:M)")
    parser.add_argument("--exp_id",type=str,help="Experiment id for AUS2200 mjo runs")
    parser.add_argument("--u_var",type=str,help="U wind variable name",default="uas")
    parser.add_argument("--v_var",type=str,help="V wind variable name",default="vas")
    args = parser.parse_args()
    u_var = args.u_var
    v_var = args.v_var
    t1 = args.t1
    t2 = args.t2        
    exp_id = args.exp_id

    #Print the times
    print("Calculating hourly perturbation composite for times {} to {}".format(t1, t2))

    # Set the domain bounds
    lat_slice, lon_slice = utils.get_aus_bounds()    

    #Load U wind and V wind components
    u = load_model_data.round_times(
        load_model_data.load_aus2200_variable(
            u_var,
            t1,
            t2,
            exp_id,
            lon_slice,
            lat_slice,
            "10min",
            chunks={"time":1,"lat":-1,"lon":-1},
            staggered="lon"),
              "10min")
    v = load_model_data.round_times(
        load_model_data.load_aus2200_variable(
            v_var,
            t1,
            t2,
            exp_id,
            lon_slice,
            lat_slice,
            "10min",
            chunks={"time":1,"lat":-1,"lon":-1},
            staggered="lat"),
              "10min")
    angle_ds = load_model_data.get_coastline_angle_kernel(
        compute=False,
        path_to_load="/g/data/ng72/ab4502/coastline_data/aus2200.nc",
        lat_slice=lat_slice,
        lon_slice=lon_slice)
    
    #Just do the hourly data
    v = v.sel(time=v.time.dt.minute==0)
    u = u.sel(time=u.time.dt.minute==0)

    # Rotate the wind components
    uprime, vprime = sea_breeze_funcs.rotate_wind(u,v,angle_ds.angle_interp)

    #Calculate the wind speed, and the wind power capacity factor
    ws = np.sqrt(u**2 + v**2)
    cf = capacity_factor(ws)

    #Group the data by time and calculate the mean for each hour of the day
    hourly = xr.Dataset(
        {"vprime":vprime.groupby("time.hour").mean(),
        "cf":cf.groupby("time.hour").mean()})
    
    # Save the result to a zarr file
    encoding = {var: {"zlib":False,"least_significant_digit":3, "dtype":np.float32} for var in hourly.data_vars}
    output_path = "/g/data/ng72/ab4502/hourly_composites/aus2200/aus2200_hourly_"+\
        u_var+"_"+v_var+"_"+\
        pd.to_datetime(t1).strftime("%Y%m%d%H%M")+"_"+\
        (pd.to_datetime(t2).strftime("%Y%m%d%H%M"))
    if os.path.isdir(os.path.dirname(output_path)):
        pass
    else:
        os.makedirs(os.path.dirname(output_path))
    hourly.to_netcdf(output_path+".nc", encoding=encoding, mode="w")    

    #Calculate the rolling daily mean
    print(vprime.shape)
    vprime_rolling = vprime.rolling(dim={"time":24},min_periods=12,center=True).mean().chunk({
        "lat":vprime.chunksizes["lat"][0],
        "lon":vprime.chunksizes["lon"][0]})    
    cf_rolling = cf.rolling(dim={"time":24},min_periods=12,center=True).mean().chunk({
        "lat":cf.chunksizes["lat"][0],
        "lon":cf.chunksizes["lon"][0]})
    
    #Calculate the hourly perturbation from the rolling daily mean
    vprime_pert = vprime - vprime_rolling
    cf_pert = cf - cf_rolling

    #Calculate the mean hourly perturbation for each hour of the day
    hourly_pert = vprime_pert.groupby("time.hour").mean()
    cf_hourly_pert = cf_pert.groupby("time.hour").mean()

    # Convert to a DataSet
    hourly_pert = xr.Dataset(
        {"vprime_pert":hourly_pert,
        "cf_pert":cf_hourly_pert})

    # Save the result to a zarr file
    encoding = {var: {"zlib":False,"least_significant_digit":3, "dtype":np.float32} for var in hourly_pert.data_vars}
    output_path = "/g/data/ng72/ab4502/hourly_composites/aus2200/aus2200_hourly_perturbation_"+\
        u_var+"_"+v_var+"_"+\
        pd.to_datetime(t1).strftime("%Y%m%d%H%M")+"_"+\
        (pd.to_datetime(t2).strftime("%Y%m%d%H%M"))
    if os.path.isdir(os.path.dirname(output_path)):
        pass
    else:
        os.makedirs(os.path.dirname(output_path))
    hourly_pert.to_netcdf(output_path+".nc", encoding=encoding, mode="w")

    client.close()
    
