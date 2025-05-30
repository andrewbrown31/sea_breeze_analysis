from sea_breeze import load_model_data, sea_breeze_funcs, utils
from dask.distributed import Client
import os
import pandas as pd
import argparse
import xarray as xr
import numpy as np

if __name__ == "__main__":

    # Set up the Dask client
    client = Client(scheduler_file=os.environ["DASK_PBS_SCHEDULER"])

    #Set the domain bounds
    parser = argparse.ArgumentParser(
        prog="BARRA-C hourly perturbation composite",
        description="This program calculates the average hourly perturbation of the rotated wind components from a rolling daily mean for BARRA-C data."
    )
    parser.add_argument("t1",type=str,help="Start time (Y-m-d H:M)")
    parser.add_argument("t2",type=str,help="End time (Y-m-d H:M)")
    parser.add_argument("--u_var",type=str,help="U wind variable name",default="uas")
    parser.add_argument("--v_var",type=str,help="V wind variable name",default="vas")
    args = parser.parse_args()
    u_var = args.u_var
    v_var = args.v_var
    t1 = args.t1
    t2 = args.t2        

    #Print the times
    print("Calculating hourly perturbation composite for times {} to {}".format(t1, t2))

    # Set the domain bounds
    lat_slice, lon_slice = utils.get_aus_bounds()    

    #Load U wind and V wind components
    uas = load_model_data.load_barra_variable(
        u_var,
        t1,
        t2,
        "AUST-04",
        "1hr",
        lat_slice,
        lon_slice,
        chunks={"time":-1,"lat":{},"lon":{}})
    vas = load_model_data.load_barra_variable(
        v_var,
        t1,
        t2,
        "AUST-04",
        "1hr",
        lat_slice,
        lon_slice,
        chunks={"time":-1,"lat":{},"lon":{}})
    angle_ds = load_model_data.get_coastline_angle_kernel(
        compute=False,
        path_to_load="/g/data/ng72/ab4502/coastline_data/barra_c.nc",
        lat_slice=lat_slice,
        lon_slice=lon_slice)
    
    # Rotate the wind components
    uprime, vprime = sea_breeze_funcs.rotate_wind(uas,vas,angle_ds.angle_interp)

    #TODO, as well as calculating the vprime perturbation, also calculate the wind speed perturbation, and the wind power capacity factor perturbation

    #Calculate the rolling daily mean
    vprime_rolling = vprime.rolling(dim={"time":24},min_periods=12,center=True).mean().chunk({
        "lat":vprime.chunksizes["lat"][0],
        "lon":vprime.chunksizes["lon"][0]})    
    
    #Calculate the hourly perturbation from the rolling daily mean
    vprime_pert = vprime - vprime_rolling

    #Calculate the mean hourly perturbation for each hour of the day
    hourly_pert = vprime_pert.groupby("time.hour").mean()

    # Convert to a DataSet
    hourly_pert = xr.Dataset({"vprime_pert":hourly_pert})

    # Save the result to a zarr file
    encoding = {var: {"zlib":False,"least_significant_digit":3, "dtype":np.float32} for var in hourly_pert.data_vars}
    output_path = "/g/data/ng72/ab4502/hourly_composites/barra_c/barra_c_hourly_perturbation_"+\
        u_var+"_"+v_var+"_"+\
        pd.to_datetime(t1).strftime("%Y%m%d%H%M")+"_"+\
        (pd.to_datetime(t2).strftime("%Y%m%d%H%M"))
    if os.path.isdir(os.path.dirname(output_path)):
        pass
    else:
        os.makedirs(os.path.dirname(output_path))
    hourly_pert.to_netcdf(output_path+".nc", encoding=encoding, mode="w")
    #hourly_pert.to_zarr(output_path+".zarr", mode="w")

    client.close()
    