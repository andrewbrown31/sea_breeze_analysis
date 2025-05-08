import xarray as xr
from sea_breeze import sea_breeze_funcs
from dask.distributed import Client, progress
import argparse
import os

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="AUS2200 fuzzy function",
        description="This program loads hourly change data and combines them into a fuzzy function"
    )
    parser.add_argument("--model",default="aus2200",type=str,help="Model name to save the output under. Could be aus2200 (default) or maybe aus2200_pert")
    args = parser.parse_args()
    model = args.model

    #Set up dask client
    #client = Client()
    client = Client(scheduler_file=os.environ["DASK_PBS_SCHEDULER"])

    #Set up paths to sea_breeze_funcs data output and other inputs
    path = "/g/data/ng72/ab4502/"
    hourly_change_path = path+ "sea_breeze_detection/"+model+"/F_hourly_mjo-*20??_*.zarr"

    #Load the hourly change dataset
    hourly_change_ds = xr.open_mfdataset(
        hourly_change_path,engine="zarr"
        )
    
    #Combine the fuzzy functions
    fuzzy = sea_breeze_funcs.fuzzy_function_combine(
        hourly_change_ds.wind_change,
        hourly_change_ds.q_change,
        hourly_change_ds.t_change,
        combine_method="mean")    
    
    #Save
    to_zarr = fuzzy.chunk({"time":744}).to_zarr(path + "sea_breeze_detection/"+model+"/fuzzy_201301010000_201802282300.zarr",compute=False,mode="w")
    progress(to_zarr.persist())