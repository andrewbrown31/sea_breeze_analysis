import xarray as xr
from dask.distributed import Client
import logging
import os

def load(model,diagnostic,settings="no_hourly_change",chunks={"time":10}):

    if settings in ["no_hourly_change","smooth1","smooth2"]:

        ds = xr.open_mfdataset("/g/data/ng72/ab4502/sea_breeze_detection/"+\
                        model+\
                        "/filters/filtered_mask_"+settings+"_"+\
                        diagnostic+\
                        "_*.zarr/",
                    chunks=chunks, engine="zarr")   

    else:

        ds = xr.open_mfdataset("/scratch/ng72/ab4502/sea_breeze_detection/"+\
                        model+\
                        "/filters/filtered_mask_"+settings+"_"+\
                        diagnostic+\
                        "_*.zarr/",
                    chunks=chunks, engine="zarr")   

    return ds    

if __name__ == "__main__":

    logging.getLogger("flox").setLevel(logging.WARNING)

    # Set up the Dask client
    client = Client(scheduler_file=os.environ["DASK_PBS_SCHEDULER"])

    # models = ["aus2200_smooth_s4","barra_c_smooth_s2","barra_r","era5"]
    # chunks = [{"time":1},{"time":1},{"time":100},{"time":100}]
    # diags = ["F","fuzzy","sbi"]

    outpath = "/g/data/ng72/ab4502/hourly_composites/"

    # for diag in diags:
    #     for model, chunk in zip(models, chunks):
    #         if (model in ["barra_c_smooth_s2", "barra_r"]) & (diag == "sbi"):
    #             pass
    #         else:
    #             print(f"Processing {model} with diagnostic {diag}")
    #             outname = outpath + model + "/" + diag + "_hourly_composite.nc"
    #             temp = load(model, diag, chunk)
    #             temp.groupby("time.hour").mean().mask.to_netcdf(outname, mode="w", compute=True)

    #Process the AUS2200 filtering sensitivity experiments
    # model = "aus2200_smooth_s4"
    # chunk = {"time":1}
    # diag = "F"
    # settings = ["area1","area2","aspect1","aspect2","land_sea1","prop_speed1","orientation1","orientation2","percentile1","percentile2"]
    # for setting in settings:
    #     print(f"Processing {model} with diagnostic {diag} and settings {setting}")
    #     outname = outpath + model + "/" + diag + "_" + setting + "_hourly_composite.nc"
    #     temp = load(model, diag, chunks=chunk, settings=setting)
    #     temp.groupby("time.hour").mean().mask.to_netcdf(outname, mode="w", compute=True)    

    # Process the AUS2200 smoothing sensitivity experiments
    model = "aus2200_smooth_s2"
    chunk = {"time":1}
    diag = "F"
    setting = "smooth1"
    print(f"Processing {model} with diagnostic {diag} and settings {setting}")
    outname = outpath + model + "/" + diag + "_" + setting + "_hourly_composite.nc"
    temp = load(model, diag, chunks=chunk, settings=setting)
    temp.groupby("time.hour").mean().mask.to_netcdf(outname, mode="w", compute=True)    

    # Process the AUS2200 smoothing sensitivity experiments
    model = "aus2200_smooth_s6"
    chunk = {"time":1}
    diag = "F"
    setting = "smooth2"
    print(f"Processing {model} with diagnostic {diag} and settings {setting}")
    outname = outpath + model + "/" + diag + "_" + setting + "_hourly_composite.nc"
    temp = load(model, diag, chunks=chunk, settings=setting)
    temp.groupby("time.hour").mean().mask.to_netcdf(outname, mode="w", compute=True)        
