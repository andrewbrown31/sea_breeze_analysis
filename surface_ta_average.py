from sea_breeze import load_model_data, utils
from dask.distributed import Client
import xarray as xr
import os

if __name__ == "__main__":

    client = Client(scheduler_file=os.environ["DASK_PBS_SCHEDULER"])

    start_times = ["2013-01-01 00:00","2016-01-01 00:00","2018-01-01 00:00"]
    end_times = ["2013-02-28 23:00","2016-02-29 23:00","2018-02-28 23:00"]
    exp_id = ["mjo-neutral2013","mjo-elnino2016","mjo-lanina2018"]

    lat_slice,lon_slice = utils.get_aus_bounds()

    era5_ls = []
    barra_r_ls = []
    barra_c_ls = []
    aus2200_ls = []

    for i in range(len(start_times)):

        print(f"Processing {start_times[i]} to {end_times[i]}")

        print("ERA5")
        era5_ta = load_model_data.load_era5_variable(
            ["2t"],
            start_times[i],
            end_times[i],
            lon_slice,
            lat_slice,
            chunks={"time":-1}
    )["2t"]["t2m"]
        era5_ls.append(era5_ta.resample({"time":"1D"}).max().persist())

        print("BARRA-R")
        barrar_ta = load_model_data.load_barra_variable(
            "tas",
            start_times[i],
            end_times[i],
            "AUS-11",
            "1hr",
            lat_slice,
            lon_slice,
            chunks={"time":-1}
        )
        barra_r_ls.append(barrar_ta.resample({"time":"1D"}).max().persist())

        print("BARRA-C")
        barrac_ta = load_model_data.load_barra_variable(
            "tas",
            start_times[i],
            end_times[i],
            "AUST-04",
            "1hr",
            lat_slice,
            lon_slice,
            chunks={"time":-1}
        )
        barra_c_ls.append(barrac_ta.resample({"time":"1D"}).max().persist())

        print("AUS2200")
        aus2200_ta = load_model_data.load_aus2200_variable(
            "ta",
            start_times[i],
            end_times[i],
            exp_id[i],
            lon_slice,
            lat_slice,
            "1hr",
            smooth=False,
            hgt_slice=slice(0,10),
            chunks=-1).sel(lev=5) 
        aus2200_ls.append(aus2200_ta.resample({"time":"1D"}).max().persist())

    outpath = "/g/data/ng72/ab4502/hourly_composites/"
    xr.concat(era5_ls,dim="time").mean("time").to_netcdf(outpath+"era5/era5_ta_daily_max_2013_2016_2018.nc")
    xr.concat(barra_r_ls,dim="time").mean("time").to_netcdf(outpath+"barra_r/barra_r_ta_daily_max_2013_2016_2018.nc")
    xr.concat(barra_c_ls,dim="time").mean("time").to_netcdf(outpath+"barra_c/barra_c_ta_daily_max_2013_2016_2018.nc")
    xr.concat(aus2200_ls,dim="time").mean("time").to_netcdf(outpath+"aus2200/aus2200_ta_daily_max_2013_2016_2018.nc")