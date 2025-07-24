from sea_breeze import load_obs, sea_breeze_funcs, load_model_data, sea_breeze_filters
import numpy as np
import xarray as xr
import pandas as pd
from dask.distributed import Client, progress

if __name__ == "__main__":

    #Initiate distributed dask client on the Gadi HPC
    #client = Client(scheduler_file=os.environ["DASK_PBS_SCHEDULER"])
    client = Client()

    #Set the domain bounds
    lat_slice=slice(-45.7,-6.9)
    lon_slice=slice(108,158.5)    

    #Set time slice and model name    
    t1_list = ["2013-01-01 00:00","2013-02-01 00:00","2016-01-01 00:00","2016-02-01 00:00","2018-01-01 00:00","2018-02-01 00:00"]
    t2_list = ["2013-01-31 23:00","2013-02-28 23:00","2016-01-31 23:00","2016-02-29 23:00","2018-01-31 23:00","2018-02-28 23:00"]

    obs_fuzzy_list = []

    for i in range(len(t1_list)):
        stn_obs_list = []
        for state in ["NSW-ACT","SA","TAS-ANT","VIC","QLD","WA","NT"]:
            print(f"Loading {state} data from {t1_list[i]} to {t2_list[i]}")

            #Load station data for chosen period
            stn_obs = load_obs.load_half_hourly_stn_obs(
                state,slice(t1_list[i],t2_list[i])
            )[["u","v","temp","hus","latitude","longitude","name"]]

            #Assign coordinates to the data
            stn_obs = stn_obs.assign_coords({"latitude":stn_obs.latitude,"longitude":stn_obs.longitude})

            #Slice to the domain
            stn_obs = stn_obs.sel(station=
                (stn_obs.latitude>=lat_slice.start) &\
                (stn_obs.latitude<=lat_slice.stop) &\
                (stn_obs.longitude>=lon_slice.start) &\
                (stn_obs.longitude<=lon_slice.stop))

            #Add to the list
            stn_obs_list.append(stn_obs.sel(time=stn_obs.time.dt.minute==0))

        # Concatenate the list of station data for each state
        stn_obs = xr.concat(stn_obs_list, dim="station")

        # Load the coastline angle data for AUS2200
        angle_ds = load_model_data.get_coastline_angle_kernel(
            None,
            compute=False,
            lat_slice=lat_slice,
            lon_slice=lon_slice,
            path_to_load="/g/data/ng72/ab4502/coastline_data/aus2200.nc")
        stn_obs["angles"] = angle_ds["angle_interp"].sel(
            xr.Dataset({"lat":stn_obs.latitude,"lon":stn_obs.longitude}),method="nearest"
            )

        # Calculate the hourly change in temperature, humidity and wind
        F_hourly = sea_breeze_funcs.hourly_change(
            stn_obs["hus"],
            stn_obs["temp"],
            stn_obs["u"],
            stn_obs["v"],   
            stn_obs["angles"],
            spatial_dims=["station"],
            spatial_chunks=[-1]
        )

        # Calculate the fuzzy logic algorithm for sea breeze probability
        obs_fuzzy = sea_breeze_funcs.fuzzy_function_combine(
            F_hourly.wind_change,
            F_hourly.q_change,
            F_hourly.t_change,
            combine_method="mean")
        obs_fuzzy_list.append(obs_fuzzy)

    # Concatenate the list of fuzzy data for each time period
    obs_combined = xr.concat(obs_fuzzy_list,"time").chunk({"time":-1})
    
    #Calculate the 99.5th percentile of the fuzzy data, and save to csv
    percentile = np.array(sea_breeze_filters.percentile(xr.concat(obs_fuzzy_list,"time").chunk({"time":-1})))[0]
    pd.DataFrame({"fuzzy":percentile},index=["obs"]).to_csv("/g/data/ng72/ab4502/sea_breeze_detection/percentiles/stn_obs_percentiles_99.5_2013_2018.csv")

    #Save the fuzzy data to zarr
    to_zarr = obs_combined.to_zarr(
        "/g/data/ng72/ab4502/sea_breeze_detection/stn_obs/fuzzy_201301010000_201802282300.zarr/",compute=True,mode="w"
        )
