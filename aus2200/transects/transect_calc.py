import pandas as pd
import xarray as xr
from sea_breeze.load_model_data import load_aus2200_variable, interp_model_level_to_z, load_aus2200_static, aus2200_hybrid_height_calc
from sea_breeze.sea_breeze_funcs import rotate_wind
from dask.distributed import Client
from metpy.interpolate import cross_section
import glob
import numpy as np
import argparse

if __name__ == "__main__":

    client = Client()

    parser = argparse.ArgumentParser(description="Calculate sea breeze index along a transect for AUS2200 data")
    parser.add_argument("region",type=str,help="Region name for transect (e.g. 'gipps')")
    parser.add_argument("--start_date",type=str,help="Start date for analysis (YYYY-MM-DD)",default=None)
    parser.add_argument("--end_date",type=str,help="End date for analysis (YYYY-MM-DD)",default=None)

    args = parser.parse_args()
    region = args.region
    start_time = pd.to_datetime(args.start_date)
    end_time = pd.to_datetime(args.end_date)
    out_start_time = start_time.strftime("%Y%m%d")
    out_end_time = end_time.strftime("%Y%m%d")

    #Set lat lon slices for each region
    if region=="illawara":
        lat_slice = slice(-36,-32.5)
        lon_slice = slice(148.5,153.5)
    else:
        raise ValueError("Region not recognised")

    #Load transects from set_up_transects.ipynb
    transect_files = glob.glob("/g/data/ng72/ab4502/coastline_data/transect_points/"+region+"*.csv")
    if len(transect_files)==0:
        raise ValueError("No transect files found for region "+region)

    #Load ancillary AUS2200 static data
    orog, lsm = load_aus2200_static("mjo-elnino2016",lon_slice,lat_slice)

    #Initialise dict with empty list for each transect
    # va_trans_all = dict([(tf.split("/")[-1].replace(".csv",""),[]) for tf in transect_files])
    # ua_trans_all = dict([(tf.split("/")[-1].replace(".csv",""),[]) for tf in transect_files])
    # zmla_trans_all = dict([(tf.split("/")[-1].replace(".csv",""),[]) for tf in transect_files])
    # theta_trans_all = dict([(tf.split("/")[-1].replace(".csv",""),[]) for tf in transect_files])

    #Loop over all the SB dates
    #for d in pd.to_datetime(sb_day.time.values)[0:3]:
    print("Processing date:",start_time," to ",end_time)

    #Determine experiment ID based on year
    if start_time.year==2013:
        exp_id="mjo-neutral2013"
    elif start_time.year==2016:
        exp_id="mjo-elnino2016"
    elif start_time.year==2018:
        exp_id="mjo-lanina2018"

    if (start_time.month==1) and (start_time.day==1):
        start_time = start_time.replace(hour=1)

    #Load necessary AUS2200 data
    ua = load_aus2200_variable(
        "ua",
        start_time,
        end_time,
        exp_id,
        lon_slice,
        lat_slice,
        "1hr",
        hgt_slice=slice(0,5000),
        chunks={"time":-1,"lev":-1},
        interp_hgts=False,
        staggered="lon")
    va = load_aus2200_variable(
        "va",
        start_time,
        end_time,
        exp_id,
        lon_slice,
        lat_slice,
        "1hr",
        hgt_slice=slice(0,5000),
        chunks={"time":-1,"lev":-1},
        interp_hgts=False,
        staggered="lat")
    theta = load_aus2200_variable(
        "theta",
        start_time,
        end_time,
        exp_id,
        lon_slice,
        lat_slice,
        "1hr",
        hgt_slice=slice(0,5000),
        chunks={"time":-1,"lev":-1},
        interp_hgts=False) 
    wa = load_aus2200_variable(
        "wa",
        start_time,
        end_time,
        exp_id,
        lon_slice,
        lat_slice,
        "1hr",
        hgt_slice=slice(0,5000),
        chunks={"time":-1,"lev":-1},
        interp_hgts=False)
    zmla = load_aus2200_variable(
        "zmla",
        start_time,
        end_time,
        exp_id,
        lon_slice,
        lat_slice,
        "1hr",
        chunks={},
        interp_hgts=False,
        staggered="time") + orog

    #Interpolate to height above sea level, mask data below topography
    Z_agl_rho = xr.open_zarr("/g/data/ng72/ab4502/sea_breeze_detection/aus2200_z_agl_rho.zarr/",
                        chunks={}).Z_agl.sel(lev=ua.lev,lat=ua.lat,lon=ua.lon).persist()
    Z_agl_theta = xr.open_zarr("/g/data/ng72/ab4502/sea_breeze_detection/aus2200_z_agl_theta.zarr/",
                        chunks={}).Z_agl.sel(lev=theta.lev,lat=theta.lat,lon=theta.lon).persist()

    ua = interp_model_level_to_z(
        Z_agl_rho-orog,
        ua.chunk({"lev":-1}),
        "lev",
        ua.lev.values,
        model="AUS2200"
        )        
    ua = xr.where(((Z_agl_rho.rename({"lev":"height"})-orog)>0),ua,np.nan)

    va = interp_model_level_to_z(
        Z_agl_rho-orog,
        va.chunk({"lev":-1}),
        "lev",
        va.lev.values,
        model="AUS2200"
        )        
    va = xr.where((Z_agl_rho.rename({"lev":"height"})-orog>0),va,np.nan)

    theta = interp_model_level_to_z(
        Z_agl_theta-orog,
        theta.chunk({"lev":-1}),
        "lev",
        theta.lev.values,
        model="AUS2200"
        )        
    theta = xr.where((Z_agl_theta.rename({"lev":"height"})-orog>0),theta,np.nan).persist()

    wa = interp_model_level_to_z(
        Z_agl_theta-orog,
        wa.chunk({"lev":-1}),
        "lev",
        wa.lev.values,
        model="AUS2200"
        )        
    wa = xr.where((Z_agl_theta.rename({"lev":"height"})-orog>0),wa,np.nan).persist()

    ua = ua.interp({"height":theta.height}).persist()
    va = va.interp({"height":theta.height}).persist()
    zmla = zmla.persist()

    #Loop over all transects in the region
    for tf in transect_files:

        #Load transect points
        transect_coords = pd.read_csv(tf)
        transect_name = tf.split("/")[-1].replace(".csv","")

        #Extract start and end points
        start_lat = transect_coords.start_lat
        start_lon = transect_coords.start_lon
        end_lat = transect_coords.end_lat
        end_lon = transect_coords.end_lon

        #Calculate length of transect and number of steps to extract along the line
        D = np.sqrt((start_lat[0]-end_lat[0])**2 + (start_lon[0]-end_lon[0])**2)
        steps = ((D) * 100) / 2.2

        #Loop over each transect and extract cross sections
        ua_trans = []
        va_trans = []
        zmla_trans = []
        theta_trans = []
        wa_trans = []
        for i in np.arange(len(start_lat)):

            ua_trans.append(cross_section(
                xr.Dataset({"u":ua}).metpy.parse_cf().u,
                [start_lat[i],start_lon[i]],[end_lat[i],end_lon[i]],steps=steps))

            va_trans.append(cross_section(
                xr.Dataset({"v":va}).metpy.parse_cf().v,
                [start_lat[i],start_lon[i]],[end_lat[i],end_lon[i]],steps=steps))

            zmla_trans.append(cross_section(
                xr.Dataset({"zmla":zmla}).metpy.parse_cf().zmla,
                [start_lat[i],start_lon[i]],[end_lat[i],end_lon[i]],steps=steps))

            theta_trans.append(cross_section(
                xr.Dataset({"theta":theta}).metpy.parse_cf().theta,
                [start_lat[i],start_lon[i]],[end_lat[i],end_lon[i]],steps=steps))

            wa_trans.append(cross_section(
                xr.Dataset({"wa":wa}).metpy.parse_cf().wa,
                [start_lat[i],start_lon[i]],[end_lat[i],end_lon[i]],steps=steps))

        ua_trans = xr.concat(ua_trans,dim="transect")
        va_trans = xr.concat(va_trans,dim="transect")
        zmla_trans = xr.concat(zmla_trans,dim="transect")
        theta_trans = xr.concat(theta_trans,dim="transect")
        wa_trans = xr.concat(wa_trans,dim="transect")

        # ua_trans_all[transect_name].append(ua_trans)
        # va_trans_all[transect_name].append(va_trans)
        # zmla_trans_all[transect_name].append(zmla_trans)
        # theta_trans_all[transect_name].append(theta_trans)

    #Save all transect data to zarr files
#    transect_names = [tf.split("/")[-1].replace(".csv","") for tf in transect_files]

        #for transect_name in transect_names:
        ua_trans.chunk({"transect": -1, "time": -1}).drop_vars("metpy_crs").to_zarr(
            f"/g/data/ng72/ab4502/sea_breeze_detection/transects/aus2200/{transect_name}_{out_start_time}_{out_end_time}_ua.zarr", mode="w")
        va_trans.chunk({"transect": -1, "time": -1}).drop_vars("metpy_crs").to_zarr(
            f"/g/data/ng72/ab4502/sea_breeze_detection/transects/aus2200/{transect_name}_{out_start_time}_{out_end_time}_va.zarr", mode="w")
        zmla_trans.chunk({"transect": -1, "time": -1}).drop_vars("metpy_crs").to_zarr(
            f"/g/data/ng72/ab4502/sea_breeze_detection/transects/aus2200/{transect_name}_{out_start_time}_{out_end_time}_zmla.zarr", mode="w")
        theta_trans.chunk({"transect": -1, "time": -1}).drop_vars("metpy_crs").to_zarr(
            f"/g/data/ng72/ab4502/sea_breeze_detection/transects/aus2200/{transect_name}_{out_start_time}_{out_end_time}_theta.zarr", mode="w")
        wa_trans.chunk({"transect": -1, "time": -1}).drop_vars("metpy_crs").to_zarr(
            f"/g/data/ng72/ab4502/sea_breeze_detection/transects/aus2200/{transect_name}_{out_start_time}_{out_end_time}_wa.zarr", mode="w")


    #Save orog and lsm transects
    for tf in transect_files:

            #Load transect points
            transect_coords = pd.read_csv(tf)
            transect_name = tf.split("/")[-1].replace(".csv","")

            #Extract start and end points
            start_lat = transect_coords.start_lat
            start_lon = transect_coords.start_lon
            end_lat = transect_coords.end_lat
            end_lon = transect_coords.end_lon

            D = np.sqrt((start_lat[0]-end_lat[0])**2 + (start_lon[0]-end_lon[0])**2)
            steps = ((D) * 100) / 2.2

            orog_cross = []
            lsm_cross = []
            for i in np.arange(len(start_lat)):
                orog_cross.append(cross_section(
                        xr.Dataset({"orog":orog}).metpy.parse_cf().orog,
                        [start_lat[i],start_lon[i]],[end_lat[i],end_lon[i]],steps=steps))

                lsm_cross.append(cross_section(
                        xr.Dataset({"lsm":lsm}).metpy.parse_cf().lsm,
                        [start_lat[i],start_lon[i]],[end_lat[i],end_lon[i]],steps=steps))

            xr.concat(orog_cross,dim="transect").drop_vars("metpy_crs").to_zarr(f"/g/data/ng72/ab4502/sea_breeze_detection/transects/aus2200/{transect_name}_orog.zarr", mode="w")
            xr.concat(lsm_cross,dim="transect").drop_vars("metpy_crs").to_zarr(f"/g/data/ng72/ab4502/sea_breeze_detection/transects/aus2200/{transect_name}_lsm.zarr", mode="w")

    client.close()