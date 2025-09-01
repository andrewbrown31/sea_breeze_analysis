from dask.distributed import Client
import xarray as xr
import numpy as np
from rasterio import features
from affine import Affine
import geopandas as gpd 
import fiona 
import pandas as pd 
import datetime as dt
import os
import argparse
import skimage
from sea_breeze_analysis.wind_turbine_power_curve import capacity_factor, iec_class2
from sea_breeze import load_model_data, sea_breeze_funcs, utils

def transform_from_latlon(lat, lon):
    lat = np.asarray(lat)
    lon = np.asarray(lon)
    trans = Affine.translation(lon[0], lat[0])
    scale = Affine.scale(lon[1] - lon[0], lat[1] - lat[0])
    return trans * scale

def rasterize(shapes, coords, fill=np.nan, **kwargs):
    """Rasterize a list of (geometry, fill_value) tuples onto the given
    xray coordinates. This only works for 1d latitude and longitude
    arrays.
    """
    transform = transform_from_latlon(coords['lat'], coords['lon'])
    out_shape = (len(coords['lat']), len(coords['lon']))
    raster = features.rasterize(shapes, out_shape=out_shape,
                                fill=fill, transform=transform,
                                dtype=float, **kwargs)
    return xr.DataArray(raster, coords=coords, dims=('lat', 'lon'))

if __name__ == "__main__":


    # Set up the Dask client
    client = Client(scheduler_file=os.environ["DASK_PBS_SCHEDULER"])

    #Set the domain bounds
    parser = argparse.ArgumentParser(
        prog="BARRA-C capacity factor time series",
        description="This program calculates the a time series of mean capacity factor from BARRA-C data."
    )
    parser.add_argument("t1",type=str,help="Start time (Y-m-d H:M)")
    parser.add_argument("t2",type=str,help="End time (Y-m-d H:M)")
    parser.add_argument("--u_var",type=str,help="U wind variable name",default="ua100m")
    parser.add_argument("--v_var",type=str,help="V wind variable name",default="va100m")
    args = parser.parse_args()
    u_var = args.u_var
    v_var = args.v_var
    t1 = args.t1
    t2 = args.t2    
    start_time = dt.datetime.strptime(t1, "%Y-%m-%d %H:%M")
    end_time = dt.datetime.strptime(t2, "%Y-%m-%d %H:%M")    

    #BARRA-C or BARRA-R?
    lat_slice, lon_slice = utils.get_aus_bounds()   
    u = load_model_data.load_barra_variable(
        "ua100m",
        t1,
        t2,
        "AUST-04",
        "1hr",
        lat_slice,
        lon_slice,
        chunks={"time":1,"lat":-1,"lon":-1})
    v = load_model_data.load_barra_variable(
        "va100m",
        t1,
        t2,
        "AUST-04",
        "1hr",
        lat_slice,
        lon_slice,
        chunks={"time":1,"lat":-1,"lon":-1})    

    #Calculate the wind speed, and the wind power capacity factor
    ws = np.sqrt(u**2 + v**2)
    print("Calculating capacity factor for times {} to {}".format(t1, t2))
    cf = (xr.apply_ufunc(iec_class2,ws,dask="parallelized") / 3450.).persist()
    #cf_daily = cf.resample({"time":"1D"}).mean()

    #Load the REZ boundaries
    rez=gpd.read_file("/g/data/ng72/ab4502/Indicative_REZ_boundaries_2024_GIS_data.kml")

    #Shape into the xarray dataset of BARRA
    shapes = [(shape, n) for n, shape in enumerate(rez.geometry)]
    raster = rasterize(shapes,{"lat":ws.lat,"lon":ws.lon},fill=np.nan)

    #TODO
    # - Decide on how we are averaging over the NEM. Avg over all at once, or by REZ? Or by state?
    # - Use all REZs or just ones that are projected to have wind farms? Onshore and offshore?
    # - When averaging over NEM, do we need to weight by area of the REZ?

    #Find area of each REZ
#    rp_ls=skimage.measure.regionprops(xr.where(raster.isnull(),0,raster+1).astype(int).values)
#    area = [rp.area for rp in rp_ls]
#    rez["area"] = area

    #Calculate the mean capacity factor grouped by REZ
    #df = cf.drop_vars(["height","crs"]).groupby(raster).mean().to_dataframe(name="cf")

    #Calculate the mean capacity factor over all REZs
    df = xr.where((~raster.isnull()),cf,np.nan).mean(("lat","lon")).drop_vars(["height","crs"]).to_dataframe(name="NEM")

    #Save the results to a CSV file
    #df.to_csv(f"/g/data/ng72/ab4502/capacity_factor/hourly_wind_ts_barra_c_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}_nem.csv", index=True)

    #Calculate the mean capacity factor for each state and save to CSV
    print("Calculating capacity factor for each state")
    rez_states = np.array([n[0] for n in rez["Name"]])
    for s in ["Q","N","V","S","T"]:
        temp_df = xr.where(raster.isin(np.where(rez_states==s)),cf,np.nan).mean(("lat","lon")).drop_vars(["height","crs"]).to_dataframe(name=s)
        df = pd.concat([df, temp_df], axis=1)
        # Save the results to a CSV file for this time period and state
    df.to_csv(f"/g/data/ng72/ab4502/capacity_factor/hourly_wind_ts_barra_c_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}.csv", index=True)

    client.close()