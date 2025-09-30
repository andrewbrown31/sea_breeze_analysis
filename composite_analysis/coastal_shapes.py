import xarray as xr
import numpy as np
import rioxarray
import geopandas as gpd 
import fiona 
import pandas as pd 
from rasterio import features
from affine import Affine

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

def define_coastal_region(sb_lat_slice,sb_lon_slice,angle_ds,lsm):

    """
    Given lat/lon bounds, a dataset containing min_coast_dist, and a land-sea mask, define two coastal regions:
    1) land within 50km of the coast
    2) land and sea within 50km of the coast
    """

    land = xr.where(
        (angle_ds.lat<sb_lat_slice.stop) &
            (angle_ds.lat>sb_lat_slice.start) & 
            (angle_ds.lon>sb_lon_slice.start) & 
            (angle_ds.lon<sb_lon_slice.stop) & 
            (angle_ds.min_coast_dist <= 50) &
            (lsm), 1, 0 )

    land_sea = xr.where(
        (angle_ds.lat<sb_lat_slice.stop) &
            (angle_ds.lat>sb_lat_slice.start) & 
            (angle_ds.lon>sb_lon_slice.start) & 
            (angle_ds.lon<sb_lon_slice.stop) & 
            (angle_ds.min_coast_dist <= 50), 1, 0 )
    return land,land_sea

if __name__ == "__main__":

    #Load in Rachaels rez mask
    rez_mask = xr.open_dataset("/g/data/ng72/ri9247/data/rez_mask_AUST-04.nc")

    #Load BARRA-C static info
    angle_ds = xr.open_dataset(
        "/g/data/ng72/ab4502/coastline_data/barra_c.nc"
    )
    lsm = xr.open_dataset(
        "/g/data/ob53/BARRA2/output/reanalysis/AUST-04/BOM/ERA5/historical/hres/BARRA-C2/v1/fx/sftlf/latest/sftlf_AUST-04_ERA5_historical_hres_BOM_BARRA-C2_v1.nc"
    ).sel(lat=angle_ds.lat,lon=angle_ds.lon)

    #Define a series of coastal regions adjacent to offshore wind REZs
    #Gippsland
    gipps_lon_slice = slice(146.6,149)
    gipps_lat_slice = slice(-39.2,-36.5)
    gipps_shape,gipps_shape_landsea = define_coastal_region(gipps_lat_slice,gipps_lon_slice,angle_ds,lsm.sftlf>=0.5)

    #Illawara
    illawara_lat_slice = slice(-35,-33.8)
    illawara_lon_slice = slice(149,154)
    illawara_shape,illawara_shape_landsea = define_coastal_region(illawara_lat_slice,illawara_lon_slice,angle_ds,lsm.sftlf>=0.5)

    #Newcastle
    newcastle_lat_slice = slice(-33.5,-32)
    newcastle_lon_slice = slice(149,154)
    newcastle_shape,newcastle_shape_landsea = define_coastal_region(newcastle_lat_slice,newcastle_lon_slice,angle_ds,lsm.sftlf>=0.5)

    #Southern Ocean
    southern_lat_slice = slice(-39.8,-37)
    southern_lon_slice = slice(141,143.5)
    southern_shape,southern_shape_landsea = define_coastal_region(southern_lat_slice,southern_lon_slice,angle_ds,lsm.sftlf>=0.5)

    #Tasmania
    tas_lat_slice = slice(-41.5,-40.5)
    tas_lon_slice = slice(145.8,147.8)
    tas_shape,tas_shape_landsea = define_coastal_region(tas_lat_slice,tas_lon_slice,angle_ds,lsm.sftlf>=0.5)

    #South Australia
    sa_lat_slice = slice(-38.2,-36)
    sa_lon_slice = slice(138.8,140.5)
    sa_shape,sa_shape_landsea = define_coastal_region(sa_lat_slice,sa_lon_slice,angle_ds,lsm.sftlf>=0.5)

    #Bunbury
    bunbury_lat_slice = slice(-33.8,-31.9)
    bunbury_lon_slice = slice(115.2,116.3)
    bunbury_shape,bunbury_shape_landsea = define_coastal_region(bunbury_lat_slice,bunbury_lon_slice,angle_ds,lsm.sftlf>=0.5)    

    shapes=xr.Dataset({
        "gipps":gipps_shape,
        "gipps_landsea":gipps_shape_landsea,
        "illawara":illawara_shape,
        "illawara_landsea":illawara_shape_landsea,
        "newcastle":newcastle_shape,
        "newcastle_landsea":newcastle_shape_landsea,
        "southern":southern_shape,
        "southern_landsea":southern_shape_landsea,
        "tas":tas_shape,
        "tas_landsea":tas_shape_landsea,
        "sa":sa_shape,
        "sa_landsea":sa_shape_landsea,
        "bunbury":bunbury_shape,
        "bunbury_landsea":bunbury_shape_landsea,
        "lsm":lsm.sftlf>=0.5,
        "rez_mask":rez_mask.rez_mask
    },coords={"lat":angle_ds.lat,"lon":angle_ds.lon})


    #Add Bunbury REZ to the rez mask 
    bunbury=gpd.read_file("/g/data/ng72/ab4502/coastline_data/OffshoreRenewable_Energy_Infrastructure_Regions_-7052837244166384748.kmz").iloc[[10,11]]
    bunbury_shapes = [(shape, n) for n, shape in enumerate(bunbury.geometry)]
    raster = rasterize(bunbury_shapes,{"lat":shapes.lat,"lon":shapes.lon},fill=np.nan)
    shapes["rez_mask"] = xr.where(raster>=0,shapes.rez_mask.max()+1,shapes["rez_mask"])

    shapes.to_netcdf("/g/data/ng72/ab4502/coastline_data/rez_coastal_shapes.nc")