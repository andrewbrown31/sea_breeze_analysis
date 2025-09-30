import xarray as xr
from dask.distributed import Client
import numpy as np

if __name__ == "__main__":

    client = Client()

    model = "barra_c_smooth_s2"
    name = "standard"
    variable = "F"
    base_path = "/g/data/ng72/ab4502/sea_breeze_detection/"

    #Load in the daily sea breeze occurrence datasets
    sb_obj_aest = xr.open_dataset(f"{base_path}/{model}/filters/daily_filtered_{name}_{variable}_aest_19790101_20250101.zarr",chunks={})
    sb_obj_awst = xr.open_dataset(f"{base_path}/{model}/filters/daily_filtered_{name}_{variable}_awst_19790101_20241231.zarr",chunks={})

    #Load in the dataset of coastal shapes to define daily sea breeze time series
    shapes = xr.open_dataset("/g/data/ng72/ab4502/coastline_data/rez_coastal_shapes.nc").sel(lat=sb_obj_aest.lat,lon=sb_obj_aest.lon)
    regions = ["gipps","illawara","newcastle","southern","tas","sa","bunbury"]
    for r in regions:
        print(f"Processing {r}")
        if r == "bunbury":
            sb_ts = xr.where(shapes[r],sb_obj_awst,np.nan).max(("lat","lon"))
        else:
            sb_ts = xr.where(shapes[r],sb_obj_aest,np.nan).max(("lat","lon"))

        sb_ts.mask.to_dataframe(name="sb").drop(columns=["height","crs","region","abbrevs","names"]).to_csv(f"/g/data/ng72/ab4502/sea_breeze_detection/{model}/sb_days/{r}.csv")