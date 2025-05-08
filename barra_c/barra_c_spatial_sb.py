from sea_breeze import load_model_data, sea_breeze_funcs, sea_breeze_filters
from dask.distributed import Client
from dask.distributed import progress
import pandas as pd
import os
import metpy.calc as mpcalc
import argparse

if __name__ == "__main__":

    #Set up argument parser
    parser = argparse.ArgumentParser(
        prog="BARRA-C spatial sea breeze functions",
        description="This program applies spatial sea breeze functions to a chosen period of BARRA-C data, such as frontogenesis"
    )
    parser.add_argument("t1",type=str,help="Start time (Y-m-d H:M)")
    parser.add_argument("t2",type=str,help="End time (Y-m-d H:M)")
    parser.add_argument("--model",default="barra_c",type=str,help="Model name to save the output under")
    parser.add_argument("--lat1",default=-45.7,type=float,help="Start latitude")
    parser.add_argument("--lat2",default=-6.9,type=float,help="End latitude")
    parser.add_argument("--lon1",default=108,type=float,help="Start longitude")
    parser.add_argument("--lon2",default=158.5,type=float,help="End longitude")
    parser.add_argument('--smooth',default=False,action=argparse.BooleanOptionalAction,help="Smooth the data before calculating frontogenesis")
    parser.add_argument("--sigma",default=2,type=int,help="Sigma for smoothing")
    args = parser.parse_args()

    #Initiate distributed dask client on the Gadi HPC
    client = Client()

    #Set the domain bounds
    lat_slice=slice(args.lat1,args.lat2)
    lon_slice=slice(args.lon1,args.lon2)    

    #Set time slice and model name    
    t1 = args.t1
    t2 = args.t2

    #Set up smoothing axes
    if args.smooth:
        smooth_axes = ["lat","lon"]   
    else:
        smooth_axes = None          

    #Load BARRA-C surface variables
    chunks = {"time":1,"lat":-1,"lon":-1}
    orog, lsm = load_model_data.load_barra_static(
        "AUST-04",
        lon_slice,
        lat_slice)
    vas = load_model_data.load_barra_variable(
            "vas",
            t1,
            t2,
            "AUST-04",
            "1hr",
            lat_slice,
            lon_slice,
            chunks=chunks,
            smooth_axes=smooth_axes,
            sigma=args.sigma,
            smooth=args.smooth)
    uas = load_model_data.load_barra_variable(
            "uas",
            t1,
            t2,
            "AUST-04",
            "1hr",
            lat_slice,
            lon_slice,
            chunks=chunks,
            smooth_axes=smooth_axes,
            sigma=args.sigma,
            smooth=args.smooth)
    huss = load_model_data.load_barra_variable(
            "huss",
            t1,
            t2,
            "AUST-04",
            "1hr",
            lat_slice,
            lon_slice,
            chunks=chunks,
            smooth_axes=smooth_axes,
            sigma=args.sigma,
            smooth=args.smooth)
    tas = load_model_data.load_barra_variable(
            "tas",
            t1,
            t2,
            "AUST-04",
            "1hr",
            lat_slice,
            lon_slice,
            chunks=chunks,
            smooth_axes=smooth_axes,
            sigma=args.sigma,
            smooth=args.smooth)     
    angle_ds = load_model_data.get_coastline_angle_kernel(
        lsm,
        compute=False,
        lat_slice=lat_slice,
        lon_slice=lon_slice,
        path_to_load="/g/data/ng72/ab4502/coastline_data/barra_c.nc",
        smooth=args.smooth,
        sigma=args.sigma)

    #Calc 2d kinematic moisture frontogenesis
    F = sea_breeze_funcs.kinematic_frontogenesis(
        huss,
        uas,
        vas
    )

    #Calc coast-relative 2d kinematic moisture frontogenesis
    # Fc = sea_breeze_funcs.coast_relative_frontogenesis(
    #     huss,
    #     uas,
    #     vas,
    #     angle_ds["angle_interp"]
    # )

    #Setup out paths
    out_path = "/g/data/ng72/ab4502/sea_breeze_detection/"+args.model+"/"
    F_fname = "F_"+pd.to_datetime(t1).strftime("%Y%m%d%H%M")+"_"+\
                    (pd.to_datetime(t2).strftime("%Y%m%d%H%M"))+".zarr"   
    Fc_fname = "Fc_"+pd.to_datetime(t1).strftime("%Y%m%d%H%M")+"_"+\
                        (pd.to_datetime(t2).strftime("%Y%m%d%H%M"))+".zarr"       
    if os.path.isdir(out_path):
        pass
    else:
        os.mkdir(out_path)   

    #Add smoothing attrs
    F = F.assign_attrs(
        smooth=args.smooth,
        sigma=args.sigma,
    )
    # Fc = Fc.assign_attrs(
    #     smooth=args.smooth,
    #     sigma=args.sigma,
    # )

    #Save the output
    print("INFO: Computing frontogenesis...")
    F_save = F.to_zarr(out_path+F_fname,compute=False,mode="w")
    progress(F_save.persist())
    # print("INFO: Computing coast-relative frontogenesis...")
    # Fc_save = Fc.to_zarr(out_path+Fc_fname,compute=False,mode="w")
    # progress(Fc_save.persist())
