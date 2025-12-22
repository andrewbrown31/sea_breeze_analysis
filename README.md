# Sea breeze analysis

A collection of scripts and notebooks for calculating and analysing sea breeze objects from model data. Sea breeze objects are defined using code from the [sea_breeze](https://github.com/andrewbrown31/sea_breeze) repository. This repository includes notebooks for [generating figures for Brown et al. (submitted).](#figures). 

## Model preprocessing

There is pre-processing of model data needed for the application of some sea breeze diagnositcs. Namely, calculating the angle of the coastline, see [`get_coastline_angles.py`](get_coastline_angles.py).

## Calculate diagnostics

Sea breeze diagnostics are caluclated using scripts in each of the model directories (`/era5 /barra_r /barra_c /aus2200/`). These include python scripts (e.g. `/aus2200/aus2200_sbi.py`) as well as PBS job scripts (e.g. `/aus2200/diagnostic_jobs/aus2200_sbi.sh`). The main diagnostic code is in the [sea_breeze](https://github.com/andrewbrown31/sea_breeze) repository.

## Identify sea breeze objects (filtering)

First, the thresholds for identifying sea breeze objects from the diagnostics are calculated using the [`compute_percentiles.py`](compute_percentiles.py) script. These thresholds are manually added to the filtering scripts, found in each of the model directories (`/era5 /barra_r /barra_c /aus2200/`). These are in the form of PBS job scripts (e.g. `/aus2200/filter_jobs/filter_smooth_s4.sh`), that run python functions in the [sea_breeze](https://github.com/andrewbrown31/sea_breeze) repository.

## Analysis

For Brown et al. (submitted): Identifying sea breezes from atmospheric model output (sea_breeze v1.1)

* [Diurnal compositing of sea breeze objects and wind](diurnal_groupby.py)
* [Analysis notebooks](/analysis_notebooks/)

For Brown and Vincent (submitted): The impact of sea breezes on offshore wind energy resources in Australia

* [Calculate sea breeze objects over 1979-2024 from BARRA-C2](/barra_c/filter_jobs/filter_F_driver.sh)
* [Define the shapes of the coastal zones for defining sea breeze days for each offshore wind area](/composite_analysis/coastal_shapes.py)
* [Resample to daily max (in local time) and define SB days for each REZ](composite_analysis/daily_resample.sh)
* Composite variables for SB days and non SB days in a set of regions, on a monthly basis, and average to seasonal (`/composite_analysis/sea_breeze_composite_*.sh`)

## Figures 

For Brown et al. (submitted): Identifying sea breezes from atmospheric model output (sea_breeze v1.1)

* [Figure 2](/analysis_notebooks/coastline_notepad.ipynb)
* [Figure 3-8](/analysis_notebooks/case_studies.ipynb)
* [Figure 9-11](/analysis_notebooks/spatial_counts.ipynb)
* [Figure 12](/analysis_notebooks/surface_heating.ipynb)
* [Figure 13](/analysis_notebooks/diurnal_hovmoller_plots.ipynb)
* [Figure S1](/analysis_notebooks/coastline_notepad.ipynb)
* [Figure S2 and S3](/analysis_notebooks/obs_comparison.ipynb)
* [Figure S4](/analysis_notebooks/diurnal_hovmoller_plots.ipynb)

For Brown and Vincent (submitted): The impact of sea breezes on offshore wind energy resources in Australia
* [Figure 1-3](/analysis_notebooks/climo.ipynb)
* [Figure 4](/analysis_notebooks/sb_days_stats_rez.ipynb)
* [Figure 5-8](/analysis_notebooks/composite_rez.ipynb)
* [Figure 9-10](/analysis_notebooks/sb_days_stats_rez.ipynb)
* [Figure S1](/analysis_notebooks/previous_study_compare.ipynb)
* [Figure S2](/analysis_notebooks/composite_rez.ipynb)
* [Figure S3](/analysis_notebooks/sb_days_stats_rez.ipynb)

## References

Brown, A., Vincent, C., and Short, E.: Identifying sea breezes from atmospheric model output (sea_breeze v1.1), EGUsphere [preprint], https://doi.org/10.5194/egusphere-2025-4848, 2025.

Brown, A. and Vincent, C.: The impact of sea breezes on offshore wind energy resources in Australia (submitted).