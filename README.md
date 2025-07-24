# Sea breeze analysis

A collection of scripts and notebooks for calculating and analysing sea breeze objects from model data. Sea breeze objects are defined using code from the [sea_breeze](https://github.com/andrewbrown31/sea_breeze) repository.

## Model preprocessing

There is pre-processing of model data needed for the application of some sea breeze diagnositcs. Namely, calculating the angle of the coastline, see [`get_coastline_angles.py`](get_coastline_angles.py).

## Calculate diagnostics

Sea breeze diagnostics are caluclated using scripts in each of the model directories (`/era5 /barra_r /barra_c /aus2200/`). These include python scripts (e.g. `/aus2200/aus2200_sbi.py`) as well as PBS job scripts (e.g. `/aus2200/diagnostic_jobs/aus2200_sbi.sh`). The main diagnostic code is in the [sea_breeze](https://github.com/andrewbrown31/sea_breeze) repository.

## Identify sea breeze objects (filtering)

First, the thresholds for identifying sea breeze objects from the diagnostics are calculated using the [`compute_percentiles.py`](compute_percentiles.py) script. These thresholds are manually added to the filtering scripts, found in each of the model directories (`/era5 /barra_r /barra_c /aus2200/`). These are in the form of PBS job scripts (e.g. `/aus2200/filter_jobs/filter_smooth_s4.sh`), that run python functions in the [sea_breeze](https://github.com/andrewbrown31/sea_breeze) repository.

## Analysis

* [Diurnal compositing of sea breeze objects and wind](diurnal_groupby.py)
* [Analysis notebooks](/analysis_notebooks/)
