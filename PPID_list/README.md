# Generating PPID list

The script crawls all of DFP data and generates PPID list for the campaigns names mentioned

To execute the script,do the following

spark-submit Extract_PPID_dfp.py campaingn_names'_list location_for_storage

Example:

spark-submit Extract_PPID_dfp.py ['Zilingo_20171101_2017-API0346','Zilingo_20171104_2017-API0346'] "gs://ds-url-catag/orders/Zilingo/test/"


