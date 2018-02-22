# These scripts help in scraping sticker data from FIJI database and process them 

The scripts uses jquery to parse the data,hence install 'jq' before executing the scripts. To install jq,execute the following command:
```sudo apt-get install jq```


To extract the data run the shell scripts first and process them with the pyspark script .

To run the shell scripts,execute the below command:
```./free.sh ```
```./discontinued.sh```
```./paid.sh```
```./subscriptions.sh```

Copy all the data to a gcs bucket using the command 
```gsutil cp -r all_stickers/* [GCS_BUCKET_NAME]```

Example : 
```gsutil cp -r all_stickers/* gs://ds-url-catag/stick_bytype/all_stickers_feb20/```


To run the pyspark program,run the following command
``` spark-submit  scrape_stick_types.py [GCS_BUCKET_NAME_READ] [GCS_BUCKET_NAME_WRITE] ```

[GCS_BUCKET_NAME_READ] - is the bucket name mentioned in the previous step where the scraped data is present
[GCS_BUCKET_NAME_WRITE] - is the bucket name where the processed data should be write

Example:
``` spark-submit  scrape_stick_types.py "gs://ds-url-catag/stick_bytype/all_stickers_feb20/" "gs://ds-url-catag/stick_bytype/agg_proc_stickertypes/"```



