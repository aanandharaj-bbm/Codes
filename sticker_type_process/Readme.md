#These scripts help in scraping sticker data from FIJI database and process them 

The scripts uses jquery to parse the data,hence install 'jq' before executing the scripts. To install jq,execute the following command:
sudo apt-get install jq


To extract the data run the shell scripts first and process them with the pyspark script .

To run the shell scripts,execute the below command:
./free.sh 
./discontinued.sh
./paid.sh
./subscriptions.sh

To run the pyspark program,run the following command


