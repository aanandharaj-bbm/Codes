
#Validating Email addresses

The script is used to validate email address according to IETF RFC 5322 standards

To use the code

Git clone the above folder and run the following spark command

spark-submit extract_valid_emails.py [input_file_location]

ex : spark-submit extract_valid_emails.py "gs://ds-url-catag/emails/emails.txt"

The input_file_location should of txt format.
By default , the outputs are saved in a csv format in the following location


Valid Emails:
gs://ds-url-catag/emails_formats//valid_emails/

Invalid Emails:
gs://ds-url-catag/emails_formats//invalid_emails/


