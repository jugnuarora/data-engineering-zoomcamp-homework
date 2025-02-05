Step 1: Create docker-compose file to run kestra locally on port 8080. 

Step 2: Create project in google cloud. IAM - create service account.   Select cloud storage and set as admin. Select bigquery and set as Admin. Generate Key and use this JSON in Kestra kv pair.

Step 3: Set-up key value pair in Kestra (in dataset when I gave zoomcamp-homework, it complained to have underscore and not '-')

Step 4: Create GCP storage bucket and bigquery bucket

Step 5: Create gcp_taxi for selective extract and load
                gcp_taxi_schedule (with triggers) for backfill

Step 6: Question 1:
    Execute gcp_taxi and select yellow, 2020, 12 
    Go to outputs tab in execution in kestra and check the extract -> outputFiles -> yellow_tripdata_2020-12.csv. On the right side pane, it shows size as 128.3 MIB
    OR
    Go to Google Cloud Storage Bucket, and Click on the Bucket and see the size for yellow_tripdata_2020-12.csv. It is 128.3 MB

    Answer: `128.3`

Step 7: Question 2:
    file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"
    taxi: green
    year: 2020
    month: 04

    rendering the values in file:
    `file: "green_tripdata_2020-04.csv"`

Step 8: Question 3:
    Execute gcp_taxi_scheduled (by clicking on Triggers tab and then on Backfill execution in purple) to run with trigger for yellow, from 1 Jan 2020 - 31 Dec 2020 and label as backfill true to identify that it is backfilling.
    Go to Google Cloud, bigquery bucket, select the database and select yellow_tripdata (merged file for 2020) and go to Details. Look for Number of Rows:  It is `24,648,499`

Step 9: 
    Execute gcp_taxi_scheduled (by clicking on Triggers tab and then on Backfill execution in purple) to run with trigger for green, from 1 Jan 2020 - 31 Dec 2020 and label as backfill true to identify that it is backfilling.
    Go to Google Cloud, bigquery bucket, select the database and select green_tripdata (merged file for 2020) and go to Details. Look for Number of Rows:  It is `1,734,051`

Step 10:
    Execute gcp_taxi and select yellow, 2021, 03 (March) (Make sure the script has input values as 2021 for year along with 2019 and 2020) - 
    1,925,152
    OR
    Execute gcp_taxi_scheduled (by clicking on Triggers tab and then on Backfill execution in purple) to run with trigger for green, from 1 Mar 2021 - 31 Mar 2021 and label as backfill true to identify that it is backfilling.
    `1,925,152`

Step 11:
    In 04_gcp_taxi_scheduled, in triggers add timezone:. Hover over timezone and it shows the website to refer for the values. https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List)\
    Look out for New York which is 	`America/New_York`
    Add this value to timezone. 
    Verify it by going to triggers and hovering over the calendar sign and it show the timezone value.

Additional Step:
    To save on space, drop the *_ext files.
