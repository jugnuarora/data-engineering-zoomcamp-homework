1) Practiced loading meteostat data to postgres. The respective code can be found in meteostat.ipynb.

2) For Homework,
    a) Create a folder .dlt and file in it named secrets.toml. Make sure to add .dlt folder or .toml files in .gitignore to avoid pushing to github.
    b)  Add following code from the json file downloaded as key from bigquery:

````

[destination.bigquery]
location = "EU"

[destination.bigquery.credentials]
project_id = "FROM JSON FILE"
private_key = "FROM JSON FILE"
client_email = "FROM JSON FILE"
````

For rest of the code refer data-ingestion-with-dlt.ipynb