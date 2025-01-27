terraform init, terraform apply -auto-approve, terraform destroy


Step 1: 
Create IAM service account - Add cloud storage, BigQuery and Compute Engine

Step 2:
Create key.

Step 3:
Store key locally in .json file

Step 4:
Install terraform in VSCode

Step 5:
Create main.tf

Step 6:
Create Variables.tf and use these variables in main.tf

Step 7:
use terraform fmt to format the files

Step 8:
run terraform init to initialize and configure the backend and check out the configuration.

Step 9:
terraform plan to check what this will do when applied.

Step 10:
terraform apply to execute the plan and apply the changes to cloud 

Note:
to see available option with terraform apply, ```terraform apply --help````
There is no option with -y. -auto-approve is there which skips the interactive approval of plan before applying. 

Step 11:
terraform destroy to bring down the resources from the cloud.

Step 12:
Make sure to add *.json in .gitignore