# nwm_bq_api_v2
This is the version 2.0.0 of the CIROH National Water Model (BigQuery) API with new endpoints, derived datasets, and enhanced functionalities.


### Google Cloud Deployment
1. Open Google Cloud Shell Editor window and drag and drop the code repository folder into the Explorer pane. Inside the deployment_terraform/terraform.tfvars file, replace `<GCP_PROJECT_ID>` and `<REGION>` with your Google Cloud project identifier and region, and place suitable names for `<SERVICE_ACCOUNT_USERNAME>` and `<REPOSITORY_ID>` as they will be created along the way.

2. Authenticate your user account for the Google Cloud CLI
```
gcloud auth login
```

3. Change the directory to 'deployment_terraform'
```
cd deployment_terraform
```

4. Set up the terraform infrastructure through the commands:
```
terraform init && \
terraform plan && \
terraform apply -auto-approve
```
