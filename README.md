# nwm_bq_api_v2
This is the version 2.0.0 of the CIROH National Water Model (BigQuery) API with new endpoints, derived datasets, and enhanced functionalities.


## Google Cloud Deployment
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

5. Change the directory to src: `cd src`. Inside  `config_cloudbuild.yaml` file, replace the placeholders `<GCP_PROJECT_ID>`, `<REGION>`, and `<REPOSITORY_ID>` as you defined in the `terraform.tfvars`, and assign new names for the `<IMAGE_NAME>` and `<CLOUD_RUN_SERVICE_NAME>`. Run the following command to build the container image, push it to the Artifact Registry, and deploy the Cloud Run app.
```
gcloud builds submit --config config_cloudbuild.yaml
```
Wait till the success message and note down the service URL.

6. Change directory to openapi_schema folder
```
cd ../openapi_schema
```

7. Create the API through API Gateway against the Cloud Run `<CLOUD_RUN_SERVICE_NAME>` as specified in the `config_cloudbuild.yaml` file.
```
gcloud api-gateway apis create <CLOUD_RUN_SERVICE_NAME> --project=<PROJECT_ID>
```

8. Enter the service URL as the `<API-BACKEND-URL>` through 'x-google-api-management' in the 'openapi303.json' file. Create the API config from the OpenAPI configuration provding a suitable `<CONFIG_ID>`, `<API_ID>`, `<GCP_PROJECT_ID>` and `<SERVICE_ACCOUNT_EMAIL>`.
```
gcloud api-gateway api-configs create <CONFIG_ID> \
--api=<API_ID> --openapi-spec=openapi_config_303.json  \
--project=<GCP_PROJECT_ID> \
--backend-auth-service-account=<SERVICE_ACCOUNT_EMAIL>
```

9. Create and Deploy the API Gateway (`GATEWAY_ID`) with the created API (`<API_ID>`) and it config at the location `<REGION>` within the project `<GCP_PROJECT_ID>`
```
gcloud api-gateway gateways create <GATEWAY_ID> \
  --api=<API_ID> --api-config=<CONFIG_ID> \
  --location=<REGION> --project=<GCP_PROJECT_ID>
```

10. Obtain the gateway description running the following code:
```
gcloud api-gateway gateways describe <GATEWAY_ID> --location=<REGION> --project=<GCP_PROJECT_ID>
```
Particularly, note the URL against defaultHostname key. This will be the root URL of the API.

11. Read the API details particularly to note the managedService in the format `<MANAGED_SERVICE_NAME>.apigateway.<GCP_PROJECT_ID>.cloud.goog`.

```
gcloud api-gateway apis describe <API_ID>
```
12. Enable the service associated with the new API config
```
gcloud services enable <MANAGED_SERVICE_URL> --project <GCP_PROJECT_ID>

```
13. Use the defaultHostname as your API root. You need to avail a standard key from your gcloud project and use it for authenticated endpoints.
