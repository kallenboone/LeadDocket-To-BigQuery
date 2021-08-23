# Introduction
This script collects data collection from LeadDocket and pushes it to BigQuery
in Google Cloud Platform. The script is designed to be run as a Google Cloud Function.

This program is useful for collecting API data in real-time for dashboards and other
custom reports based on LeadDocket data.

# Design Decisions and Limitations
  - This program only collects API fields that are available via both API and LeadDocket UI reports
so that historical data can be loaded into the same BigQuery database as real-time data
  - The LeadDocket rate limits make it difficult to collect large amounts of data at a time,
and larger lookback periods (multiple years) may cause failures or timeouts in the script

# Deployment
This script can be deployed via the UI or with the gcloud toolkit. This README only describes
how to deploy to the script via the UI.

## Deploying via the UI

This deployment for this script has four components:
1. A pub/sub topic (used to trigger this script)
2. The API key for a LeadDocket user stored in Google Secret Manager
3. The Google function (this script)
4. A cloud scheduler job (used to send a message to the pub/sub topic)

The first step to deploying this tool is storing the API key for a specific LeadDocket user
in Google secret manager. To do this, visit the Secret Manager service and Create a new secret.
Name the secret, and paste the API key into the "Secret value" field. The rest of the
configuration option values can be left default. Press "Create Secret" to finish this step

Next you will need to  visit the Pub/Sub page for your GCP project. You can find this
by searching for "pub/sub" in the search box at the top of the page. On the Pub/Sub page,
create a new topic. For the options you will need to:
  - Choose a name (I use the name "lead_docket")
  - Uncheck the default subscription option
  - Click "Create Topic"

Now that we have a topic created as a trigger for the function, we are ready to deploy the
script. Visit the Cloud Functions page for your GCP project, and click "Create Function"
near the top of the page. For this first page you will need to:
  - Name your function
  - Choose a region that is reasonably close to you
  - Change the trigger type to "Cloud Pub/Sub"

For Runtime, Build, and Connection Settings, the default options for Memory Allocated and
Timeout should be sufficient. The service account used to run this will need the following
permissions:
  - bigquery.datasets.get
  - bigquery.tables.create
  - bigquery.tables.get
  - bigquery.tables.getData
  - bigquery.tables.update
  - bigquery.tables.updateData
  - bigquery.jobs.create
  - secretmanager.versions.access

The configuration of this service account is outside the scope of this documentation. For
more information see https://cloud.google.com/iam/docs/creating-managing-service-accounts.

For RunTime Environment Variables, the following variables need to be set:
  - PROJECT_ID: The numeric ID for your GCP project
  - SECRET_ID: The name of the LeadDocket API key stored in Google Secret Manager (created in step 1)
  - DATASET_ID: The name of the dataset to store the LeadDocket data
  - PROD_TABLE_ID: The name of production table used to store LeadDocket data
  - STAGING_TABLE_ID: The name of the staging table used to upsert LeadDocket data
  - LEAD_DOCKET_BASE_URL: The base url for LeadDocket API for a specific client (client.leaddocket.com/api)

Click "Save" to move on to the "Code" section of the deployment.

Add the code into the editor using either a zip of the source files or by copying and
pasting the source via the inline editor. Change the Runtime to be Python 3.8 and the
Entry Point to be "google_cloud_main".

Click "Deploy" to finish this step.

### Testing the deployment
At this point, you should have a fully functioning data collector that collects data
from LeadDocket and pushes it to BigQuery. Before automating the data collection process,
it is highly recommended that you test the deployment. To do so, visit the Pub/Sub topic
that was created as a trigger and create a new message with the content of "1" without
the quotation marks.

If everything is working as expected, you should be able to go back to your cloud function
and see script scanning LeadDocket for changes in the last minute. If you don't see anything
in the logs, or you are seeing error messages, review the previous steps for guidance.

### Automating the data collection
As the last step for deploying this script, the data collection process should be automated
with the Cloud Scheduler. Visit the Cloud Scheduler page and create a new job with the
"Create Job" button near the top of the page. Name the job, and choose a frequency according
to the crontab format: https://crontab.tech/examples

For the job's target, choose "Pub/Sub" and then select the topic created in the first step
of the deployment section. For the message body, choose far back you want the script to scan
for changes (in minutes). For example, a message body of 10 would scan 10 minutes into the
past for changes each time the cloud function is triggered.

To ensure that no data is missed, it is recommended that the frequency is slightly lower
than the lookback period configured in the message body. For example, triggering the function
every 10 minutes to look for changes in the last 12 minutes will ensure that there is no
gaps in data collection due to script execution time.

The advanced settings can be left as their defaults.

Click "Create" to schedule the job and go back to the cloud function deployment to ensure
that the function is being on the schedule that you are expecting. If so, congratulations,
have fully deployed the LeadDocket Data Connector!

