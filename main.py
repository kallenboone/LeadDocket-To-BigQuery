import base64
import datetime
import io
import os
import requests
import json
import ndjson
import time
from urllib.parse import urljoin
from google.cloud import bigquery, secretmanager
from google.api_core.exceptions import ClientError
from google.cloud.exceptions import NotFound

# The total amount of leads found in the time period
LEADCOUNT = 0
# A global count of API requests used to track API rate limits
TOTALREQUESTS = 0
# The base URL for all requests
LEAD_DOCKET_BASE_URL = os.environ.get('LEAD_DOCKET_BASE_URL')
if not (LEAD_DOCKET_BASE_URL):
    exit("Required environment variables not set. Please set the LEAD_DOCKET_BASE_URL variable.")

def convert_to_newline_delimeted_json(jsonData):
    """ Converts JSOn to Newline Delimited JSON

    This helper function converts JSON into newline delimeted JSON. This is useful
    because BigQuery can run into issues when using normal JSON:
    https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json

    :param jsonData (dict) the jsonData to convert
    :return (json) the converted json
    """
    return ndjson.dumps(jsonData)

def save_data_to_json(fileName, jsonData):
    """ Saves the output of a run to a JSON file

    This helper function allows a developer to output the data to a JSON format instead of
    BigQuery. Primarily used for testing purposes

    :param fileName: (str) name of the file to use for output
    :param jsonData (dict) the jsonData to print to a JSON file
    :return none
    """
    f = open(fileName, "w")
    f.write(json.dumps(jsonData))

def handle_api_errors(response, url, headers):
    """ Handles LeadDocket API Errors

    This simple function handles common errors that happen with requests to LeadDocket.
    Retries requests after a few minutes if the leaddocket API starts failing.

    :param response: (dict) response from a get request to LeadDocket
    :param url (dict) the url of the above request
    :param headers (dict) the headers to use if the request needs to be retried
    :return response (dict) the retried response
    """
    global TOTALREQUESTS
    if (response.status_code != 200):
        print(f"API requests have started to fail with status code: {response.status_code}")
        if (response.status_code == 429):
            print(f"Headers for this response are: {response.headers}")
            print(f"Rate limits hit at {datetime.datetime.now()} and after {TOTALREQUESTS} requests, trying again "
                         f"in 2 minutes")
            time.sleep(120)
        else:
            print(f"Unexpected error, trying again in 1 minute")
            time.sleep(60)

        response = requests.get(url, headers=headers)
        TOTALREQUESTS +=1

    return response

def convert_datetime_to_date(datetime_string):
    """ Converts a DateTime object to a Date object

    A helper function to convert datetimes into dates. This is necessary because the
    LeadDocket API returns a datetime for client_birthday as a datetime, but exports
    the same field in UI reports as a date.

    :param datetime_string: (string) a datetime in the form of a string
    :return (string) a string formatted as a date
    """
    format = "%Y-%m-%dT%H:%M:%S"
    dt_object = datetime.datetime.strptime(datetime_string, format)
    return f"{dt_object.year}-{dt_object.month}-{dt_object.day}"

def get_lead_changes_since(minutes, headers):
    """ Gets LeadDocket leads that have experienced a status changes since a specified time

    This function takes a number of minutes and a set of headers and returns
    a list of leads that changed since the number of minutes into the past

    :param minutes: (int) the number of minutes in the past to look for changes
    :param headers (dict) the headers for an API request, including the API key
    :return records_to_update (list) a list of leads that have been recently updated
    """

    # Find the time in the past that corresponds to the number of minutes
    # sent as an input parameter and format it for an API request
    print(f"Scanning for changes in the last {minutes} minutes")

    utcnow = datetime.datetime.utcnow()

    minutesAgo = utcnow - datetime.timedelta(minutes=minutes)
    time_to_query = minutesAgo.strftime("%Y-%m-%dT%H:%M")

    # Look for changes since time_to_query
    last_status_changes_since_endpoint = f"leads/laststatuschangesince?date={time_to_query}"
    url = urljoin(LEAD_DOCKET_BASE_URL, last_status_changes_since_endpoint)
    response = requests.get(url, headers=headers)

    global TOTALREQUESTS
    TOTALREQUESTS += 1

    print(f"Initial LeadDocket API request returned with status code: {response.status_code}")
    changed_records_json = response.json()

    # Output and store information about the total number of leads found
    global LEADCOUNT
    LEADCOUNT = changed_records_json['TotalRecordCount']
    print(f"{LEADCOUNT} changes found since {time_to_query}")

    # Extract the records from the API response
    records_to_update = []

    def _extractRecords(json_to_extract):
        for record in json_to_extract['Records']:
            records_to_update.append(record)

    _extractRecords(changed_records_json)

    if changed_records_json['TotalPages'] > 1:
        print("Additional pages of leads found, grabbing more data...")

        # Start the loop on page two since we already gathered page one with our initial request
        page_iter = 2

        # Send a new API request for each new page of data
        while page_iter <= changed_records_json['TotalPages']:
            page_endpoint = f"{last_status_changes_since_endpoint}&page={page_iter}"
            page_url = urljoin(LEAD_DOCKET_BASE_URL, page_endpoint)
            print(f"Getting records on page: {page_iter}")

            # Pass the response to an error handler to deal with rate limits and unexpected errors.
            # The error handler will retry a single failed request
            response = handle_api_errors(requests.get(page_url, headers=headers), url, headers)
            TOTALREQUESTS += 1

            if(response.status_code != 200):
                # If the second API request fails, the rate limit may have a backoff period, or our key may be blocked,
                # and we should give up
                print(f"API requests have continued to fail on page {page_iter}")
                exit("API requests failing after waiting. Exiting Early.")

            leads_json = response.json()

            # Extract the records from the API data
            _extractRecords(leads_json)

            page_iter += 1

    return records_to_update


def upload_to_bigquery(leads, table_id, write_mode):
    """ Upload to Big Query

    This function takes an array of NDJSON formatted leads and uploads the set of leads to a
    BigQuery table.

    :param leads: (array) array of leads in JSON format to be uploaded to BigQuery
    :param table_id (string) id of the table to insert records into
    :param write_mode (bigquery.WriteDisposition) WriteDisposition that tells the function
    how to handle inserting records when a table already exists
    :return none
    """
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField("id", "INTEGER"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("substatus", "STRING"),
                bigquery.SchemaField("severitylevel", "INTEGER"),
                bigquery.SchemaField("code", "STRING"),
                bigquery.SchemaField("contact_firstname", "STRING"),
                bigquery.SchemaField("contact_middlename", "STRING"),
                bigquery.SchemaField("contact_lastname", "STRING"),
                bigquery.SchemaField("contact_address1", "STRING"),
                bigquery.SchemaField("contact_address2", "STRING"),
                bigquery.SchemaField("contact_city", "STRING"),
                bigquery.SchemaField("contact_state", "STRING"),
                bigquery.SchemaField("contact_zip", "STRING"),
                bigquery.SchemaField("contact_county", "STRING"),
                bigquery.SchemaField("contact_homephone", "STRING"),
                bigquery.SchemaField("contact_mobilephone", "STRING"),
                bigquery.SchemaField("contact_workphone", "STRING"),
                bigquery.SchemaField("contact_email", "STRING"),
                bigquery.SchemaField("contact_preferredcontactmethod", "STRING"),
                bigquery.SchemaField("contact_birthdate", "DATE"),
                bigquery.SchemaField("contact_subscribetomailinglist", "BOOL"),
                bigquery.SchemaField("contact_badaddress", "BOOL"),
                bigquery.SchemaField("contact_deceased", "BOOL"),
                bigquery.SchemaField("contact_gender", "STRING"),
                bigquery.SchemaField("contact_minor", "BOOL"),
                bigquery.SchemaField("contact_language", "STRING"),
                bigquery.SchemaField("practicearea_name", "STRING"),
                bigquery.SchemaField("practicearea_code", "STRING"),
                bigquery.SchemaField("marketingsource", "STRING"),
                bigquery.SchemaField("contactsource", "STRING"),
                bigquery.SchemaField("talkedtootherattorneys", "BOOL"),
                bigquery.SchemaField("utm", "STRING"),
                bigquery.SchemaField("currenturl", "STRING"),
                bigquery.SchemaField("referringurl", "STRING"),
                bigquery.SchemaField("clickid", "STRING"),
                bigquery.SchemaField("clientid", "STRING"),
                bigquery.SchemaField("keywords", "STRING"),
                bigquery.SchemaField("campaign", "STRING"),
                bigquery.SchemaField("appointmentlocation", "STRING"),
                bigquery.SchemaField("office", "STRING"),
                bigquery.SchemaField("referredto_name", "STRING"),
                bigquery.SchemaField("referredbyname", "STRING"),
                bigquery.SchemaField("createddate", "DATETIME"),
                bigquery.SchemaField("incidentdate", "DATETIME"),
                bigquery.SchemaField("rejecteddate","DATETIME"),
                bigquery.SchemaField("referreddate","DATETIME"),
                bigquery.SchemaField("assigneddate","DATETIME"),
                bigquery.SchemaField("appointmentscheduleddate","DATETIME"),
                bigquery.SchemaField("chasedate","DATETIME"),
                bigquery.SchemaField("signedupdate","DATETIME"),
                bigquery.SchemaField("casecloseddate","DATETIME"),
                bigquery.SchemaField("lostdate","DATETIME"),
                bigquery.SchemaField("underreviewdate", "DATETIME"),
                bigquery.SchemaField("pendingsignupdate", "DATETIME"),
                bigquery.SchemaField("holddate","DATETIME"),
                bigquery.SchemaField("intake_firstname", "STRING"),
                bigquery.SchemaField("intake_lastname", "STRING"),
                bigquery.SchemaField("intake_email", "STRING"),
                bigquery.SchemaField("intake_code", "STRING"),
                bigquery.SchemaField("paralegal_firstname", "STRING"),
                bigquery.SchemaField("paralegal_lastname", "STRING"),
                bigquery.SchemaField("paralegal_email", "STRING"),
                bigquery.SchemaField("paralegal_code", "STRING"),
                bigquery.SchemaField("investigator_firstname", "STRING"),
                bigquery.SchemaField("investigator_lastname", "STRING"),
                bigquery.SchemaField("investigator_email", "STRING"),
                bigquery.SchemaField("investigator_code", "STRING"),
                bigquery.SchemaField("attorney_firstname", "STRING"),
                bigquery.SchemaField("attorney_lastname", "STRING"),
                bigquery.SchemaField("attorney_email", "STRING"),
                bigquery.SchemaField("attorney_code", "STRING"),
                bigquery.SchemaField("creator_firstname", "STRING"),
                bigquery.SchemaField("creator_lastname", "STRING"),
                bigquery.SchemaField("creator_email", "STRING"),
                bigquery.SchemaField("creator_code", "STRING"),
                bigquery.SchemaField("phonecall_id", "INTEGER"),
                bigquery.SchemaField("phonecall_callfrom", "STRING"),
                bigquery.SchemaField("phonecall_callto", "STRING"),
                bigquery.SchemaField("phonecall_callsid", "STRING"),
                bigquery.SchemaField("phonecall_label", "STRING"),
                bigquery.SchemaField("phonecall_recordingurl", "STRING"),
                bigquery.SchemaField("phonecall_createddate", "DATETIME")],
        write_disposition=write_mode,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)

    # For some reason, BigQuery doesn't like loading json with
    # load_table_from_json: https://stackoverflow.com/questions/59681072
    leads_as_file = io.StringIO(leads)
    load_job = client.load_table_from_file(
        leads_as_file,
        table_id,
        location="US",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    try:
        load_job.result()  # Waits for the job to complete.
        print(f"BigQuery load of {table_id} complete.")
    except ClientError as e:
        print(load_job.errors)
        raise e

def upsert_to_bigquery(prod_table_id, staging_table_id):
    """ Upserts one table into another table

    A function to simulate upsert in BigQuery. This function takes all of the records
    from a staging table and attempts to upsert those records into a production table.
    Since the schema for the staging table and production table are exactly the same,
    there should not be any data consistency issues when using a ROW insert.

    :param prod_table_id: (str) the id of a bigquery table that is considered the "master" set of data
    :param staging_table_id: (str) the id of a bigquery table that is considered the "staging" set of data
    :return: (int) the number of rows inserted into the production table
    """

    client = bigquery.Client()

    # Get the row count before the change so that the delta can be tracked
    destination_table = client.get_table(prod_table_id)
    row_count_before_query = destination_table.num_rows

    # Uses a merge query to simulate an upsert operation. When a field already exists, the query will update
    # all fields that might have changed. When not matched, a row is inserted.
    # It is assumed that information about the contact will not change (name, phone number, address, etc), but
    # the status of the lead might (dates, whether an attorney is assigned to the case, etc.)
    query = f"""
        MERGE {prod_table_id} prod
        USING {staging_table_id} staging
        ON prod.Id = staging.Id
        WHEN MATCHED THEN
          UPDATE SET status = staging.status, substatus = staging.substatus, rejecteddate = staging.rejecteddate,
          referreddate = staging.referreddate, assigneddate = staging.assigneddate,
          appointmentscheduleddate = staging.appointmentscheduleddate, chasedate = staging.chasedate,
          signedupdate = staging.signedupdate, casecloseddate = staging.casecloseddate, lostdate = staging.lostdate,
          underreviewdate = staging.underreviewdate, pendingsignupdate = staging.pendingsignupdate,
          holddate = staging.signedupdate, paralegal_firstname = staging.paralegal_firstname, 
          paralegal_lastname = staging.paralegal_lastname,paralegal_email = staging.paralegal_email, 
          paralegal_code = staging.paralegal_code, investigator_firstname = staging.investigator_firstname,
          investigator_lastname = staging.investigator_lastname, investigator_email = staging.investigator_email, 
          investigator_code = staging.investigator_code, attorney_firstname = staging.attorney_firstname, 
          attorney_lastname = staging.attorney_lastname,attorney_email = staging.attorney_email, 
          attorney_code = staging.attorney_code, phonecall_id = staging.phonecall_id,
          phonecall_callfrom = staging.phonecall_callfrom, phonecall_callto = staging.phonecall_callto,
          phonecall_callsid = staging.phonecall_callsid, phonecall_label = staging.phonecall_label,
          phonecall_recordingurl = staging.phonecall_recordingurl, phonecall_createddate = staging.phonecall_createddate
        WHEN NOT MATCHED THEN
          INSERT ROW
    """

    query_job = client.query(query)
    query_job.result()  # Waits for query to finish

    destination_table = client.get_table(prod_table_id)
    rows_inserted = (destination_table.num_rows - row_count_before_query)

    print("The merge query processed {} bytes.".format(query_job.total_bytes_processed))
    print("The merge query loaded {} rows.".format(rows_inserted))
    return rows_inserted

def normalize_lead(detailed_lead):
    """ Normalizes a single detailed lead

    A helper function that normalizes the data in a lead. This normalization process ensures that there are no
    data processing errors.

    The normalization includes adding null values to avoid variable length data, lowercasing lead keys,
    and flattening the json. This method builds a lead up in the desired format as an additive process
    instead of removing fields so that any additional fields that are added into the API in the future won't
    cause this function to fail.

    For consistency's sake, the lead format chosen by this method excludes all data that isn't present in the
    lead reports that can be exported via the Lead Docket UI. This ensures that data from the API is consistent with
    the data from historical reports that can be exported.

    :param detailed_lead: (dict) a single, detailed LeadDocket lead loaded into a python dict
    :return: (dict) a normalized LeadDocket lead loaded into a python dict
    """

    def _normalize_person_field(json_data, field):
        if not json_data[field]:
            json_data[field] = {
                "FirstName": None,
                "LastName": None,
                "Email": None,
                "Code": None
            }

        return json_data

    def _normalize_referral_field(json_data, field):
        if not json_data[field]:
            json_data[field] = {
                "Name": None,
            }

        return json_data

    def _normalize_phonecall_field(json_data, field):
        if not json_data[field]:
            json_data[field] = {
                "Id": None,
                "CallFrom": None,
                "CallTo": None,
                "CallSID": None,
                "Label": None,
                "RecordingUrl": None,
                "CreatedDate": None
            }
        return json_data

    if detailed_lead['Contact']['Birthdate']:
        detailed_lead['Contact']['Birthdate'] = convert_datetime_to_date(detailed_lead['Contact']['Birthdate'])

    detailed_lead = convert_severity_level_to_severity_id(detailed_lead)
    detailed_lead = _normalize_person_field(detailed_lead, "Paralegal")
    detailed_lead = _normalize_person_field(detailed_lead, "Investigator")
    detailed_lead = _normalize_person_field(detailed_lead, "Attorney")
    detailed_lead = _normalize_person_field(detailed_lead, "Creator")
    detailed_lead = _normalize_person_field(detailed_lead, "Investigator")
    detailed_lead = _normalize_person_field(detailed_lead, "Intake")
    detailed_lead = _normalize_referral_field(detailed_lead, "ReferredBy")
    detailed_lead = _normalize_referral_field(detailed_lead, "ReferredTo")
    detailed_lead = _normalize_phonecall_field(detailed_lead, "PhoneCall")

    new_lead = {}
    new_lead['id'] = detailed_lead['Id']
    new_lead['status'] = detailed_lead['Status']
    new_lead['substatus'] = detailed_lead['SubStatus']
    new_lead['severitylevel'] = convert_severity_level_to_severity_id(detailed_lead)['SeverityLevel']
    new_lead['code'] = detailed_lead['Code']
    new_lead['contact_firstname'] = detailed_lead['Contact']['FirstName']
    new_lead['contact_middlename'] = detailed_lead['Contact']['MiddleName']
    new_lead['contact_lastname'] = detailed_lead['Contact']['LastName']
    new_lead['contact_address1'] = detailed_lead['Contact']['Address1']
    new_lead['contact_address2'] = detailed_lead['Contact']['Address2']
    new_lead['contact_city'] = detailed_lead['Contact']['City']
    new_lead['contact_state'] = detailed_lead['Contact']['State']
    new_lead['contact_zip'] = detailed_lead['Contact']['Zip']
    new_lead['contact_county'] = detailed_lead['Contact']['County']
    new_lead['contact_homephone'] = detailed_lead['Contact']['HomePhone']
    new_lead['contact_mobilephone'] = detailed_lead['Contact']['MobilePhone']
    new_lead['contact_workphone'] = detailed_lead['Contact']['WorkPhone']
    new_lead['contact_email'] = detailed_lead['Contact']['Email']
    new_lead['contact_preferredcontactmethod'] = detailed_lead['Contact']['PreferredContactMethod']
    new_lead['contact_birthdate'] = detailed_lead['Contact']['Birthdate']
    new_lead['contact_subscribetomailinglist'] = detailed_lead['Contact']['SubscribeToMailingList']
    new_lead['contact_badaddress'] = detailed_lead['Contact']['BadAddress']
    new_lead['contact_deceased'] = detailed_lead['Contact']['Deceased']
    new_lead['contact_gender'] = detailed_lead['Contact']['Gender']
    new_lead['contact_minor'] = detailed_lead['Contact']['Minor']
    new_lead['contact_language'] = detailed_lead['Contact']['Language']
    new_lead['practicearea_name'] = detailed_lead['PracticeArea']['Name']
    new_lead['practicearea_code'] = detailed_lead['PracticeArea']['Code']
    new_lead['marketingsource'] = detailed_lead['MarketingSource']
    new_lead['contactsource'] = detailed_lead['ContactSource']
    new_lead['talkedtootherattorneys'] = detailed_lead['TalkedToOtherAttorneys']
    new_lead['utm'] = detailed_lead['UTM']
    new_lead['currenturl'] = detailed_lead['CurrentUrl']
    new_lead['clickid'] = detailed_lead['ClickId']
    new_lead['clientid'] = detailed_lead['ClientId']
    new_lead['keywords'] = detailed_lead['Keywords']
    new_lead['campaign'] = detailed_lead['Campaign']
    new_lead['appointmentlocation'] = detailed_lead['AppointmentLocation']
    new_lead['office'] = detailed_lead['Office']
    new_lead['referredto_name'] = detailed_lead['ReferredTo']['Name']
    new_lead['referredbyname'] = detailed_lead['ReferredByName']
    new_lead['createddate'] = detailed_lead['CreatedDate']
    new_lead['incidentdate'] = detailed_lead['IncidentDate']
    new_lead['rejecteddate'] = detailed_lead['RejectedDate']
    new_lead['referreddate'] = detailed_lead['ReferredDate']
    new_lead['assigneddate'] = detailed_lead['AssignedDate']
    new_lead['appointmentscheduleddate'] = detailed_lead['AppointmentScheduledDate']
    new_lead['chasedate'] = detailed_lead['ChaseDate']
    new_lead['signedupdate'] = detailed_lead['SignedUpDate']
    new_lead['casecloseddate'] = detailed_lead['CaseClosedDate']
    new_lead['lostdate'] = detailed_lead['LostDate']
    new_lead['underreviewdate'] = detailed_lead['UnderReviewDate']
    new_lead['pendingsignupdate'] = detailed_lead['PendingSignupDate']
    new_lead['holddate'] = detailed_lead['HoldDate']
    new_lead['paralegal_firstname'] = detailed_lead['Paralegal']['FirstName']
    new_lead['paralegal_lastname'] = detailed_lead['Paralegal']['LastName']
    new_lead['paralegal_email'] = detailed_lead['Paralegal']['Email']
    new_lead['paralegal_code'] = detailed_lead['Paralegal']['Code']
    new_lead['investigator_firstname'] = detailed_lead['Investigator']['FirstName']
    new_lead['investigator_lastname'] = detailed_lead['Investigator']['LastName']
    new_lead['investigator_email'] = detailed_lead['Investigator']['Email']
    new_lead['investigator_code'] = detailed_lead['Investigator']['Code']
    new_lead['attorney_firstname'] = detailed_lead['Attorney']['FirstName']
    new_lead['attorney_lastname'] = detailed_lead['Attorney']['LastName']
    new_lead['attorney_email'] = detailed_lead['Attorney']['Email']
    new_lead['attorney_code'] = detailed_lead['Attorney']['Code']
    new_lead['creator_firstname'] = detailed_lead['Creator']['FirstName']
    new_lead['creator_lastname'] = detailed_lead['Creator']['LastName']
    new_lead['creator_email'] = detailed_lead['Creator']['Email']
    new_lead['creator_code'] = detailed_lead['Creator']['Code']
    new_lead['phonecall_id'] = detailed_lead['PhoneCall']['Id']
    new_lead['phonecall_callfrom'] = detailed_lead['PhoneCall']['CallFrom']
    new_lead['phonecall_callto'] = detailed_lead['PhoneCall']['CallTo']
    new_lead['phonecall_callsid'] = detailed_lead['PhoneCall']['CallSID']
    new_lead['phonecall_label'] = detailed_lead['PhoneCall']['Label']
    new_lead['phonecall_recordingurl'] = detailed_lead['PhoneCall']['RecordingUrl']
    new_lead['phonecall_createddate'] = detailed_lead['PhoneCall']['CreatedDate']

    return new_lead

def convert_severity_level_to_severity_id(detailed_lead):
    """ Convert Severity To Severity Level

    This simple function converts a Severity Level string to a Severity Id integer.
    This is necessary because UI reports only give severity ID, while the API only gives
    severity text.

    :param detailed_lead: (dict) a single lead represented as a dictionary (with a SeverityLevel)
    :return detailedLead (dict) a single lead represented as a dictionary (with a SeverityId)
    """
    if detailed_lead['SeverityLevel'] == "No Case":
        detailed_lead['SeverityLevel'] = 1
    if detailed_lead['SeverityLevel'] == "Unlikely Case - No Injuries":
        detailed_lead['SeverityLevel'] = 2
    if detailed_lead['SeverityLevel'] == "Possible Case - Minor Injuries / Light Therapy / Short Hospital Stay":
        detailed_lead['SeverityLevel'] = 3
    if detailed_lead['SeverityLevel'] == "Likely Case - Moderate Injuries / Ongoing Treatment":
        detailed_lead['SeverityLevel'] = 4
    if detailed_lead['SeverityLevel'] == "Very Likely Case - Severe Injuries / Catastrophic":
        detailed_lead['SeverityLevel'] = 5

    return detailed_lead


def get_lead_details(leads_to_update, headers):
    """ Gets the details for every lead in a set of leads

    A function that gets all of the details for each lead in a list of LeadDocket leads. This function
    iterates through a list of leads and gathers detailed information on all of them, then returns a list
    of detailed leads with all of the information needed to store in BigQuery.

    :param leads_to_update: (list) A list of LeadDocket lead summaries in JSON format
    :param headers: (str) the headers to use for API requests to LeadDocket
    :return: (list) List of detailed leads
    """

    global TOTALREQUESTS
    detailed_leads = []
    current_lead_counter = 1
    for lead in leads_to_update:
        print(f"Processing lead {current_lead_counter} of {LEADCOUNT}")
        lead_by_id_endpoint = f"leads/{lead['Id']}"
        url = urljoin(LEAD_DOCKET_BASE_URL, lead_by_id_endpoint)

        # Pass the response to an error handler to deal with rate limits and unexpected errors.
        # The error handler will retry a single failed request
        response = handle_api_errors(requests.get(url, headers=headers), url, headers)
        TOTALREQUESTS +=1

        if (response.status_code != 200):
            print(f"API requests have continued to fail on lead {current_lead_counter} of {LEADCOUNT}")
            exit("API requests failing after waiting. Exiting Early")

        detailed_lead = response.json()

        # Normalize the lead to ensure data consistency
        detailed_leads.append(normalize_lead(detailed_lead))

        current_lead_counter += 1

    return detailed_leads

def get_secret(project_id, secret_id):
    """ Get a secret from Google Secret Manager

    A simple function that retrieves the specified secret from Google Secret Manager
    given a project ID and secret ID.

    :param project_id: (str) Google Project ID
    :param secret_id: (str) Secret Manager Secret ID
    :return: (dict) Secret payload
    """

    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    client = secretmanager.SecretManagerServiceClient()

    try:
        response = client.access_secret_version(request={"name": name})
    except ClientError as e:
        raise e

    payload = response.payload.data.decode("UTF-8")
    return payload

def get_lead_docket_headers():
    """ Get Lead Docket Headers

    This is a simple function to get the static headers for API requests to LeadDocket. This
    function exists to lessen the number of calls to the fetch the API key.

    :return: headers (dict) a dictionary of headers to use for Lead Docket API requests
    """
    project_id = os.environ.get('PROJECT_ID')
    secret_id = os.environ.get('SECRET_ID')

    if not (project_id, secret_id):
        exit("Required environment variables not set. Please ensure PROJECT_ID and SECRET_ID have been configured"
             "have been configured")

    headers = {
        'Accept': 'application/json',
        'api_key': f"{get_secret(project_id=project_id, secret_id=secret_id)}"
    }
    return headers


def google_cloud_main(event, context):
    """ Entry point for a Google Cloud Function

    A top-level function that pulls data from lead docket, pushes them to a staging table, and then
    merges the staging table into the production table to avoid duplicate records.

    - Deployment -

    Deployment should have the following characteristics:
      - A python 3.8 environment
      - A pub/sub topic trigger
      - Environmental variable set for DATASET_ID, PROD_TABLE_ID, STAGING_TABLE_ID,
        SECRET_ID, PROJECT_ID, and LEAD_DOCKET_BASE_URL

    - Pub/Sub -

    This function is designed to be the entry-point when this source code is deployed in GCP via
    a pub/sub topic. Messages in this topic should contain a single integer, which represents
    a number of minutes in the past that the script should look for new leaddocket data. For
    example, "60" would look at the last hour of leaddocket leads.

        Default message every 10 minutes: 12 (for -12 minutes from the current time)

    :param event: (str) Google Event from a pub/sub topic
    :param context: (str) Google Context from a pub/sub topic
    :return: None
    """
    dataset_id = os.environ.get('DATASET_ID')
    prod_table_id = os.environ.get('PROD_TABLE_ID')
    staging_table_id = os.environ.get('STAGING_TABLE_ID')

    if not (dataset_id, prod_table_id, staging_table_id):
        exit("Required environment variables not set. Please ensure DATASET_ID, PROD_TABLE_ID, and STAGING_TABLE_ID"
             "have been configured")

    client = bigquery.Client()

    def _dataset_exists(client, dataset_id):
        try:
            dataset = client.get_dataset(dataset_id)
            if dataset:
                return True
        except NotFound:
            return False

    def _table_exists(client, table_id):
        try:
            table = client.get_table(table_id)  # Make an API request.
            if table:
                return True
        except NotFound:
            return False

    # By default, Python reads input as a string. Convert the string to an integer.
    pub_sub_time = int(base64.b64decode(event['data']).decode('utf-8'))
    headers = get_lead_docket_headers()
    leads_to_update = get_lead_changes_since(pub_sub_time, headers)
    detailed_leads = get_lead_details(leads_to_update, headers)

    if not (_dataset_exists(client, dataset_id)):
        print(f"The {dataset_id} dataset does not exist. Creating dataset...")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset, timeout=30)
        print(f"The {dataset_id} dataset has been created")


    if not (_table_exists(client, prod_table_id)):
        # If the prod table doesn't exist, write directly to the prod table
        upload_to_bigquery(convert_to_newline_delimeted_json(detailed_leads), prod_table_id, write_mode=bigquery.WriteDisposition.WRITE_EMPTY)

    else:
        # If the prod table already exists, overwrite the staging page
        upload_to_bigquery(convert_to_newline_delimeted_json(detailed_leads), staging_table_id, write_mode=bigquery.WriteDisposition.WRITE_TRUNCATE)
        # Merge the staging table into the production table
        upsert_to_bigquery(prod_table_id, staging_table_id)