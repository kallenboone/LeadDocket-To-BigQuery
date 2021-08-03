import datetime
import requests
import json
import time
import logging
from urllib.parse import urljoin
from google.cloud import bigquery
from google.api_core.exceptions import ClientError
from google.cloud.exceptions import NotFound

start_time = time.time()
leadCount = 0
BASE_URL = "https://REDACTED.leaddocket.com/api/"
headers = {
    'Accept': 'application/json',
    'api_key': "REDACTED"
}


def saveDataToFile(fileName, jsonData):
    f = open(fileName, "w")
    f.write(json.dumps(jsonData))

def saveDataToCSV(fileName, jsonData):
    f = open(fileName, "w")

    # Iterate through the data once to write the column headers
    for record in jsonData[:1]:
        for field in record:
            f.write(f"{field}; ")

        f.write("\n")

    # Iterate through the rest of the data and grab the values
    for record in jsonData:
        for field in record:
            f.write(f"{record[field]}; ")

        f.write("\n")

def removeUnwantedData(record):
    if(record["PreferredContactMethod"]):
        del record["PreferredContactMethod"]

# # Lost leads represent screened leads that are in the "Lost" status
# def findLeadsInStatus(status):
#     # Create an empty dictionary to store all of the records we get from API requests
#     allLeadsForThisStatus = []
#
#     # Configure the URL to point to the status endpoint
#     statusEndPoint = f"leads?status={status}"
#     url = urljoin(BASE_URL, statusEndPoint)
#
#     print(f"Getting {status} leads...")
#     response = requests.get(url, headers=headers)
#     leadsJson = response.json()
#
#     def extractRecords():
#         for record in leadsJson['Records']:
#             allLeadsForThisStatus.append(record)
#
#     # Extract the records from the API data (specifically, grab the "Records" array)
#     extractRecords()
#     # Grab any additional pages of results
#     if leadsJson['TotalPages'] > 1:
#         print("Additional pages of leads found, grabbing more data...")
#
#         # Start the loop on page two since we already gathered page one with our initial request
#         pageIter = 2
#
#         # Send a new API request for each new page of data
#         while pageIter <= leadsJson['TotalPages']:
#             pageEndpoint = f"{statusEndPoint}&page={pageIter}"
#             pageUrl = urljoin(url, pageEndpoint)
#             print("Getting records on page:", pageIter)
#             response = requests.get(pageUrl, headers=headers)
#
#             # Extract the records from the API data
#             leadsJson = response.json()
#             extractRecords()
#             pageIter += 1
#
#     print(f"All {status} leads gathered")
#     return allLeadsForThisStatus

# def getStatuses():
#     # Configure the URL to point to the status list endpoint
#     url = urljoin(BASE_URL, "statuses/list")
#
#     # Hit the status list API to make sure we are using an
#     # up-to-date set of statuses
#     response = requests.get(url, headers=headers)
#     statusJson = response.json()
#
#     # Iterate through the statuses to just pull out the top-level
#     # status names
#     statuses = []
#     for status in statusJson:
#         statuses.append(status['StatusName'])
#
#     return statuses


# def gatherAllLeads():
#     # Get a current list of statuses
#     statuses = getStatuses()
#
#     # Create an empty array to store all leads
#     leads = []
#
#     # Gather leads for every status and remove unused fields in the process
#     for status in statuses:
#         if not leads:
#             leads = removeUnwantedData(findLeadsInStatus(status))
#         else:
#             leads.append(removeUnwantedData(findLeadsInStatus(status)))
#
#     return leads




   # def extractRecords():
#         for record in leadsJson['Records']:
#             allLeadsForThisStatus.append(record)

def getLeadChangesSince(minutes):
    # Find the time in the past that corresponds to the number of minutes
    # sent as an input parameter and format it for an API request
    print(f"Scanning for changes in the last {minutes} minutes")
    now = datetime.datetime.now()
    minutesAgo = now - datetime.timedelta(minutes=minutes)
    timeToQuery = minutesAgo.strftime("%Y-%m-%d")
    # timeToQuery = minutesAgo.strftime("%Y-%m-%dT%H:%M")

    # Look for changes since timeToQuery
    lastStatusChangesSinceEndpoint = f"leads/laststatuschangesince?date={timeToQuery}"
    url = urljoin(BASE_URL, lastStatusChangesSinceEndpoint)
    response = requests.get(url, headers=headers)

    print(f"Initial request returned with status code: {response.status_code}")

    changesJson = response.json()

    # Output and store information about the total number of leads found
    print(f"{changesJson['TotalRecordCount']} changes found in the last {minutes} minutes")
    global leadCount
    leadCount = changesJson['TotalRecordCount']

    # Extract the records from the API response
    recordsToUpdate = []

    def extractRecords(json_to_extract):
        for record in json_to_extract['Records']:
            removeUnwantedData(record)
            recordsToUpdate.append(record)

    extractRecords(changesJson)

    if changesJson['TotalPages'] > 1:
        print("Additional pages of leads found, grabbing more data...")

        # Start the loop on page two since we already gathered page one with our initial request
        pageIter = 2

        # Send a new API request for each new page of data
        while pageIter <= changesJson['TotalPages']:
            pageEndpoint = f"{lastStatusChangesSinceEndpoint}&page={pageIter}"
            pageUrl = urljoin(BASE_URL, pageEndpoint)
            print("Getting records on page:", pageIter)
            response = requests.get(pageUrl, headers=headers)
            print(f"Additional page request returned with status code: {response.status_code}")

            # Extract the records from the API data
            leadsJson = response.json()

            extractRecords(leadsJson)
            pageIter += 1

    return recordsToUpdate


# def uploadToStaging(leads):
#     """ Export to Big Query
#
#     This function takes an array of JSON formatted leads and
#     exports the leads to a BigQuery table.
#
#     :param leads: (array) array of leads in JSON format to be uploaded to BigQuery
#     """
#     client = bigquery.Client()
#
#     table_id = 'eclipselegalmarketing.leaddocket.REDACTEDstaging'
#
#     job_config = bigquery.LoadJobConfig(
#         schema=[bigquery.SchemaField("Id", "INTEGER"),
#                 bigquery.SchemaField("ContactId", "INTEGER"),
#                 bigquery.SchemaField("PhoneNumber", "STRING"),
#                 bigquery.SchemaField("MobilePhone", "STRING"),
#                 bigquery.SchemaField("HomePhone", "STRING"),
#                 bigquery.SchemaField("WorkPhone", "STRING"),
#                 bigquery.SchemaField("Email", "STRING"),
#                 bigquery.SchemaField("FirstName", "STRING"),
#                 bigquery.SchemaField("LastName", "STRING"),
#                 bigquery.SchemaField("StatusId", "INTEGER"),
#                 bigquery.SchemaField("StatusName", "STRING"),
#                 bigquery.SchemaField("SubStatusId", "INTEGER"),
#                 bigquery.SchemaField("SubStatusName", "STRING"),
#                 bigquery.SchemaField("CaseType", "STRING"),
#                 bigquery.SchemaField("Code", "INTEGER"),
#                 bigquery.SchemaField("LastStatusChangeDate", "STRING"),
#                 bigquery.SchemaField("previousPeriodPhoneCalls", "INTEGER")],
#         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#         source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
#
#     load_job = client.load_table_from_json(
#         leads,
#         table_id,
#         location="US",  # Must match the destination dataset location.
#         job_config=job_config,
#     )  # Make an API request.
#
#     try:
#         load_job.result()  # Waits for the job to complete.
#     except ClientError as e:
#         logging.error(load_job.errors)
#         raise e

# def upsertIntoBigQuery(table_id):
#     # Perform a query.
#
#     job_config = bigquery.QueryJobConfig(destination=table_id)
#     sql = """
#         MERGE eclipselegalmarketing.leaddocket.REDACTEDstaging prod
#         USING eclipselegalmarketing.leaddocket.REDACTED staging
#         ON prod.id = staging.id
#         WHEN MATCHED THEN
#           UPDATE SET status_name = staging.status_name
#           UPDATE SET status_id = staging.status_id
#         WHEN NOT MATCHED THEN
#           INSERT (id, value) VALUES(id, value)
#     """
#     client = bigquery.Client()
#     query_job = client.query(sql, job_config=job_config)
#     rows = query_job.result()  # Waits for query to finish
#
#     updateCount = 0
#     for row in rows:
#         updateCount += 1
#
#     return updateCount

def mainHandler():
    # client = bigquery.Client()
    # table_id = 'eclipselegalmarketing.leaddocket.REDACTED'

    # def tableExists(client, table_id):
    #     try:
    #         table = client.get_table(table_id)  # Make an API request.
    #         return True
    #     except NotFound:
    #         return False

    # if not (tableExists(client, table_id)):
        # Calculate 3 years in minutes
    # threeYears = 60 * 24 * 365 * 3
    # leadsToChange = getLeadChangesSince(threeYears)

    # else:
    fourYears = 60 * 24 * 365 * 4
    leadsToChange = getLeadChangesSince(fourYears)

    saveDataToCSV("output.txt", leadsToChange)
    saveDataToFile("output.json", leadsToChange)
    # uploadToStaging(leadsToChange)
    # upsertToBigQuery(table_id)

    execution_time = time.time() - start_time
    print(f"Processing of {leadCount} records complete in {execution_time} seconds.")


mainHandler()