"""
This module provides functionality for fetching data from a SharePoint Taxonomy Hidden List
and inserting the retrieved data into a database using a stored procedure.
"""
import json
import requests
from requests_ntlm import HttpNtlmAuth

from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure


def fetch_data(session, url):
    """
    Sends a POST request to the specified URL using the provided session and returns the JSON response.

    Args:
        session (requests.Session): The session object to use for the request.
        url (str): The URL to send the POST request to.

    Returns:
        dict: The JSON response from the server.

    Raises:
        Exception: If the response status is not OK, raises an exception with the error message.
    """
    response = session.post(url)
    if not response.ok:
        raise requests.exceptions.RequestException(f"Error fetching data: {response.text}")
    return response.json()


def insert_into_database(connection_string, stored_procedure, data, case_type):
    """
    Inserts a list of records into the database by executing a stored procedure for each record.

    Args:
        connection_string (str): The connection string for the database.
        stored_procedure (str): The name of the stored procedure to execute.
        data (list of dict): The list of data records to insert into the database.
        case_type (str): Case type.

    Prints:
        str: A message indicating the failure of an insertion along with the error message.
    """
    for item in data:
        params = {
            "ID": ("str", item.get("ID", "")),
            "Title": ("str", item.get("Title", "")),
            "IdForTermStore": ("str", item.get("IdForTermStore", "")),
            "IdForTerm": ("str", item.get("IdForTerm", "")),
            "IdForTermSet": ("str", item.get("IdForTermSet", "")),
            "Path": ("str", item.get("Path", "")),
            "CaseType": ("str", case_type)
        }
        print(params)
        result = execute_stored_procedure(connection_string, stored_procedure, params)
        if not result["success"]:
            print(f"Failed to insert record: {result['error_message']}")


def get_taxononmy(credentials, case_type, view_id, base_url):
    """
    Fetches data from a SharePoint Taxonomy Hidden List and inserts it into a database.

    Args:
        credentials (dict): A dictionary containing credentials for authentication and database connection.
        case_type (str): The case type used to build the SharePoint endpoint.
        view_id (str): The ID of the SharePoint view.
        base_url (str): The base URL of the SharePoint site.

    Prints:
        str: Messages indicating the progress of data fetching and insertion, or errors if they occur.
    """
    endpoint = f"/{case_type}/_api/web/GetList('%2F{case_type}%2FLists%2FTaxonomyHiddenList')/RenderListDataAsStream"
    initial_url = f"{endpoint}?Paged=TRUE&p_ID=0&PageFirstRow=31&View={view_id}"
    full_url = base_url + initial_url

    session = requests.Session()
    session.auth = HttpNtlmAuth(credentials['go_api_username'], credentials['go_api_password'])

    all_rows = []
    next_url = full_url

    try:
        while next_url:
            json_data = fetch_data(session, next_url)
            print(f"Fetched data from: {next_url}")
            if "Row" in json_data:
                all_rows.extend(json_data["Row"])
            if "NextHref" in json_data:
                next_url = base_url + endpoint + json_data["NextHref"]
            else:
                next_url = None
        insert_into_database(credentials['sql_conn_string'], "rpa.GO_TaxonomyList_Insert", all_rows, case_type)
        print("All rows have been inserted into the database.")
    except requests.exceptions.RequestException as re:
        print(f"Request error occurred: {str(re)}")
    except KeyError as ke:
        print(f"Missing key in credentials: {str(ke)}")
    except TypeError as te:
        print(f"Type error: {str(te)}")
    except json.JSONDecodeError as je:
        print(f"JSON decode error: {str(je)}")
