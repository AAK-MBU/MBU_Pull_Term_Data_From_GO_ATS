"""Module to handle pulling term data from GetOrganized (GO)."""
import re
import json
import requests
from requests_ntlm import HttpNtlmAuth

from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure


class APIClient:
    """
    Class to handle API requests.
    """
    def __init__(self, username, password):
        """
        Initialize the APIClient with NTLM authentication credentials.

        Args:
            username (str): The NTLM username.
            password (str): The NTLM password.
        """
        self.auth = HttpNtlmAuth(username, password)

    def get_form_digest(self, url):
        """
        Get form digest value by making an API POST request.

        Args:
            url (str): The URL to fetch the form digest value from.

        Returns:
            str: The form digest value if successful, None otherwise.
        """
        headers = {
            "Content-Type": "application/json; charset=UTF-8"
        }
        try:
            response = requests.post(url, headers=headers, auth=self.auth, timeout=60)
            response.raise_for_status()
            return re.search(r'formDigestValue":"([^"]+)"', response.text).group(1)
        except requests.exceptions.RequestException as e:
            print(f"Request failed. Error message: {str(e)}")
            return None

    def post_data(self, url, headers, body):
        """
        Post data to a given URL.

        Args:
            url (str): The endpoint URL.
            headers (dict): HTTP headers to include in the request.
            body (dict): JSON body to send with the request.

        Returns:
            dict: JSON response if successful, None otherwise.
        """
        try:
            response = requests.post(url, headers=headers, json=body, auth=self.auth, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Request failed. Error message: {str(e)}")
            return None


def _get_child_terms(api_client, headers, parent_uuid, term_set_uuid, base_url, case_type):
    """
    Retrieve child terms for a given parent term.

    Args:
        api_client (APIClient): Instance of the APIClient to make requests.
        headers (dict): HTTP headers to include in the request.
        parent_uuid (str): UUID of the parent term. If None, root terms are retrieved.
        term_set_uuid (str): UUID of the term set.
        base_url (str): Base URL for the API.
        case_type (str): Case type to be used in the API endpoint.

    Returns:
        dict: A dictionary representing the term hierarchy with children.
    """
    parent_id_bool = not parent_uuid

    body = {
        "guid": parent_uuid,
        "includeDeprecated": True,
        "includeNoneTaggableTermset": True,
        "lcid": 1030,
        "listId": "00000000-0000-0000-0000-000000000000",
        "sspId": "fa62fa7306a44d3fac304c119cbd4bd7",
        "webId": "00000000-0000-0000-0000-000000000000",
        "includeCurrentChild": True,
        "currentChildId": "00000000-0000-0000-0000-000000000000",
        "pagingForward": True,
        "pageLimit": 2000
    }

    if not parent_id_bool:
        body["termsetId"] = term_set_uuid

    url = (
        f"{base_url}/{case_type}/_vti_bin/taxonomyinternalservice.json/GetChildTermsInTermSetWithPaging"
        if parent_id_bool
        else f"{base_url}/{case_type}/_vti_bin/taxonomyinternalservice.json/GetChildTermsInTermWithPaging"
    )

    json_data = api_client.post_data(url, headers, body)
    if json_data and "d" in json_data and "Content" in json_data["d"]:
        content = json_data["d"]["Content"]
        result = {"Id": parent_uuid, "Children": []}

        for node in content:
            node_name = node.get("Nm")
            node_id = node.get("Id")

            if node_name and node_id:
                child = {"Name": node_name, "Id": node_id, "ParentId": parent_uuid}

                if node.get("Cc", 0) > 0:
                    child["Children"] = _get_child_terms(api_client, headers, node_id, term_set_uuid, base_url, case_type)["Children"]

                result["Children"].append(child)

        return result

    return None


def _save_json(data, file_path):
    """
    Save data as a JSON file.

    Args:
        data (dict): Data to be saved as JSON.
        file_path (str): Path to the output JSON file.
    """
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def _insert_term_data_to_sql(data, sql_conn_string, term_set_uuid, sql_stored_procedure):
    """
    Recursively insert data into the SQL table.

    Args:
        data (dict): Dictionary containing term data to insert.
        sql_conn_string (str): SQL connection string.
        term_set_uuid (str): UUID of the term set.
        sql_stored_procedure (str): Name of the SQL stored procedure to execute.
    """
    if data:
        name = data.get("Name")
        uuid = data.get("Id")
        parent_uuid = data.get("ParentId")
        sql_data_params = {
                "name": ("str", f"{name}"),
                "uuid": ("str", f"{uuid}"),
                "parent_uuid": ("str", f"{parent_uuid}"),
                "term_set_uuid": ("str", f"{term_set_uuid}")
            }
        execute_stored_procedure(sql_conn_string, f"rpa.{sql_stored_procedure}", sql_data_params)

        for child in data.get("Children", []):
            _insert_term_data_to_sql(child, sql_conn_string, term_set_uuid, sql_stored_procedure)


def pull_term_data_from_go_to_sql(credentials, base_url, case_type, start_term_id, sql_stored_procedure, term_set_uuid):
    """
    Fetch term data from an API and insert the data into a SQL database.

    Args:
        credentials (dict): Dictionary containing authentication credentials (e.g., API username and password)
                            and SQL connection string.
        base_url (str): Base URL for the API.
        case_type (str): Case type to determine the appropriate API endpoint.
        start_term_id (str): UUID of the starting term (parent term or root term).
        sql_stored_procedure (str): Name of the SQL stored procedure to execute for inserting data.
        term_set_uuid (str): UUID of the term set to be used in the API request.
    """
    url_form_digest = f"{base_url}/{case_type}/_layouts/15/termstoremanager.aspx"

    api_client = APIClient(credentials['go_api_username'], credentials['go_api_password'])

    form_digest_value = api_client.get_form_digest(url_form_digest)

    if form_digest_value:
        headers = {
            "Content-Type": "application/json; charset=UTF-8",
            "X-RequestDigest": form_digest_value
        }

        result = _get_child_terms(api_client, headers, start_term_id, term_set_uuid, base_url, case_type)

        _insert_term_data_to_sql(result, credentials['sql_conn_string'], term_set_uuid, sql_stored_procedure)
