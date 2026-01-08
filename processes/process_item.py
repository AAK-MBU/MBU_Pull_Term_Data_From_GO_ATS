"""Module to handle item processing"""
# from mbu_rpa_core.exceptions import ProcessError, BusinessError

import os

import logging

from mbu_dev_shared_components.database.connection import RPAConnection

from helpers.taxonomy_data_handler import get_taxononmy
from helpers.term_data_handler import pull_term_data_from_go_to_sql

logger = logging.getLogger(__name__)


def process_item(item_data: dict, item_reference: str):
    """Function to handle item processing"""
    assert item_data, "Item data is required"
    assert item_reference, "Item reference is required"

    with RPAConnection as rpa_conn:
        go_api_creds = rpa_conn.get_credential("go_api")
        go_api_username = go_api_creds.get("username", "")
        go_api_password = go_api_creds.get("decrypted_password", "")

        creds = {
            "go_api_username": go_api_username,
            "go_api_password": go_api_password,
            "sql_conn_string": os.getenv("DBCONNECTIONSTRINGPROD")
        }

    process = item_data.get("process")
    base_url = item_data.get("baseUrl")
    case_type = item_data.get("caseType")

    if process == "taxonomy":
        logger.info("Pull taxonomy data from GO")

        view_id = item_data.get("viewData")

        get_taxononmy(
            credentials=creds,
            case_type=case_type,
            view_id=view_id,
            base_url=base_url
        )

        logger.info("Taxonomy data was successfully pulled from GO")

    elif process == "term":
        logger.info("Pull term data from GO")

        sql_stored_procedure = item_data.get("storedProcedure")
        object_type = item_data.get("objectType")
        start_term_id = item_data.get("startTermId")
        term_set_uuid = item_data.get("termSetUuid")

        pull_term_data_from_go_to_sql(
            credentials=creds,
            base_url=base_url,
            case_type=case_type,
            start_term_id=start_term_id,
            sql_stored_procedure=sql_stored_procedure,
            term_set_uuid=term_set_uuid
        )

        logger.info("Term data was successfully pulled from GO")
