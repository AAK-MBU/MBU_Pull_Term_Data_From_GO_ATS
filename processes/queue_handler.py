"""Module to hande queue population"""

import asyncio
import json
import logging

from datetime import datetime

from automation_server_client import Workqueue

from helpers import config

logger = logging.getLogger(__name__)

proces_arguments = {
    "go_pull_taxonomy_data_bor": {
        "process": "taxonomy",
        "baseUrl": "https://ad.go.aarhuskommune.dk",
        "caseType": "borgersager",
        "viewId": "f8f14409-e2f6-452f-aeac-acc18b3e6b5c"
    },
    "go_pull_taxonomy_data_emn": {
        "process": "taxonomy",
        "baseUrl": "https://ad.go.aarhuskommune.dk",
        "caseType": "emnesager",
        "viewId": "db7f8be3-a3cb-4ea2-b495-7eba638f3fc7"
    },
    "go_pull_term_data_case_profile_bor": {
        "process": "term",
        "baseUrl": "https://ad.go.aarhuskommune.dk",
        "caseType": "borgersager",
        "storedProcedure": "GO_CaseProfiles_Insert",
        "objectType": "case profiles",
        "startTermId": "dc87e41a-cab3-4286-8a1b-72ba6368cb90",
        "termSetUuid": "8adfc3ee-428f-47fb-80aa-c285c28d3d93"
    },
    "go_pull_term_data_case_profile_emn": {
        "process": "term",
        "baseUrl": "https://ad.go.aarhuskommune.dk",
        "caseType": "emnesager",
        "storedProcedure": "GO_CaseProfiles_Insert",
        "objectType": "case profiles",
        "startTermId": "9cd80176-977f-4d4b-9073-7bfb9361afe0",
        "termSetUuid": "b52da1e5-209d-4ed6-b923-6d590072aa49"
    },
    "go_pull_term_data_departments": {
        "process": "term",
        "baseUrl": "https://ad.go.aarhuskommune.dk",
        "caseType": "emnesager",
        "storedProcedure": "GO_Departments_Insert",
        "objectType": "departments",
        "startTermId": "3239f2cb-1cac-4f10-8897-39d64127a2e2",
        "termSetUuid": "62c5a7cc-eb6f-4704-a86f-c576c4bebcca"
    },
}


def retrieve_items_for_queue() -> list[dict]:
    """Function to populate queue"""
    data = []
    references = []

    todays_date = datetime.today().date().isoformat()

    for run_type, proc_args in proces_arguments.items():
        references.append(f"{todays_date}_{run_type}")
        data.append(proc_args)

    items = [
        {"reference": ref, "data": d} for ref, d in zip(references, data, strict=True)
    ]

    return items


def create_sort_key(item: dict) -> str:
    """
    Create a sort key based on the entire JSON structure.
    Converts the item to a sorted JSON string for consistent ordering.
    """
    return json.dumps(item, sort_keys=True, ensure_ascii=False)


async def concurrent_add(workqueue: Workqueue, items: list[dict]) -> None:
    """
    Populate the workqueue with items to be processed.
    Uses concurrency and retries with exponential backoff.

    Args:
        workqueue (Workqueue): The workqueue to populate.
        items (list[dict]): List of items to add to the queue.

    Returns:
        None

    Raises:
        Exception: If adding an item fails after all retries.
    """
    sem = asyncio.Semaphore(config.MAX_CONCURRENCY)

    async def add_one(it: dict):
        reference = str(it.get("reference") or "")
        data = {"item": it}

        async with sem:
            for attempt in range(1, config.MAX_RETRIES + 1):
                try:
                    await asyncio.to_thread(workqueue.add_item, data, reference)
                    logger.info("Added item to queue with reference: %s", reference)
                    return True

                except Exception as e:
                    if attempt >= config.MAX_RETRIES:
                        logger.error(
                            "Failed to add item %s after %d attempts: %s",
                            reference,
                            attempt,
                            e,
                        )
                        return False

                    backoff = config.RETRY_BASE_DELAY * (2 ** (attempt - 1))

                    logger.warning(
                        "Error adding %s (attempt %d/%d). Retrying in %.2fs... %s",
                        reference,
                        attempt,
                        config.MAX_RETRIES,
                        backoff,
                        e,
                    )
                    await asyncio.sleep(backoff)

    if not items:
        logger.info("No new items to add.")
        return

    sorted_items = sorted(items, key=create_sort_key)
    logger.info(
        "Processing %d items sorted by complete JSON structure", len(sorted_items)
    )

    results = await asyncio.gather(*(add_one(i) for i in sorted_items))
    successes = sum(1 for r in results if r)
    failures = len(results) - successes

    logger.info(
        "Summary: %d succeeded, %d failed out of %d", successes, failures, len(results)
    )
