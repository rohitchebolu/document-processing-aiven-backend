"""Document extraction pipeline for grouped PDF orders."""

from __future__ import annotations

import concurrent.futures
import contextvars
import json
import logging
import os
import uuid

from src.core.processing.prompts import resolve_prompt_template
from src.core.processing.splitter import split_pdf_by_orders
from src.services.llm import run_document_formatter as formatter
from src.services.vertex_ai_service import processor

logger = logging.getLogger(__name__)

SCHEMA_PATH = "data/schemas/schema.json"


def pdf_extractor(filename: str, pdf_path: str) -> list[dict]:
    """Group the uploaded PDF and extract structured data for each group."""

    logger.info(
        "pdf_extractor started: filename=%s pdf_path=%s",
        filename,
        pdf_path,
    )

    try:
        logger.info("Running document formatter for page grouping: filename=%s", filename)
        grouped_documents = formatter(pdf_path)
        logger.info(
            "Document grouping completed: filename=%s grouped_orders=%s payload=%s",
            filename,
            len(grouped_documents),
            grouped_documents,
        )

        pages_list = [group["pages"] for group in grouped_documents]
        logger.info("Grouped page ranges: filename=%s pages_list=%s", filename, pages_list)

        output_files = split_pdf_by_orders(pdf_path, pages_list)
        for group, path in zip(grouped_documents, output_files):
            group["pdf_path"] = path
            logger.info(
                "Prepared split PDF for grouped order: filename=%s pages=%s split_path=%s",
                filename,
                group.get("pages"),
                path,
            )
    except Exception as exc:
        logger.exception("Order grouping failed: filename=%s", filename)
        return [{"status": "Failed", "reason": str(exc)}]

    def process_single_group(group: dict) -> dict | list[dict]:
        order_pages = group.get("pages")
        logger.info(
            "Starting grouped order extraction: filename=%s pages=%s document_type=%s",
            filename,
            order_pages,
            group.get("Document_type", "-"),
        )

        try:
            prompt_name, prompt_template = resolve_prompt_template(
                orders_per_page=group["orders_per_page"],
            )
            payload = {
                "filename": SCHEMA_PATH,
                "pdf_path": group["pdf_path"],
                "prompt": prompt_template,
                "orders_per_page": group["orders_per_page"],
            }
            logger.info(
                "Prompt resolved for grouped order: filename=%s pages=%s prompt_name=%s",
                filename,
                order_pages,
                prompt_name,
            )

            logger.info(
                "Calling processor for grouped order: filename=%s pages=%s",
                filename,
                order_pages,
            )
            results = processor(payload)
            logger.info(
                "Processor completed for grouped order: filename=%s pages=%s",
                filename,
                order_pages,
            )

            parsed_results = json.loads(results)
            extracted_documents = (
                parsed_results
                if group["orders_per_page"] == "multi"
                else [parsed_results]
            )

            assembled_results: list[dict] = []
            for extracted_document in extracted_documents:
                document = dict(extracted_document)
                document["source_document_type"] = group.get("Document_type")
                document["source_classification_reason"] = group.get("Reason")
                document["filename"] = (
                    str("-".join(filename.split("-")[0:-1])) + ".pdf"
                    if "-" in filename
                    else filename
                )
                document["uniqueId"] = str(uuid.uuid4())
                if group.get("pdf_path"):
                    document["pdf_path"] = os.path.abspath(group["pdf_path"])
                document["_prompt_name"] = prompt_name
                document["_prompt_template"] = prompt_template
                assembled_results.append(document)
                logger.info(
                    "Grouped order extraction assembled: filename=%s pages=%s document_type=%s",
                    filename,
                    order_pages,
                    group.get("Document_type"),
                )

            if group["orders_per_page"] == "multi":
                logger.info(
                    "Returning multi extracted order set: filename=%s pages=%s count=%s",
                    filename,
                    order_pages,
                    len(assembled_results),
                )
                return assembled_results

            logger.info(
                "Returning single extracted order: filename=%s pages=%s",
                filename,
                order_pages,
            )
            return assembled_results[0]
        except Exception:
            logger.exception(
                "Grouped order extraction failed: filename=%s pages=%s",
                filename,
                order_pages,
            )
            raise

    results: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        logger.info(
            "Submitting grouped orders for concurrent extraction: filename=%s grouped_orders=%s",
            filename,
            len(grouped_documents),
        )
        futures = [
            executor.submit(contextvars.copy_context().run, process_single_group, group)
            for group in grouped_documents
        ]

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            logger.info(
                "One grouped order finished: filename=%s completed_groups=%s total_groups=%s",
                filename,
                len(results),
                len(grouped_documents),
            )

    logger.info("pdf_extractor completed: filename=%s total_results=%s", filename, len(results))
    return results
