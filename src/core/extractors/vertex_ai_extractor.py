"""
Vertex AI Extraction Orchestrator
Orchestrates the extraction workflow for PDFs using the configured LLM provider.
Handles both single-pass and parallel page-by-page extraction.
"""

import json
import logging
import concurrent.futures
from typing import Dict, List, Tuple

from src.core.extractors.vertex_ai_agent import VertexAIAnalysisAgent
from src.core.processing.pdf_utils import (
    get_pdf_page_count,
    split_pdf_to_pages,
    cleanup_temp_files,
    merge_extraction_results,
    parse_json_output,
)
from src.services.llm.factory import LLMProviderFactory

# Configure logging
logger = logging.getLogger(__name__)

# Configuration constants
PAGE_BY_PAGE_THRESHOLD = 5
MAX_PARALLEL_WORKERS = 5


def extract_page_items(
    agent: VertexAIAnalysisAgent, page_path: str, page_num: int, is_first: bool
) -> Dict:
    """
    Extract items from a single page.

    Args:
        agent (VertexAIAnalysisAgent): Analysis agent instance
        page_path (str): Path to the page PDF
        page_num (int): Page number
        is_first (bool): Whether this is the first page

    Returns:
        Dict: Extracted items data
    """
    raw_output = agent.generate_response_for_page(
        page_path, page_num, is_first_page=is_first
    )
    return parse_json_output(raw_output)


def _extract_parallel_pages(
    agent: VertexAIAnalysisAgent,
    page_paths: List[str],
    page_count: int,
) -> Tuple[List[Dict], Dict]:
    """
    Process remaining pages in parallel after first page.

    Args:
        agent (VertexAIAnalysisAgent): Analysis agent
        page_paths (List[str]): List of all page file paths
        page_count (int): Total number of pages

    Returns:
        Tuple[List[Dict], Dict]: Aggregated results and base result
    """
    all_results = []

    # Step 1: Process first page to get base structure (must be sequential)
    logger.info(f"Processing page 1/{page_count} (base structure)")
    base_result = extract_page_items(agent, page_paths[0], 1, is_first=True)
    base_items = base_result.get("items_details", [])
    logger.info(f"Page 1: extracted {len(base_items)} items")
    all_results.append(base_result)

    # Step 2: Process remaining pages in parallel
    if len(page_paths) > 1:
        remaining_pages = [(page_paths[i], i + 1) for i in range(1, len(page_paths))]
        logger.info(
            f"Processing pages 2-{page_count} in parallel (max {MAX_PARALLEL_WORKERS} workers)"
        )

        def process_page(args):
            page_path, page_num = args
            result = extract_page_items(agent, page_path, page_num, is_first=False)
            items = result.get("items_details", [])
            return (page_num, result, len(items))

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_PARALLEL_WORKERS
        ) as executor:
            futures = {
                executor.submit(process_page, args): args for args in remaining_pages
            }

            for future in concurrent.futures.as_completed(futures):
                page_num, result, items_count = future.result()
                is_last = page_num == page_count

                # Skip last page if it has no items (likely signature page)
                if is_last and items_count == 0:
                    logger.info(
                        f"Page {page_num} has no items (likely signature page), skipping"
                    )
                    continue

                all_results.append(result)
                logger.info(f"Page {page_num}: extracted {items_count} items")

    return all_results, base_result


def _extract_single_pass(
    schema: Dict,
    system_prompt: str,
    pdf_path: str,
    orders_per_page: str,
) -> str:
    """
    Execute single-pass extraction — direct LLM API call.

    Returns:
        str: Cleaned JSON string from model output
    """
    logger.info(
        "Using single-pass extraction: orders_per_page=%s pdf_path=%s",
        orders_per_page,
        pdf_path,
    )
    logger.info("Creating LLM provider for single-pass extraction")
    provider = LLMProviderFactory.create_provider()
    agent = VertexAIAnalysisAgent(provider, system_prompt, schema)
    logger.info("Starting single-pass Gemini extraction: pdf_path=%s", pdf_path)
    pdf_output_data = agent.generate_response(pdf_path)
    logger.info("Single-pass Gemini extraction completed: pdf_path=%s", pdf_path)

    # Clean JSON from model output
    if orders_per_page == "multi":
        opening_index = pdf_output_data.find("[")
        if opening_index != -1:
            pdf_output_data = pdf_output_data[opening_index:].replace("```", "")
        else:
            logger.warning("Opening brace '[' not found in the JSON data.")
    else:
        opening_index = pdf_output_data.find("{")
        if opening_index != -1:
            pdf_output_data = pdf_output_data[opening_index:].replace("```", "")
        else:
            logger.warning("Opening brace '{' not found in the JSON data.")

    return pdf_output_data


def _extract_page_by_page(
    schema: Dict, system_prompt: str, pdf_path: str
) -> str:
    """
    Execute parallel page-by-page extraction workflow.

    Returns:
        str: JSON string of merged extraction result
    """
    logger.info(
        "Using parallel page-by-page extraction: pdf_path=%s",
        pdf_path,
    )
    logger.info("Creating LLM provider for page-by-page extraction")
    provider = LLMProviderFactory.create_provider()
    agent = VertexAIAnalysisAgent(provider, system_prompt, schema)
    page_paths = split_pdf_to_pages(pdf_path)
    page_count = len(page_paths)
    logger.info("Split PDF into pages for parallel extraction: page_count=%s", page_count)

    try:
        all_results, base_result = _extract_parallel_pages(agent, page_paths, page_count)

        # Merge all results
        if base_result:
            final_result = merge_extraction_results(all_results, base_result)
            total_items = len(final_result.get("items_details", []))
            logger.info(f"Total items extracted: {total_items}")
            return json.dumps(final_result)
        else:
            logger.error("No base result from first page")
            return "{}"
    finally:
        logger.info("Cleaning up temporary page files: count=%s", len(page_paths))
        cleanup_temp_files(page_paths)


def extract_document(
    schema: Dict,
    system_prompt: str,
    pdf_path: str,
    orders_per_page: str = "single",
) -> str:
    """
    Extract data from a PDF document using the configured LLM provider.

    Supports both single-pass and parallel page-by-page extraction.
    Page-by-page extraction is used for documents with more than
    PAGE_BY_PAGE_THRESHOLD pages (currently enabled for VC organization only).

    Args:
        schema (Dict): JSON schema for output validation
        system_prompt (str): System prompt for extraction
        pdf_path (str): Path to the PDF to process
        orders_per_page (str): "single" or "multi" processing mode

    Returns:
        str: JSON string containing extracted data

    Raises:
        FileNotFoundError: If PDF file not found
        Exception: If extraction fails
    """
    page_count = get_pdf_page_count(pdf_path)
    logger.info(
        "extract_document started: pdf_path=%s orders_per_page=%s page_count=%s",
        pdf_path,
        orders_per_page,
        page_count,
    )

    # Determine extraction strategy
    use_page_by_page = (
        page_count > PAGE_BY_PAGE_THRESHOLD
        and orders_per_page != "multi"
    )

    if use_page_by_page:
        logger.info("Selected extraction strategy: page-by-page")
        return _extract_page_by_page(schema, system_prompt, pdf_path)
    else:
        logger.info("Selected extraction strategy: single-pass")
        return _extract_single_pass(
            schema, system_prompt, pdf_path, orders_per_page
        )
