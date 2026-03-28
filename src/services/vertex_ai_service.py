"""
Vertex AI Service Module
Thin orchestration layer for PDF document processing using Vertex AI.
Delegates core extraction logic to specialized modules in src/core.
"""

import json
import logging

from src.core.extractors.vertex_ai_extractor import extract_document

# Configure logging
logger = logging.getLogger(__name__)


# ============================================================================
# Public API
# ============================================================================


def processor(details_dict: dict) -> str:
    """
    Process a PDF document using Vertex AI generative models.

    This is a thin orchestration layer that:
    1. Loads the extraction schema
    2. Delegates extraction to the core extraction module

    Supports both single-pass and parallel page-by-page extraction.
    Page-by-page extraction is used for documents with more than
    5 pages (currently enabled for VC organization only).

    Args:
        details_dict (dict): Processing configuration containing:
            - filename: Path to the JSON schema file
            - pdf_path: Path to the PDF to process
            - prompt: System prompt for extraction
            - orders_per_page: "single" or "multi" processing mode

    Returns:
        str: JSON string containing extracted data

    Raises:
        FileNotFoundError: If schema or PDF file not found
        json.JSONDecodeError: If schema file is invalid JSON
        Exception: If extraction fails
    """
    try:
        logger.debug("Started PDF extraction process")

        # Load the schema
        with open(details_dict["filename"], "r") as f:
            schema = json.load(f)

        orders_per_page = details_dict.get("orders_per_page", "single")

        # Delegate to core extraction module
        output_data = extract_document(
            schema=schema,
            system_prompt=details_dict["prompt"],
            pdf_path=details_dict["pdf_path"],
            orders_per_page=orders_per_page,
        )

        logger.debug("PDF extraction completed successfully")
        return output_data

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in schema: {e}")
        raise
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise
