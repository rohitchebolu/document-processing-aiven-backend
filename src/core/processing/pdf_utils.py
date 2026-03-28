"""
PDF Processing Utilities
Handles PDF manipulation operations like splitting, merging, and page counting.
"""

import json
import logging
import tempfile
from typing import Dict, List

from PyPDF2 import PdfReader, PdfWriter

# Configure logging
logger = logging.getLogger(__name__)


def get_pdf_page_count(pdf_path: str) -> int:
    """
    Get the number of pages in a PDF.

    Args:
        pdf_path (str): Path to the PDF file

    Returns:
        int: Number of pages in the PDF

    Raises:
        FileNotFoundError: If PDF file does not exist
        Exception: If PDF cannot be read
    """
    reader = PdfReader(pdf_path)
    return len(reader.pages)


def split_pdf_to_pages(pdf_path: str) -> List[str]:
    """
    Split a PDF into individual page files.

    Args:
        pdf_path (str): Path to the PDF file

    Returns:
        List[str]: List of temporary file paths for each page

    Raises:
        FileNotFoundError: If PDF file does not exist
        Exception: If PDF cannot be split
    """
    reader = PdfReader(pdf_path)
    page_paths = []

    for i, page in enumerate(reader.pages):
        writer = PdfWriter()
        writer.add_page(page)

        # Create temp file for this page
        temp_file = tempfile.NamedTemporaryFile(
            delete=False, suffix=f"_page_{i+1}.pdf"
        )
        with open(temp_file.name, "wb") as f:
            writer.write(f)
        page_paths.append(temp_file.name)

    return page_paths


def cleanup_temp_files(file_paths: List[str]) -> None:
    """
    Clean up temporary PDF files.

    Args:
        file_paths (List[str]): List of file paths to remove
    """
    for path in file_paths:
        try:
            import os
            os.remove(path)
        except Exception as e:
            logger.warning(f"Failed to cleanup temp file {path}: {e}")


def merge_extraction_results(results: List[Dict], base_result: Dict) -> Dict:
    """
    Merge items_details from multiple page extractions into base result.

    Args:
        results (List[Dict]): List of extraction results from all pages
        base_result (Dict): Base result structure from first page

    Returns:
        Dict: Merged result with all items combined
    """
    all_items = []

    for result in results:
        if result and isinstance(result, dict):
            items = result.get("items_details", [])
            if items:
                all_items.extend(items)

    # Use base_result structure but with merged items
    base_result["items_details"] = all_items
    return base_result


def parse_json_output(raw_output: str, expect_array: bool = False) -> Dict:
    """
    Parse JSON from model output, handling common formatting issues.

    Args:
        raw_output (str): Raw text output from the model
        expect_array (bool): Whether to expect a JSON array or object

    Returns:
        Dict: Parsed JSON data or empty dict if parsing fails
    """
    try:
        # Clean up the output
        cleaned = raw_output.replace("```json", "").replace("```", "").strip()

        if expect_array:
            idx = cleaned.find("[")
            if idx != -1:
                cleaned = cleaned[idx:]
        else:
            idx = cleaned.find("{")
            if idx != -1:
                cleaned = cleaned[idx:]

        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.warning(f"JSON parse error: {e}")
        return {}
