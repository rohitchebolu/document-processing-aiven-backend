"""Gemini-backed document segmentation helpers."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

from .base import LLMProvider, LLMProviderError, LLMResult
from .factory import LLMProviderFactory
from .usage_tracker import usage_tracker

logger = logging.getLogger(__name__)


def run_document_formatter(pdf_path: str) -> list[dict[str, Any]]:
    """Group document pages using the classification-and-segmentation prompt."""

    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    logger.info("Processing PDF for classification prompt: pdf_path=%s", pdf_path)

    try:
        provider = LLMProviderFactory.create_provider()
    except Exception as credentials_error:
        logger.error("Failed to initialize LLM provider: %s", credentials_error)
        raise LLMProviderError(
            f"LLM provider initialization failed. Error: {credentials_error}"
        ) from credentials_error

    with open(pdf_path, "rb") as pdf_file:
        pdf_data = pdf_file.read()

    prompt = _build_classification_prompt()
    result = provider.generate_from_pdf(pdf_data, prompt, max_tokens=10000)
    usage_tracker.record(result, call_type="classification")
    response_text = result.text.strip()
    parsed_orders = _parse_pdf_formatter_response(response_text)

    if not isinstance(parsed_orders, dict) or "orders" not in parsed_orders:
        logger.warning("Unexpected response format from provider: %s", response_text[:200])
        return []

    grouped_orders: list[dict[str, Any]] = []
    for order in parsed_orders.get("orders", []):
        grouped_orders.append(
            {
                "pages": order.get("pages", []),
                "orders_per_page": order.get("orders per page", "single"),
                "Document_type": order.get("classification", "Others"),
                "Reason": order.get("reason", ""),
            }
        )

    logger.info("Successfully processed %s grouped orders from PDF", len(grouped_orders))
    return grouped_orders


def _build_classification_prompt() -> str:
    """Return the document grouping prompt used before extraction."""

    return """
            You are an order creation agent working on logistics documents.

            NOTE: Provide a brief analysis of your work, then return the JSON response.
            Keep your analysis concise to ensure complete output - focus on the final JSON structure.

                Classification Prompt: You should classify based on the below rules. Act as a logical classifier

                    classify each order only using the specified keywords and headers.
                    If an order has multiple pages, use the FIRST INCLUDED PAGE (not excluded pages) for classification.
                    IMPORTANT: When determining the first page for classification, SKIP any pages marked for exclusion (instructions, terms, disclaimers). Use the first page that contains actual order data.
                    If the header of document contains keywords like "DELIVERY RECEIPT","Delivery Manifest","Delivery Alert", "Delivery Order", "Alert","Recovery Alert","Alert Manifest","Delivery Request" , "SHIPPING PRE ALERT", "IMMEDIATE DELIVERY" then classify as "Delivery Order".
                    If the document related to PILOT and if it has "SHIPPER'S RECEIPT"/"Shipper Receipt"/"DELIVERY RECEIPT" at the bottom classify the document as "Delivery Order".
                    If the header of document contains keywords like "Pickup Alert", "PICK UP", "Pickup Order", "Pickup Request", "Recovery Order" then classify as "Pickup Order".
                    If the header of the document contains the keyword 'BOL','Bill of Lading', 'Straight Bill of Lading' classify it as BOL.
                    Sometimes there maybe a label "Bill of lading #" in some documents but those cannot be classified as BOL all the time.
                    If the "BOL" or "Bill of lading" is in header then only it will be considered as BOL.
                    If any of the above mentioned keywords or conditions related to 'delivery', 'pickup', 'bol' are not found and satisfied, then classify the document as "Others".
                    If document contains these phrases "Transport Document" or "Shipment Cartage Advice with Receipt" in its header, classify as "Others".
                    If any of the above given conditions are not satisfied it should be "Others"
                    If any of the document contains "Pickup/Delivery" then classify the order as "Others".
                    If document contains "Transport Document" classify as "Others".
                    If you are not able to classify the document based on the given information, then classify as "Others".
                    Give the Classification result with key name "Document Classification Result" only in a single string line.


                multi-order in each page:
                    Your first task is to determine whether any single page contains more than one DISTINCT ORDER (not line items).

                    KEY DISTINCTION:
                    ✓ LINE ITEMS = Multiple items within ONE order (same DELIVERY TICKET NUMBER) = "single"
                    ✗ DIFFERENT ORDERS = Different LOAD NUMBERs or different primary orders = "multi"

                    MANDATORY TOP PRIORITY RULE - VISUAL COMFORT & CO :
                    If the document header contains 'Visual Comfort & Co' AND all pages consistently use 'DELIVERY TICKET' as the identifier:
                    → AUTOMATICALLY classify as "single" orders per page (for ALL pages)
                    → Do NOT apply other algorithm steps - this overrides all other rules
                    → This applies regardless of different LOT NOs, different line items, or multiple blocks
                    → All pages with 'Visual Comfort & Co' header + 'DELIVERY TICKET' identifier = ONE order, "single" orders per page

                    IMPORTANT ALGORITHM - Follow these steps STRICTLY in order:

                    Step 0: FIRST - EXTRACT LOAD NUMBER (PRIMARY IDENTIFIER)
                    - Scan the page for LOAD NUMBER, LOAD #, Card #, Card No, or similar MASTER identifier
                    - ALL blocks/items on this page that share the SAME LOAD NUMBER belong to ONE order
                    - If all blocks on page have SAME LOAD NUMBER → Answer is "single" (regardless of LOT NOs)
                    - Proceed to Step 1 only if you find DIFFERENT LOAD NUMBERs on the same page

                    Step 1: IDENTIFY ALL BLOCKS/SECTIONS on the page
                    - Look for visually separated sections (horizontal lines, spacing, different formatting)
                    - Each section typically contains: Header with reference numbers → Details → Footer
                    - Count the total number of distinct blocks you identify
                    - FOR EACH BLOCK, identify its LOAD NUMBER first

                    Step 2: EXTRACT REFERENCE NUMBERS AND DETAILS from EACH block
                    For each block, extract ALL of these fields:
                        MASTER identifiers (check these FIRST):
                        - LOAD NUMBER / LOAD # / Card # / Card No (PRIMARY - groups all items together)

                        Secondary identifiers:
                        - HAWB / HWB / House Waybill / Shipment Number
                        - Invoice Number
                        - Reference Number / I.T NO / BOL Number

                        Order details (critical for validation):
                        - Consignee/Receiver Name
                        - Consignee/Receiver Address/Location
                        - Invoice Amount/Total Amount
                        - Weight
                        - Number of pieces/units
                        - Commodity/Description

                    Step 3: CHECK LOAD NUMBERS ACROSS ALL BLOCKS ON PAGE
                    - Do all blocks share the SAME LOAD NUMBER?
                    - YES → Answer: "single" (all line items belong to ONE order(shares common DELIVERY TICKET NUMBER/ LOAD NUMBER)) - STOP HERE
                    - NO → Multiple different LOAD NUMBERs detected → Continue to Step 4

                    Step 4: VERIFY COPIES (applies only if reference numbers are SAME)
                    If reference numbers are identical, check for copy indicators:
                    - Look for labels: "Shipper's Copy", "Driver Copy", "Consignee Copy", "Top portion", "Bottom portion", "Customer Copy"
                    - Check if consignee/receiver details are identical
                    - Check if invoice amounts are identical
                    If copy labels present OR all details are identical → ANSWER: "single" (these are copies)

                    Step 5: VALIDATE MULTI-ORDER (if reference numbers differ)
                    If you found different reference numbers:
                    - Verify that each block also has DIFFERENT Consignee names/addresses
                    - Verify that each block has DIFFERENT Invoice amounts (if amounts exist)
                    - If reference numbers differ AND consignees/amounts differ → CONFIRMED "multi"
                    - If reference numbers differ BUT other details seem copied → Inspect more carefully, may still be "single"

                    CRITICAL DISTINCTION:
                    ✓ COPIES (= "single"): Same HAWB + Same Invoice + Same Consignee + Same Amount
                                        Usually with "Copy" label

                    ✓ MULTI-ORDERS (= "multi"): Different HAWB OR Different Invoice OR Different Consignee AND Different Amount
                                                Each order is independent with different details

                    FINAL DECISION CHECKLIST:
                    Answer "single" IF:
                    ✓ All blocks have same HAWB AND same Invoice AND same Consignee AND same Amount
                    ✓ OR copy labels confirm these are duplicates
                    ✓ IF all pages belong to the same order and each page contains multiple line items, the "orders per page" for all pages should be "Single".

                    Answer "multi" IF:
                    ✓ Any block has DIFFERENT HAWB OR DIFFERENT Invoice, AND different consignee/amount
                    ✓ OR blocks show multiple distinct delivery locations/consignees
                    ✓ OR invoice amounts vary between blocks (multi-stop manifest)

                    Give the result with key name "orders per page" only in a single string line: either "single" or "multi".

                Order segregation:
                        Task: Analyze the attached multi-page logistics PDF and accurately group pages into complete orders with no omissions or overlaps.

                        Instructions:
                        Goal:
                        Group all pages that belong to the same order by comparing ACTUAL VALUES (not field names/keys). Pages are the same order if they share identical reference values, regardless of what the field is called.

                        CRITICAL INSTRUCTION - Value-Based Matching (MANDATORY):
                        IGNORE FIELD NAMES/KEYS - COMPARE ONLY VALUES
                        1. For EACH page, extract ALL alphanumeric values associated with identifier fields (even if you don't recognize the field name)
                        2. Normalize each value: trim whitespace, convert to standard format
                        3. Compare the ACTUAL VALUES across ALL pages
                        4. If ANY value matches between pages → THOSE PAGES BELONG TO THE SAME ORDER
                        5. Do NOT reject a match just because the field names are different

                        PRIMARY GROUPING RULES (HIGHEST PRIORITY):
                        ✓ LOAD NUMBER is the PRIMARY grouping identifier - pages with same LOAD NUMBER ALWAYS belong to same order
                        ✓ Do NOT segregate pages just because they have different LOT NUMBERS - LOT numbers are line items within a load
                        ✓ Multiple LOT NUMBERS on same page/load = ONE order (NOT multiple orders)

                        REQUIRED OUTPUT FORMAT:
                        {
                          "orders": [
                            {
                              "orders per page": "single",
                              "pages": [1, 2, 3],
                              "classification": "Document Classification Result",
                              "reason": "Give a valid reason for the classification"
                            }
                          ]
                        }
        """


def _parse_pdf_formatter_response(text: str) -> dict[str, Any]:
    """Parse Gemini response from the document grouping prompt."""

    if not text or not isinstance(text, str):
        logger.debug("Empty or invalid response text")
        return {}

    cleaned_text = text.strip().replace("```json", "").replace("```", "").strip()
    json_str = _extract_json_by_braces(cleaned_text)
    if not json_str:
        logger.debug("Could not extract JSON from response")
        return {}

    try:
        return json.loads(json_str)
    except json.JSONDecodeError as exc:
        logger.error("JSON decode error: %s", exc)
        fixed_json = _attempt_fix_json(json_str)
        try:
            return json.loads(fixed_json)
        except json.JSONDecodeError:
            logger.error("Could not parse JSON even after fixing")
            return {}


def _extract_json_by_braces(text: str) -> str:
    """Extract a complete JSON object by brace matching."""

    start_idx = text.find("{")
    if start_idx == -1:
        return ""

    brace_count = 0
    end_idx = -1
    in_string = False
    escape_next = False

    for idx in range(start_idx, len(text)):
        char = text[idx]

        if escape_next:
            escape_next = False
            continue

        if char == "\\":
            escape_next = True
            continue

        if char == '"' and not escape_next:
            in_string = not in_string

        if not in_string:
            if char == "{":
                brace_count += 1
            elif char == "}":
                brace_count -= 1
                if brace_count == 0:
                    end_idx = idx + 1
                    break

    return "" if end_idx == -1 else text[start_idx:end_idx]


def _attempt_fix_json(json_str: str) -> str:
    """Attempt to fix common JSON formatting issues."""

    fixed = json_str.split("|")[0].strip()
    fixed = re.sub(r",(\s*[}\]])", r"\1", fixed)

    open_braces = fixed.count("{")
    close_braces = fixed.count("}")
    if open_braces > close_braces:
        fixed += "}" * (open_braces - close_braces)

    return fixed


__all__ = [
    "LLMProvider",
    "LLMProviderError",
    "LLMResult",
    "LLMProviderFactory",
    "run_document_formatter",
]
