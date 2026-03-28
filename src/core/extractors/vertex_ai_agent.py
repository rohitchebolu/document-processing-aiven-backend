"""
Vertex AI Analysis Agent
Handles document analysis and extraction using the configured LLM provider.
"""

import json
import logging
from typing import Dict

from src.services.llm.base import LLMProvider
from src.services.llm.usage_tracker import usage_tracker

# Configure logging
logger = logging.getLogger(__name__)


class VertexAIAnalysisAgent:
    """Base agent for LLM-powered document analysis."""

    def __init__(
        self,
        provider: LLMProvider,
        system_prompt: str,
        schema: Dict,
    ):
        self.provider = provider
        self.system_prompt = system_prompt
        self.schema = schema

    def generate_response(self, pdf_path: str) -> str:
        """
        Generate extraction response for a complete PDF.
        Retry with backoff is handled by the provider.

        Raises:
            Exception: On non-retryable errors or after all retries exhausted.
        """
        with open(pdf_path, "rb") as f:
            pdf_data = f.read()
        logger.info(
            "Submitting full PDF to Gemini: pdf_path=%s size_bytes=%s",
            pdf_path,
            len(pdf_data),
        )

        prompt_text = (
            f"prompt: {self.system_prompt}\n\n"
            f"Follow this schema: {json.dumps(self.schema)}"
        )

        result = self.provider.generate_from_pdf(
            pdf_data, prompt_text, max_tokens=32000
        )
        logger.info(
            "Gemini full PDF response received: pdf_path=%s input_tokens=%s output_tokens=%s response_time_ms=%s",
            pdf_path,
            result.input_tokens,
            result.output_tokens,
            result.response_time_ms,
        )
        usage_tracker.record(result, call_type="extraction")
        return result.text

    def generate_response_for_page(
        self, pdf_path: str, page_num: int, is_first_page: bool = False
    ) -> str:
        """
        Extract data from a single page.
        Retry with backoff is handled by the provider.

        Raises:
            Exception: On non-retryable errors or after all retries exhausted.
        """
        with open(pdf_path, "rb") as f:
            pdf_data = f.read()
        logger.info(
            "Submitting page to Gemini: pdf_path=%s page_num=%s is_first_page=%s size_bytes=%s",
            pdf_path,
            page_num,
            is_first_page,
            len(pdf_data),
        )

        if is_first_page:
            prompt_text = (
                f"prompt: {self.system_prompt}\n\n"
                f"Follow this schema: {json.dumps(self.schema)}"
            )
        else:
            items_schema = self.schema.get("properties", {}).get("items_details", {})

            prompt_text = f"""Extract ALL items/products from this page (Page {page_num}).

This is a CONTINUATION page - you must extract EVERY single item/line visible on this page.

Required Schema for items_details:
{json.dumps(items_schema)}

CRITICAL INSTRUCTIONS:
1. Find EVERY item row on this page
2. Extract item fields matching the schema exactly
3. Do NOT skip any items
4. Do NOT add fields not in schema
5. Return only valid JSON

Return format:
{{"items_details": [...]}}"""

        result = self.provider.generate_from_pdf(
            pdf_data, prompt_text, max_tokens=16000
        )
        logger.info(
            "Gemini page response received: pdf_path=%s page_num=%s input_tokens=%s output_tokens=%s response_time_ms=%s",
            pdf_path,
            page_num,
            result.input_tokens,
            result.output_tokens,
            result.response_time_ms,
        )
        usage_tracker.record(result, call_type="page_extraction")
        return result.text
