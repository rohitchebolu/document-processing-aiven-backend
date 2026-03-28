"""Gemini provider implementation for the active extraction flow."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Dict

from src.config.secrets import get_secrets_config
from src.services.llm.base import (
    LLMProvider,
    LLMProviderError,
    LLMResult,
)

logger = logging.getLogger(__name__)

_RETRYABLE_EXCEPTIONS = None


def _get_retryable_exceptions():
    """Lazy-load retryable exceptions to avoid import at module level."""

    global _RETRYABLE_EXCEPTIONS
    if _RETRYABLE_EXCEPTIONS is None:
        from google.api_core.exceptions import (
            DeadlineExceeded,
            InternalServerError,
            ResourceExhausted,
            ServiceUnavailable,
        )

        _RETRYABLE_EXCEPTIONS = (
            ResourceExhausted,
            ServiceUnavailable,
            DeadlineExceeded,
            InternalServerError,
            ConnectionError,
            TimeoutError,
        )
    return _RETRYABLE_EXCEPTIONS


class GeminiProvider(LLMProvider):
    """Gemini provider used for both document segmentation and extraction."""

    def __init__(self):
        super().__init__()
        self.secrets = get_secrets_config()

        project_id = self._setup_credentials()

        import vertexai
        from vertexai.generative_models import (
            GenerativeModel,
            HarmBlockThreshold,
            HarmCategory,
            SafetySetting,
        )

        if not project_id:
            raise LLMProviderError(
                "VERTEX_AI_SERVICE_ACCOUNT_FILE must contain a valid project_id."
            )

        vertexai.init(
            project=project_id,
            location=self.secrets.VERTEX_AI_LOCATION,
        )

        self.model_name = self.secrets.VERTEX_AI_MODEL
        self.model = GenerativeModel(self.model_name)
        self.safety_settings = [
            SafetySetting(
                category=HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                threshold=HarmBlockThreshold.OFF,
            ),
            SafetySetting(
                category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                threshold=HarmBlockThreshold.OFF,
            ),
            SafetySetting(
                category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                threshold=HarmBlockThreshold.OFF,
            ),
            SafetySetting(
                category=HarmCategory.HARM_CATEGORY_HARASSMENT,
                threshold=HarmBlockThreshold.OFF,
            ),
        ]
        self._base_generation_config = {
            "temperature": 0,
            "top_p": 0.8,
            "top_k": 40,
        }

        logger.info("Initialized Gemini provider with model: %s", self.model_name)

    def _setup_credentials(self) -> str:
        """Set up Vertex credentials from the configured service account file."""

        credentials_path = self.secrets.VERTEX_AI_SERVICE_ACCOUNT_FILE
        if not credentials_path:
            raise LLMProviderError(
                "VERTEX_AI_SERVICE_ACCOUNT_FILE is required for Gemini access."
            )
        if not os.path.exists(credentials_path):
            raise LLMProviderError(
                f"Vertex AI service account file not found: {credentials_path}"
            )

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        logger.debug("Using Vertex AI credentials from: %s", credentials_path)

        try:
            with open(credentials_path, "r", encoding="utf-8") as credentials_file:
                credentials = json.load(credentials_file)
        except (OSError, json.JSONDecodeError) as exc:
            raise LLMProviderError(
                f"Could not read Vertex AI service account file: {credentials_path}"
            ) from exc

        project_id = credentials.get("project_id")
        if not project_id:
            raise LLMProviderError(
                "The Vertex AI service account file does not include project_id."
            )

        return str(project_id)

    def _get_generation_config(
        self,
        max_tokens: int = 32000,
        temperature: float = 0,
        include_stop_sequences: bool = True,
    ) -> Dict:
        """Build a generation config dict."""

        config = {
            **self._base_generation_config,
            "temperature": temperature,
            "max_output_tokens": max_tokens,
        }
        if include_stop_sequences:
            config["stop_sequences"] = ["\n\n###", "\n\nNote:", "\n\nInstruction:"]
        return config

    def generate_from_pdf(
        self,
        pdf_data: bytes,
        prompt: str,
        max_tokens: int = 32000,
        temperature: float = 0,
    ) -> LLMResult:
        """Generate a text response from PDF bytes plus prompt using Gemini."""

        from vertexai.generative_models import Part

        prompt_part = Part.from_text(prompt)
        pdf_part = Part.from_data(mime_type="application/pdf", data=pdf_data)
        gen_config = self._get_generation_config(
            max_tokens=max_tokens,
            temperature=temperature,
        )

        def _call():
            started_at = time.time()
            response = self.model.generate_content(
                [prompt_part, pdf_part],
                generation_config=gen_config,
                safety_settings=self.safety_settings,
            )
            elapsed_ms = int((time.time() - started_at) * 1000)
            usage = getattr(response, "usage_metadata", None)
            return LLMResult(
                text=response.text,
                input_tokens=getattr(usage, "prompt_token_count", 0) or 0,
                output_tokens=getattr(usage, "candidates_token_count", 0) or 0,
                model=self.model_name,
                provider="gemini",
                response_time_ms=elapsed_ms,
            )

        return self.call_with_retry(
            _call,
            description=f"Gemini generate_from_pdf (tokens={max_tokens})",
            retryable_exceptions=_get_retryable_exceptions(),
        )

    def generate_text(
        self,
        prompt: str,
        max_tokens: int = 1024,
        temperature: float = 0.7,
    ) -> LLMResult:
        """Generate a text response from a text prompt using Gemini."""

        config = {
            **self._base_generation_config,
            "temperature": temperature,
            "max_output_tokens": max_tokens,
        }

        def _call():
            started_at = time.time()
            response = self.model.generate_content(
                prompt,
                generation_config=config,
                safety_settings=self.safety_settings,
            )
            elapsed_ms = int((time.time() - started_at) * 1000)
            usage = getattr(response, "usage_metadata", None)
            return LLMResult(
                text=response.text,
                input_tokens=getattr(usage, "prompt_token_count", 0) or 0,
                output_tokens=getattr(usage, "candidates_token_count", 0) or 0,
                model=self.model_name,
                provider="gemini",
                response_time_ms=elapsed_ms,
            )

        return self.call_with_retry(
            _call,
            description="Gemini generate_text",
            retryable_exceptions=_get_retryable_exceptions(),
        )

    async def health_check(self) -> bool:
        """Check if Gemini provider is healthy."""

        try:
            loop = asyncio.get_event_loop()
            await asyncio.wait_for(
                loop.run_in_executor(None, self._test_gemini_connection),
                timeout=10,
            )
            logger.debug("Gemini health check passed")
            return True
        except Exception as exc:
            logger.warning("Gemini health check failed: %s", exc)
            return False

    def get_model_name(self) -> str:
        return self.model_name

    def _test_gemini_connection(self) -> bool:
        response = self.model.generate_content("test")
        return response is not None
