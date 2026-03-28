"""Core abstractions for the Gemini-backed extraction service."""

from __future__ import annotations

import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Type, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class LLMResult:
    """Result from an LLM call, carrying both text and token usage metadata."""

    text: str
    input_tokens: int = 0
    output_tokens: int = 0
    model: str = ""
    provider: str = ""
    response_time_ms: int = 0


DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 2
DEFAULT_MAX_DELAY = 30


class LLMProvider(ABC):
    """Minimal provider interface used by the active Gemini extraction flow."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def generate_from_pdf(
        self,
        pdf_data: bytes,
        prompt: str,
        max_tokens: int = 32000,
        temperature: float = 0,
    ) -> LLMResult:
        """Generate a response from PDF bytes and a text prompt."""

    @abstractmethod
    def generate_text(
        self,
        prompt: str,
        max_tokens: int = 1024,
        temperature: float = 0.7,
    ) -> LLMResult:
        """Generate a response from a text-only prompt."""

    @abstractmethod
    async def health_check(self) -> bool:
        """Return whether the provider is healthy."""

    def get_provider_name(self) -> str:
        return self.__class__.__name__

    def get_model_name(self) -> str:
        return "unknown"

    @staticmethod
    def call_with_retry(
        func: Callable[[], T],
        description: str,
        retryable_exceptions: tuple[Type[BaseException], ...] = (
            ConnectionError,
            TimeoutError,
        ),
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
    ) -> T:
        """Call a function with exponential backoff and jitter."""

        last_exception: BaseException | None = None
        for attempt in range(1, max_retries + 1):
            try:
                return func()
            except retryable_exceptions as exc:
                last_exception = exc
                if attempt == max_retries:
                    logger.error(
                        "%s: all %s retries exhausted. Last error: %s",
                        description,
                        max_retries,
                        exc,
                    )
                    raise

                delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                jitter = random.uniform(0, delay * 0.3)
                wait = delay + jitter
                logger.warning(
                    "%s: attempt %s/%s failed (%s: %s). Retrying in %.1fs...",
                    description,
                    attempt,
                    max_retries,
                    type(exc).__name__,
                    exc,
                    wait,
                )
                time.sleep(wait)
            except Exception:
                logger.exception("%s: non-retryable error", description)
                raise

        raise last_exception or RuntimeError(f"{description}: retry loop failed")


class LLMProviderError(Exception):
    """Base exception for LLM provider errors."""


class LLMProviderUnavailableError(LLMProviderError):
    """Raised when provider is unavailable."""


class LLMProviderTimeoutError(LLMProviderError):
    """Raised when provider request times out."""


class LLMProviderValidationError(LLMProviderError):
    """Raised when input validation fails."""
