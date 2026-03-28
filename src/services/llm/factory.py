"""Gemini-only LLM provider factory."""

import logging

from src.services.llm.base import LLMProvider, LLMProviderError
from src.services.llm.gemini_provider import GeminiProvider

logger = logging.getLogger(__name__)


class LLMProviderFactory:
    """Factory for the single supported Gemini provider."""

    _provider_cache: dict[str, LLMProvider] = {}

    @classmethod
    def create_provider(
        cls,
        provider_type: str | None = None,
        use_cache: bool = True,
    ) -> LLMProvider:
        """Create or retrieve the Gemini provider instance."""
        resolved_provider = (provider_type or "gemini").lower()
        if resolved_provider != "gemini":
            raise LLMProviderError(
                f"Unsupported provider '{resolved_provider}'. This service only supports 'gemini'."
            )

        if use_cache and resolved_provider in cls._provider_cache:
            logger.debug("Using cached provider: %s", resolved_provider)
            return cls._provider_cache[resolved_provider]

        try:
            provider = GeminiProvider()
            if use_cache:
                cls._provider_cache[resolved_provider] = provider
            logger.info("Created gemini provider: %s", provider.get_model_name())
            return provider
        except ImportError as e:
            raise LLMProviderError(
                f"Provider '{resolved_provider}' SDK not installed: {e}. "
                f"Install the required package to use this provider."
            ) from e
        except Exception as e:
            logger.error("Failed to create gemini provider: %s", e)
            raise LLMProviderError(f"Failed to initialize provider: {e}") from e

    @classmethod
    def list_registered_providers(cls) -> list[str]:
        """Get list of supported provider names."""
        return ["gemini"]

    @classmethod
    def clear_cache(cls, provider_type: str | None = None) -> None:
        """Clear provider cache."""
        if provider_type:
            cls._provider_cache.pop(provider_type.lower(), None)
            logger.debug("Cleared cache for %s", provider_type)
        else:
            cls._provider_cache.clear()
            logger.debug("Cleared all provider cache")
