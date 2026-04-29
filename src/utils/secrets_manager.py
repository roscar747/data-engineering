"""Pluggable secrets backend.

Order of resolution:
    1. Cloud secret manager (AWS Secrets Manager / GCP Secret Manager / Azure Key Vault)
       - chosen by SECRETS_BACKEND env var.
    2. Airflow Variables / Connections (when running inside Airflow).
    3. Plain environment variables (local dev, CI).

Code never reads os.environ directly for sensitive values - it goes through
`get_secret()` so the resolution path can change per environment without code
edits.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from functools import lru_cache

from src.utils.logger import get_logger

log = get_logger(__name__)


class SecretsBackend(ABC):
    @abstractmethod
    def get(self, key: str) -> str | None: ...


class EnvBackend(SecretsBackend):
    def get(self, key: str) -> str | None:
        return os.getenv(key)


class AwsSecretsManagerBackend(SecretsBackend):
    def __init__(self, region: str | None = None) -> None:
        import boto3  # lazy import - boto3 is heavy
        self._client = boto3.client(
            "secretsmanager",
            region_name=region or os.getenv("AWS_DEFAULT_REGION"),
        )

    def get(self, key: str) -> str | None:
        try:
            response = self._client.get_secret_value(SecretId=key)
            return response.get("SecretString")
        except Exception as exc:  # noqa: BLE001
            log.warning("aws_secret_lookup_failed", key=key, error=str(exc))
            return None


class GcpSecretsManagerBackend(SecretsBackend):
    def __init__(self, project_id: str | None = None) -> None:
        from google.cloud import secretmanager  # type: ignore[import-untyped]
        self._client = secretmanager.SecretManagerServiceClient()
        self._project = project_id or os.getenv("GCP_PROJECT_ID")

    def get(self, key: str) -> str | None:
        if not self._project:
            return None
        try:
            name = f"projects/{self._project}/secrets/{key}/versions/latest"
            response = self._client.access_secret_version(request={"name": name})
            return response.payload.data.decode("utf-8")
        except Exception as exc:  # noqa: BLE001
            log.warning("gcp_secret_lookup_failed", key=key, error=str(exc))
            return None


class AzureKeyVaultBackend(SecretsBackend):
    def __init__(self, vault_url: str | None = None) -> None:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient  # type: ignore[import-untyped]
        url = vault_url or os.environ["AZURE_KEY_VAULT_URL"]
        self._client = SecretClient(vault_url=url, credential=DefaultAzureCredential())

    def get(self, key: str) -> str | None:
        try:
            return self._client.get_secret(key).value
        except Exception as exc:  # noqa: BLE001
            log.warning("azure_secret_lookup_failed", key=key, error=str(exc))
            return None


@lru_cache(maxsize=1)
def _backend() -> SecretsBackend:
    backend = os.getenv("SECRETS_BACKEND", "env").lower()
    log.info("secrets_backend_selected", backend=backend)
    if backend == "aws":
        return AwsSecretsManagerBackend()
    if backend == "gcp":
        return GcpSecretsManagerBackend()
    if backend == "azure":
        return AzureKeyVaultBackend()
    return EnvBackend()


def get_secret(key: str, default: str | None = None) -> str | None:
    """Resolve a secret value through the configured backend, falling back to env."""
    value = _backend().get(key)
    if value is None:
        value = os.getenv(key, default)
    return value


def require_secret(key: str) -> str:
    value = get_secret(key)
    if value is None or value.startswith("REPLACE_ME"):
        raise RuntimeError(f"Secret '{key}' is not configured")
    return value
