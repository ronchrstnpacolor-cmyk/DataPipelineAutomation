from google.cloud import secretmanager

from .config import PROJECT_ID, SECRET_NAME


def get_secret(secret_id: str = SECRET_NAME, version: str = "latest") -> str:
    """Fetch a secret value from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")
