from __future__ import annotations
import io, os
from typing import List, Tuple
from google.cloud import storage

_client = None

def client() -> storage.Client:
    global _client
    if _client is None:
        _client = storage.Client()
    return _client

def _split(gcs_path: str) -> Tuple[str, str]:
    assert gcs_path.startswith("gs://"), f"not a GCS path: {gcs_path}"
    x = gcs_path[5:]
    return x.split("/", 1) if "/" in x else (x, "")

def read_bytes(gcs_path: str) -> bytes:
    b, k = _split(gcs_path)
    blob = client().bucket(b).blob(k)
    return blob.download_as_bytes()

def read_text(gcs_path: str) -> str:
    return read_bytes(gcs_path).decode("utf-8")

def write_bytes(gcs_path: str, data: bytes, content_type: str | None = None) -> None:
    b, k = _split(gcs_path)
    blob = client().bucket(b).blob(k)
    if content_type:
        blob.content_type = content_type
    blob.upload_from_file(io.BytesIO(data), rewind=True)

def upload_file(local_path: str, gcs_path: str, content_type: str | None = None) -> None:
    with open(local_path, "rb") as f:
        write_bytes(gcs_path, f.read(), content_type)

def download_to_file(gcs_path: str, local_path: str) -> None:
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    b, k = _split(gcs_path)
    blob = client().bucket(b).blob(k)
    blob.download_to_filename(local_path)

def list_prefix(gcs_prefix: str) -> list[tuple[str, int]]:
    b, k = _split(gcs_prefix)
    if k and not k.endswith("/"):
        k += "/"
    out: List[Tuple[str, int]] = []
    for blob in client().list_blobs(b, prefix=k):
        out.append((blob.name, int(blob.size or 0)))
    return out
