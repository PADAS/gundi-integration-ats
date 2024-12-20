import aiohttp
import stamina
import asyncio
from gcloud.aio.storage import Storage
from app import settings


class CloudFileStorage:
    def __init__(self, bucket_name=None, root_prefix=None):
        self.root_prefix = root_prefix or settings.GCP_BUCKET_ROOT_PREFIX
        self.bucket_name = bucket_name or settings.GCP_BUCKET_NAME
        self._storage_client = None  # Lazy initialization

    @property
    def storage_client(self):
        if self._storage_client is None:
            self._storage_client = Storage()
        return self._storage_client

    def get_file_fullname(self, integration_id, blob_name):
        return f"{self.root_prefix}/{integration_id}/{blob_name}"

    async def upload_file(self, integration_id, local_file_path, destination_blob_name, metadata=None):
        target_path = self.get_file_fullname(integration_id, destination_blob_name)
        custom_metadata = {"metadata": metadata} if metadata else None
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.upload_from_filename(
                    self.bucket_name, target_path, local_file_path, metadata=custom_metadata
                )

    async def download_file(self, integration_id, source_blob_name, destination_file_path):
        source_path = self.get_file_fullname(integration_id, source_blob_name)
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.download_to_filename(self.bucket_name, source_path, destination_file_path)

    async def delete_file(self, integration_id, blob_name):
        target_path = self.get_file_fullname(integration_id, blob_name)
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.delete(self.bucket_name, target_path)

    async def list_files(self, integration_id):
        blobs = await self.storage_client.list_objects(self.bucket_name, params={"prefix": f"{self.root_prefix}/{integration_id}"})
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                return [blob['name'] for blob in blobs.get('items', [])]

    async def get_file_metadata(self, integration_id, blob_name):
        target_path = self.get_file_fullname(integration_id, blob_name)
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                response = await self.storage_client.download_metadata(self.bucket_name, target_path)
                return response.get('metadata', {})

    async def update_file_metadata(self, integration_id, blob_name, metadata):
        target_path = self.get_file_fullname(integration_id, blob_name)
        custom_metadata = {"metadata": metadata}
        for attempt in stamina.retry_context(on=(aiohttp.ClientError, asyncio.TimeoutError),
                                             attempts=5, wait_initial=1.0, wait_max=30, wait_jitter=3.0):
            with attempt:
                await self.storage_client.patch_metadata(self.bucket_name, target_path, custom_metadata)

