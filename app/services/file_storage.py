import aiofiles
from gcloud.aio.storage import Storage
from app import settings


class CloudFileStorage:
    def __init__(self, bucket_name=None):
        self.bucket_name = bucket_name or settings.GCP_BUCKET_NAME
        self.storage_client = Storage()

    async def upload_file(self, local_file_path, destination_blob_name, metadata=None):
        async with aiofiles.open(local_file_path, 'rb') as file:
            content = await file.read()
            await self.storage_client.upload(self.bucket_name, destination_blob_name, content, metadata=metadata)

    async def download_file(self, source_blob_name, destination_file_path):
        content = await self.storage_client.download(self.bucket_name, source_blob_name)
        async with aiofiles.open(destination_file_path, 'wb') as file:
            await file.write(content)

    async def delete_file(self, blob_name):
        await self.storage_client.delete(self.bucket_name, blob_name)

    async def list_files(self, prefix=None):
        blobs = await self.storage_client.list_objects(self.bucket_name, prefix=prefix)
        return [blob['name'] for blob in blobs.get('items', [])]

    async def get_file_metadata(self, blob_name):
        return await self.storage_client.download_metadata(self.bucket_name, blob_name)
