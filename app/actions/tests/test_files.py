import pytest
from unittest.mock import AsyncMock, patch
from app.actions.handlers import action_get_file_status, action_set_file_status, action_reprocess_file
from app.actions.ats_client import  FileStatus
from app.actions.configurations import GetFileStatusConfig, SetFileStatusConfig, ReprocessFileConfig

@pytest.mark.asyncio
async def test_action_get_file_status(mocker, integration_v2, mock_file_storage):
    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)
    action_config = GetFileStatusConfig(filename="test_file.xml")

    result = await action_get_file_status(integration_v2, action_config)

    mock_file_storage.get_file_metadata.assert_any_call(
        integration_id=str(integration_v2.id),
        blob_name="test_file.xml"
    )
    assert result == {"file_status": "pending"}

@pytest.mark.asyncio
async def test_action_set_file_status(mocker, integration_v2, mock_file_storage):
    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)
    action_config = SetFileStatusConfig(filename="test_file.xml", status=FileStatus.PROCESSED)

    result = await action_set_file_status(integration_v2, action_config)

    mock_file_storage.update_file_metadata.assert_any_call(
        integration_id=str(integration_v2.id),
        blob_name="test_file.xml",
        metadata={"status": FileStatus.PROCESSED}
    )
    assert result == {"file_status": FileStatus.PROCESSED}

@pytest.mark.asyncio
async def test_action_reprocess_file(mocker, integration_v2, mock_file_storage, mock_state_manager):
    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_process_data_file = mocker.patch("app.actions.handlers.process_data_file", new_callable=AsyncMock, return_value=10)
    action_config = ReprocessFileConfig(filename="test_file.xml")

    result = await action_reprocess_file(integration_v2, action_config)

    mock_process_data_file.assert_awaited_once_with(
        file_name="test_file.xml",
        integration=integration_v2,
        process_config=action_config
    )
    assert result == {"observations_processed": 10}
