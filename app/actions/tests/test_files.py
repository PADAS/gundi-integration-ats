import pytest
from unittest.mock import AsyncMock
from app.actions.handlers import action_get_file_status, action_set_file_status, action_reprocess_file, PENDING_FILES
from app.actions.configurations import FileStatus, GetFileStatusConfig, SetFileStatusConfig, ReprocessFileConfig

@pytest.mark.asyncio
async def test_action_get_file_status(mocker, integration_v2, mock_state_manager):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    action_config = GetFileStatusConfig(filename="test_file.xml")

    result = await action_get_file_status(integration_v2, action_config)

    mock_state_manager.group_ismember.assert_any_call(PENDING_FILES, "test_file.xml")
    assert result == {"file_status": FileStatus.PENDING.value}

@pytest.mark.asyncio
async def test_action_set_file_status(mocker, integration_v2, mock_state_manager, mock_file_storage):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)
    action_config = SetFileStatusConfig(filename="test_file.xml", status=FileStatus.IN_PROGRESS)

    result = await action_set_file_status(integration_v2, action_config)

    mock_state_manager.group_move.assert_any_call(
        from_group="ats_pending_files",
        to_group="ats_in_progress_files",
        values=[action_config.filename]
    )
    assert result == {"file_status": action_config.status.value}

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
