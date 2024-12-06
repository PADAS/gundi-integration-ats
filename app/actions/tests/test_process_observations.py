import pytest
from app.services.action_runner import execute_action
from ..handlers import PENDING_FILES, PROCESSED_FILES


@pytest.mark.asyncio
async def test_execute_process_observations_action(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_file_storage, mock_ats_client,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, ats_integration_v2,
        mock_transmissions_file_name, mock_data_file_name, mock_publish_event,
        mock_gundi_client_v2_class, mock_aiofiles
):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.aiofiles", mock_aiofiles)
    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)
    mocker.patch("app.actions.handlers.ats_client", mock_ats_client)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    integration_id = str(ats_integration_v2.id)

    response = await execute_action(
        integration_id=integration_id,
        action_id="process_observations"
    )

    assert response.get("observations_processed") == 3

    # Check that pending files were processed
    mock_state_manager.group_get.assert_called_once_with(PENDING_FILES)
    assert mock_ats_client.parse_transmissions_from_xml.called
    assert mock_ats_client.parse_data_points_from_xml.called
    # Check that the observations were sent to gundi
    assert mock_gundi_sensors_client_class.return_value.post_observations.called
    # Check that the data file is marked as processed
    mock_state_manager.group_move.assert_called_once_with(
        from_group=PENDING_FILES,
        to_group=PROCESSED_FILES,
        values=[mock_data_file_name]
    )
    # Check that processed files are removed from storage
    mock_file_storage.delete_file.assert_any_call(
        integration_id=integration_id,
        blob_name=mock_transmissions_file_name
    )
    mock_file_storage.delete_file.assert_any_call(
        integration_id=integration_id,
        blob_name=mock_data_file_name
    )
