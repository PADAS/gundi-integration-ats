import pytest
from app.services.action_runner import execute_action
from app.actions.handlers import PENDING_FILES, retrieve_transmissions, retrieve_data_points


@pytest.mark.asyncio
async def test_execute_pull_observations_action(
        mocker, mock_gundi_client_v2, mock_state_manager, mock_file_storage, mock_ats_client,
        mock_get_gundi_api_key, mock_gundi_sensors_client_class, ats_integration_v2,
        mock_ats_data_parsed, mock_publish_event, mock_gundi_client_v2_class, mock_config_manager_ats
):
    mocker.patch("app.services.action_runner._portal", mock_gundi_client_v2)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.config_manager", mock_config_manager_ats)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)
    mocker.patch("app.actions.handlers.ats_client", mock_ats_client)
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)

    response = await execute_action(
        integration_id=str(ats_integration_v2.id),
        action_id="pull_observations"
    )

    assert "data_points_file" in response
    assert "transmissions_file" in response
    # Check that the data is extracted from ATS
    assert mock_ats_client.get_transmissions_endpoint_response.called
    assert mock_ats_client.get_data_endpoint_response.called
    # Check that the data is saved as xml files in the cloud
    assert mock_file_storage.upload_file.call_count == 2
    assert not mock_gundi_sensors_client_class.return_value.post_observations.called  # No data sent to Gundi
    # Check that the data file is marked as pending for processing
    mock_state_manager.group_add.assert_called_once_with(
        group_name=PENDING_FILES,
        values=[response["data_points_file"]]
    )
