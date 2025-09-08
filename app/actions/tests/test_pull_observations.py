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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "xml_response",
    [
        '<DataSet><xs:schema id="NewDataSet" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:msdata="urn:schemas-microsoft-com:xml-msdata"><xs:element name="NewDataSet" msdata:IsDataSet="true" msdata:UseCurrentLocale="true"><xs:complexType><xs:choice minOccurs="0" maxOccurs="unbounded"><xs:element name="Table"><xs:complexType><xs:sequence><xs:element name="DateSent" type="xs:dateTime" minOccurs="0"/><xs:element name="CollarSerialNum" type="xs:string" minOccurs="0"/><xs:element name="NumberFixes" type="xs:int" minOccurs="0"/><xs:element name="BattVoltage" type="xs:string" minOccurs="0"/><xs:element name="Mortality" type="xs:string" minOccurs="0"/><xs:element name="BreakOff" type="xs:string" minOccurs="0"/><xs:element name="SatErrors" type="xs:short" minOccurs="0"/><xs:element name="YearBase" type="xs:short" minOccurs="0"/><xs:element name="DayBase" type="xs:short" minOccurs="0"/><xs:element name="GmtOffset" type="xs:short" minOccurs="0"/><xs:element name="Event" type="xs:string" minOccurs="0"/><xs:element name="evCondition" type="xs:string" minOccurs="0"/><xs:element name="LowBattVoltage" type="xs:boolean" minOccurs="0"/></xs:sequence></xs:complexType></xs:element></xs:choice></xs:complexType></xs:element></xs:schema><diffgr:diffgram xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1" xmlns:msdata="urn:schemas-microsoft-com:xml-msdata"/></DataSet>',
        '<DataSet><xs:schema id=\"NewDataSet\" xmlns:xs=\"http:\/\/www.w3.org\/2001\/XMLSchema\" xmlns:msdata=\"urn:schemas-microsoft-com:xml-msdata\"><xs:element name=\"NewDataSet\" msdata:IsDataSet=\"true\" msdata:UseCurrentLocale=\"true\"><xs:complexType><xs:choice minOccurs=\"0\" maxOccurs=\"unbounded\"><xs:element name=\"Table\"><xs:complexType><xs:sequence><xs:element name=\"AtsSerialNum\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Latitude\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Longitude\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"DateYearAndJulian\" type=\"xs:dateTime\" minOccurs=\"0\"\/><xs:element name=\"NumSats\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Hdop\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"FixTime\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Dimension\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Activity\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Temperature\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Mortality\" type=\"xs:boolean\" minOccurs=\"0\"\/><xs:element name=\"LowBattVoltage\" type=\"xs:boolean\" minOccurs=\"0\"\/><\/xs:sequence><\/xs:complexType><\/xs:element><\/xs:choice><\/xs:complexType><\/xs:element><\/xs:schema><diffgr:diffgram xmlns:diffgr=\"urn:schemas-microsoft-com:xml-diffgram-v1\" xmlns:msdata=\"urn:schemas-microsoft-com:xml-msdata\"\/><\/DataSet>'
    ]
)
async def test_retrieve_transmissions_xml_from_server(mocker, mock_file_storage, xml_response):
    mocker.patch(
        "app.actions.ats_client.get_transmissions_endpoint_response",
        return_value=xml_response
    )
    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)

    integration_id = "test_integration"
    auth_config = mocker.Mock()
    pull_config = mocker.Mock()
    file_prefix = "test_prefix"

    result = await retrieve_transmissions(
        integration_id, auth_config, pull_config, file_prefix
    )
    assert result.endswith("_transmissions.xml")
    assert mock_file_storage.upload_file.call_count == 1


@pytest.mark.asyncio
async def test_retrieve_transmissions_failure(mocker):
    mocker.patch(
        "app.actions.ats_client.get_transmissions_endpoint_response",
        side_effect=Exception("Error")
    )

    integration_id = "test_integration"
    auth_config = mocker.Mock()
    pull_config = mocker.Mock()
    file_prefix = "test_prefix"

    with pytest.raises(Exception):
        await retrieve_transmissions(integration_id, auth_config, pull_config, file_prefix)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "xml_response",
    [
        '<DataSet><xs:schema id="NewDataSet" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:msdata="urn:schemas-microsoft-com:xml-msdata"><xs:element name="NewDataSet" msdata:IsDataSet="true" msdata:UseCurrentLocale="true"><xs:complexType><xs:choice minOccurs="0" maxOccurs="unbounded"><xs:element name="Table"><xs:complexType><xs:sequence><xs:element name="DateSent" type="xs:dateTime" minOccurs="0"/><xs:element name="CollarSerialNum" type="xs:string" minOccurs="0"/><xs:element name="NumberFixes" type="xs:int" minOccurs="0"/><xs:element name="BattVoltage" type="xs:string" minOccurs="0"/><xs:element name="Mortality" type="xs:string" minOccurs="0"/><xs:element name="BreakOff" type="xs:string" minOccurs="0"/><xs:element name="SatErrors" type="xs:short" minOccurs="0"/><xs:element name="YearBase" type="xs:short" minOccurs="0"/><xs:element name="DayBase" type="xs:short" minOccurs="0"/><xs:element name="GmtOffset" type="xs:short" minOccurs="0"/><xs:element name="Event" type="xs:string" minOccurs="0"/><xs:element name="evCondition" type="xs:string" minOccurs="0"/><xs:element name="LowBattVoltage" type="xs:boolean" minOccurs="0"/></xs:sequence></xs:complexType></xs:element></xs:choice></xs:complexType></xs:element></xs:schema><diffgr:diffgram xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1" xmlns:msdata="urn:schemas-microsoft-com:xml-msdata"/></DataSet>',
        '<DataSet><xs:schema id=\"NewDataSet\" xmlns:xs=\"http:\/\/www.w3.org\/2001\/XMLSchema\" xmlns:msdata=\"urn:schemas-microsoft-com:xml-msdata\"><xs:element name=\"NewDataSet\" msdata:IsDataSet=\"true\" msdata:UseCurrentLocale=\"true\"><xs:complexType><xs:choice minOccurs=\"0\" maxOccurs=\"unbounded\"><xs:element name=\"Table\"><xs:complexType><xs:sequence><xs:element name=\"AtsSerialNum\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Latitude\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Longitude\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"DateYearAndJulian\" type=\"xs:dateTime\" minOccurs=\"0\"\/><xs:element name=\"NumSats\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Hdop\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"FixTime\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Dimension\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Activity\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Temperature\" type=\"xs:string\" minOccurs=\"0\"\/><xs:element name=\"Mortality\" type=\"xs:boolean\" minOccurs=\"0\"\/><xs:element name=\"LowBattVoltage\" type=\"xs:boolean\" minOccurs=\"0\"\/><\/xs:sequence><\/xs:complexType><\/xs:element><\/xs:choice><\/xs:complexType><\/xs:element><\/xs:schema><diffgr:diffgram xmlns:diffgr=\"urn:schemas-microsoft-com:xml-diffgram-v1\" xmlns:msdata=\"urn:schemas-microsoft-com:xml-msdata\"\/><\/DataSet>'
    ]
)
async def test_retrieve_data_points_xml_from_server(mocker, mock_file_storage, mock_state_manager, xml_response):
    mocker.patch(
        "app.actions.ats_client.get_data_endpoint_response",
        return_value=xml_response
    )
    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)

    integration_id = "test_integration"
    auth_config = mocker.Mock()
    pull_config = mocker.Mock()
    file_prefix = "test_prefix"

    result = await retrieve_data_points(
        integration_id, auth_config, pull_config, file_prefix
    )
    assert result.endswith("_data_points.xml")
    assert mock_file_storage.upload_file.call_count == 1

    mock_state_manager.group_add.assert_called_once_with(
        group_name=PENDING_FILES,
        values=[result]
    )


@pytest.mark.asyncio
async def test_retrieve_data_points_failure(mocker):
    mocker.patch(
        "app.actions.ats_client.get_transmissions_endpoint_response",
        side_effect=Exception("Error")
    )

    integration_id = "test_integration"
    auth_config = mocker.Mock()
    pull_config = mocker.Mock()
    file_prefix = "test_prefix"

    with pytest.raises(Exception):
        await retrieve_data_points(integration_id, auth_config, pull_config, file_prefix)
