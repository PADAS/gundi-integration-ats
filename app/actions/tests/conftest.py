import asyncio
import pytest
import datetime

import xmltodict
from gundi_core.schemas.v2 import Integration

from app.actions.ats_client import TransmissionsResponse, DataResponse, ATSBadXMLException


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f


@pytest.fixture
def ats_integration_v2():
    return Integration.parse_obj(
        {
            "id": "1eb8ba40-6312-4093-9b47-7786320b11fb",
            "name": "ATS Test Integration", "base_url": "", "enabled": False,
            "type": {"id": "c1d15ed0-6f56-45bc-853c-099ba1d8e8d2", "name": "Ats", "value": "ats",
                     "description": "Default type for integrations with ATS", "actions": [
                    {"id": "dc78e1d4-1492-493a-9cf7-958c66a5b3c6", "type": "auth", "name": "Authenticate",
                     "value": "auth", "description": "Authenticate against ATS",
                     "schema": {"type": "object", "title": "ATS Credentials", "required": ["username", "password"],
                                "properties": {
                                    "password": {"type": "string", "title": "Password", "format": "password"},
                                    "username": {"type": "string", "title": "Username"}}}, "ui_schema": {}},
                    {"id": "38a6af88-5b87-4c3a-b3ff-30fbc49e2785", "type": "pull", "name": "Process Observations",
                     "value": "process_observations", "description": "Ats Process Observations action",
                     "schema": {"type": "object", "title": "ProcessObservationsConfig", "properties": {},
                                "definitions": {}}, "ui_schema": {}},
                    {"id": "1a73af40-5b1e-4afe-8f20-bc54ab4e8383", "type": "pull", "name": "Pull Observations",
                     "value": "pull_observations", "description": "Ats Pull Observations action",
                     "schema": {"type": "object", "title": "PullObservationsConfig",
                                "required": ["data_endpoint", "transmissions_endpoint"],
                                "properties": {"data_endpoint": {"type": "string", "title": "Data Endpoint"},
                                               "transmissions_endpoint": {"type": "string",
                                                                          "title": "Transmissions Endpoint"}},
                                "definitions": {}}, "ui_schema": {}}], "webhook": None},
            "owner": {"id": "f5a91774-fe2e-4b92-ad94-6f752bcc1409", "name": "EarthRanger Team", "description": ''},
            "configurations": [
                {"id": "8db70fe2-c8f7-4701-ad24-5c64c030b69e", "integration": "1eb8ba40-6312-4093-9b47-7786320b11fb",
                 "action": {"id": "38a6af88-5b87-4c3a-b3ff-30fbc49e2785", "type": "pull",
                            "name": "Process Observations", "value": "process_observations"},
                 "data": {"mortality_event_type": "mortality_event", "observations_per_request": 200}},
                {"id": "5b1e1e2e-89d9-47b2-adac-3f87453c47f3", "integration": "1eb8ba40-6312-4093-9b47-7786320b11fb",
                 "action": {"id": "dc78e1d4-1492-493a-9cf7-958c66a5b3c6", "type": "auth", "name": "Authenticate",
                            "value": "auth"}, "data": {"password": "testpswd", "username": "testusr"}},
                {"id": "7fc52e15-48a7-4ee6-9869-c217c204e090", "integration": "1eb8ba40-6312-4093-9b47-7786320b11fb",
                 "action": {"id": "1a73af40-5b1e-4afe-8f20-bc54ab4e8383", "type": "pull", "name": "Pull Observations",
                            "value": "pull_observations"},
                 "data": {"data_endpoint": "http://ats-url.org/Service1.svc/GetPointsAtsIri/1",
                          "transmissions_endpoint": "http://ats-url.org/Service1.svc/GetAllTransmission/1"}}],
            "webhook_configuration": None, "additional": {},
            "default_route": {"id": "a315015f-fd02-430d-8c35-4656c76446ae",
                              "name": "ATS Test - Default Route"}, "status": "healthy",
            "status_details": ''}
    )


@pytest.fixture
def mock_gundi_client_v2(
        mocker,
        ats_integration_v2,
):
    mock_client = mocker.MagicMock()
    mock_client.get_integration_details.return_value = async_return(
        ats_integration_v2
    )
    mock_client.__aenter__.return_value = mock_client
    return mock_client


@pytest.fixture
def mock_transmissions_file_name(mocker):
    return "20241206121217722379_1eb8ba40-6312-4093-9b47-7786320b11fb_transmissions.xml"


@pytest.fixture
def mock_data_file_name(mocker):
    return "20241206121217722379_1eb8ba40-6312-4093-9b47-7786320b11fb_data_points.xml"


@pytest.fixture
def mock_state_manager(mocker, mock_transmissions_file_name, mock_data_file_name):
    mock_state_manager = mocker.MagicMock()
    mock_state_manager.get_state.return_value = async_return({})
    mock_state_manager.set_state.return_value = async_return(None)
    mock_state_manager.group_add.return_value = async_return(2)
    mock_state_manager.group_get.return_value = async_return([
        mock_transmissions_file_name,
        mock_data_file_name,
    ])
    mock_state_manager.group_ismember.return_value = async_return(True)
    mock_state_manager.group_move.return_value = async_return(1)
    mock_state_manager.group_remove.return_value = async_return(1)
    return mock_state_manager


@pytest.fixture
def mock_file_storage(mocker):
    mock_file_storage = mocker.MagicMock()
    mock_file_storage.upload_file.return_value = async_return(None)
    mock_file_storage.download_file.return_value = async_return(None)
    mock_file_storage.delete_file.return_value = async_return(None)
    mock_file_storage.update_file_metadata.return_value = async_return(None)
    mock_file_storage.get_file_metadata.return_value = async_return({"status": "pending"})
    mock_file_storage.list_files.return_value = async_return([
        "20241206121217722379_1eb8ba40-6312-4093-9b47-7786320b11fb_transmissions.xml",
        "20241206121217722379_1eb8ba40-6312-4093-9b47-7786320b11fb_data_points.xml",
    ])
    return mock_file_storage


@pytest.fixture
def mock_ats_transmissions_response_xml():
    with open("app/actions/tests/files/ats_transmissions.xml") as f:
        return f.read()


@pytest.fixture
def mock_ats_transmissions_response_with_invalid_offsets():
    with open("app/actions/tests/files/ats_transmissions_invalid_offsets.xml") as f:
        return f.read()


@pytest.fixture
def mock_ats_transmissions_response_empty_xml():
    with open("app/actions/tests/files/ats_no_transmissions.xml") as f:
        return f.read()


@pytest.fixture
def mock_ats_transmissions_response_single_point_xml():
    with open("app/actions/tests/files/ats_transmissions_single_point.xml") as f:
        return f.read()


@pytest.fixture
def mock_ats_transmissions_parsed(mock_ats_transmissions_response_xml):
    return [
        TransmissionsResponse(
            date_sent=datetime.datetime(2024, 10, 26, 23, 12, 10, 740000, tzinfo=datetime.timezone.utc),
            collar_serial_num="052191",
            number_fixes=21,
            batt_voltage=7.056,
            mortality="THIS COLLAR IS IN MORTALITY !!",
            break_off="No",
            sat_errors="0",
            year_base="24",
            day_base="294",
            gmt_offset=0,
            low_batt_voltage=False
        ),
        TransmissionsResponse(
            date_sent=datetime.datetime(2024, 8, 7, 11, 12, 22, 430000, tzinfo=datetime.timezone.utc),
            collar_serial_num="052194",
            number_fixes=9,
            batt_voltage=6.984,
            mortality="No",
            break_off="No",
            sat_errors="7",
            year_base="24",
            day_base="217",
            gmt_offset=3,
            low_batt_voltage=False
        ),
    ]


@pytest.fixture
def mock_ats_transmissions_with_invalid_offsets_parsed(mock_ats_transmissions_response_xml):
    return [
        TransmissionsResponse(
            date_sent=datetime.datetime(2024, 10, 26, 23, 12, 10, 740000, tzinfo=datetime.timezone.utc),
            collar_serial_num="052191",
            number_fixes=21,
            batt_voltage=7.056,
            mortality="THIS COLLAR IS IN MORTALITY !!",
            break_off="No",
            sat_errors="0",
            year_base="24",
            day_base="294",
            gmt_offset=25,
            low_batt_voltage=False
        ),
        TransmissionsResponse(
            date_sent=datetime.datetime(2024, 8, 7, 11, 12, 22, 430000, tzinfo=datetime.timezone.utc),
            collar_serial_num="052194",
            number_fixes=9,
            batt_voltage=6.984,
            mortality="No",
            break_off="No",
            sat_errors="7",
            year_base="24",
            day_base="217",
            gmt_offset=-25,
            low_batt_voltage=False
        ),
    ]


@pytest.fixture
def mock_ats_transmissions_with_single_point_parsed(mock_ats_transmissions_response_xml):
    return [
        TransmissionsResponse(
            date_sent=datetime.datetime(2024, 10, 26, 23, 12, 10, 740000, tzinfo=datetime.timezone.utc),
            collar_serial_num="052191",
            number_fixes=21,
            batt_voltage=7.056,
            mortality="THIS COLLAR IS IN MORTALITY !!",
            break_off="No",
            sat_errors="0",
            year_base="24",
            day_base="294",
            gmt_offset=0,
            low_batt_voltage=False
        ),
    ]


@pytest.fixture
def mock_ats_data_response_xml():
    with open("app/actions/tests/files/ats_data_points.xml") as f:
        return f.read()


@pytest.fixture
def mock_ats_data_response_with_invalid_xml():
    with open("app/actions/tests/files/ats_data_points_invalid_xml.xml") as f:
        return f.read()


@pytest.fixture
def mock_ats_data_response_no_points_xml():
    with open("app/actions/tests/files/ats_no_data_points.xml") as f:
        return f.read()


@pytest.fixture
def mock_ats_data_response_single_point_xml():
    with open("app/actions/tests/files/ats_data_points_single_point.xml") as f:
        return f.read()


@pytest.fixture
def mock_ats_data_parsed(mock_ats_data_response_xml):
    return {
        "052194": [
            DataResponse(
                ats_serial_num="052194",
                longitude=-68.52625,
                latitude=5.52827,
                date_year_and_julian=datetime.datetime(2024, 5, 31, 0, 0),
                num_sats="08",
                hdop="0.9",
                fix_time="039",
                dimension="3",
                activity="02",
                temperature="+24",
                mortality=False,
                low_batt_voltage=False
            ),
            DataResponse(
                ats_serial_num="052194",
                longitude=-68.52596,
                latitude=5.52827,
                date_year_and_julian=datetime.datetime(2024, 5, 31, 8, 0),
                num_sats="10",
                hdop="0.8",
                fix_time="039",
                dimension="3",
                activity="02",
                temperature="+24",
                mortality=False,
                low_batt_voltage=False
            )
        ],
        "052191": [
            DataResponse(
                ats_serial_num="052191",
                longitude=-68.26540,
                latitude=5.44309,
                date_year_and_julian=datetime.datetime(2024, 10, 26, 16, 0),
                num_sats="07",
                hdop="1.1",
                fix_time="039",
                dimension="3",
                activity="00",
                temperature="+28",
                mortality=True,
                low_batt_voltage=False
            )
        ],
    }


@pytest.fixture
def mock_ats_data_single_point_parsed(mock_ats_data_response_xml):
    return {
        "052194": [
            DataResponse(
                ats_serial_num="052194",
                longitude=-68.52625,
                latitude=5.52827,
                date_year_and_julian=datetime.datetime(2024, 5, 31, 0, 0),
                num_sats="08",
                hdop="0.9",
                fix_time="039",
                dimension="3",
                activity="02",
                temperature="+24",
                mortality=False,
                low_batt_voltage=False
            )
        ]
    }


@pytest.fixture
def mock_aiofiles(
        mocker,
        mock_ats_transmissions_response_xml
):
    mock_client = mocker.MagicMock()
    mock_fd = mocker.MagicMock()
    mock_fd.__aenter__.return_value = mock_fd
    mock_fd.read.return_value = async_return(mock_ats_transmissions_response_xml)
    mock_client.open.return_value = mock_fd
    return mock_client


@pytest.fixture
def mock_ats_client(
        mocker,
        mock_ats_data_response_xml,
        mock_ats_transmissions_response_xml,
        mock_ats_data_parsed,
        mock_ats_transmissions_parsed,

):
    ats_client_mock = mocker.MagicMock()
    ats_client_mock.get_data_endpoint_response.return_value = async_return(mock_ats_data_response_xml)
    ats_client_mock.get_transmissions_endpoint_response.return_value = async_return(mock_ats_transmissions_response_xml)
    ats_client_mock.parse_data_points_from_xml.return_value = mock_ats_data_parsed
    ats_client_mock.parse_transmissions_from_xml.return_value = mock_ats_transmissions_parsed
    return ats_client_mock


@pytest.fixture
def mock_ats_client_with_invalid_tz_offsets(
        mocker,
        mock_ats_data_response_xml,
        mock_ats_transmissions_response_with_invalid_offsets,
        mock_ats_data_parsed,
        mock_ats_transmissions_with_invalid_offsets_parsed,

):
    ats_client_mock = mocker.MagicMock()
    ats_client_mock.get_data_endpoint_response.return_value = async_return(mock_ats_data_response_xml)
    ats_client_mock.get_transmissions_endpoint_response.return_value = async_return(mock_ats_transmissions_response_with_invalid_offsets)
    ats_client_mock.parse_data_points_from_xml.return_value = mock_ats_data_parsed
    ats_client_mock.parse_transmissions_from_xml.return_value = mock_ats_transmissions_with_invalid_offsets_parsed
    return ats_client_mock


@pytest.fixture
def mock_ats_client_with_parse_error(
        mocker,
        mock_ats_data_response_xml,
        mock_ats_transmissions_response_xml,
        mock_ats_data_parsed,
        mock_ats_transmissions_parsed,

):
    ats_client_mock = mocker.MagicMock()
    ats_client_mock.get_data_endpoint_response.return_value = async_return(mock_ats_data_response_xml)
    ats_client_mock.get_transmissions_endpoint_response.return_value = async_return(mock_ats_transmissions_response_xml)
    ats_client_mock.parse_data_points_from_xml.side_effect = (
        ATSBadXMLException(message="Invalid XML.",  error=xmltodict.ParsingInterrupted()),
    )
    ats_client_mock.parse_transmissions_from_xml.return_value = mock_ats_transmissions_parsed
    return ats_client_mock


@pytest.fixture
def mock_gundi_sensors_client_class(mocker, events_created_response, observations_created_response):
    mock_gundi_sensors_client_class = mocker.MagicMock()
    mock_gundi_sensors_client = mocker.MagicMock()
    mock_gundi_sensors_client.post_events.return_value = async_return(
        events_created_response
    )
    mock_gundi_sensors_client.post_observations.return_value = async_return(
        observations_created_response
    )
    mock_gundi_sensors_client_class.return_value = mock_gundi_sensors_client
    return mock_gundi_sensors_client_class


@pytest.fixture
def events_created_response():
    return [
        {
            "object_id": "abebe106-3c50-446b-9c98-0b9b503fc900",
            "created_at": "2023-11-16T19:59:50.612864Z"
        },
        {
            "object_id": "cdebe106-3c50-446b-9c98-0b9b503fc911",
            "created_at": "2023-11-16T19:59:50.612864Z"
        }
    ]


@pytest.fixture
def observations_created_response():
    return [
        {
            "object_id": "efebe106-3c50-446b-9c98-0b9b503fc922",
            "created_at": "2023-11-16T19:59:55.612864Z"
        },
        {
            "object_id": "ghebe106-3c50-446b-9c98-0b9b503fc933",
            "created_at": "2023-11-16T19:59:56.612864Z"
        }
    ]


class AsyncIterator:
    def __init__(self, seq):
        self.iter = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration


@pytest.fixture
def mock_get_gundi_api_key(mocker, mock_api_key):
    mock = mocker.MagicMock()
    mock.return_value = async_return(mock_api_key)
    return mock


@pytest.fixture
def mock_api_key():
    return "MockAP1K3y"


@pytest.fixture
def er_client_close_response():
    return {}
