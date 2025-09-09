import httpx
import pytest
import respx
import xmltodict

from app.actions.ats_client import (
    get_transmissions_endpoint_response,
    get_data_endpoint_response,
    parse_data_points_from_xml,
    parse_transmissions_from_xml,
    ATSBadXMLException,
)
from app.actions.configurations import PullObservationsConfig, AuthenticateConfig


@pytest.mark.asyncio
async def test_get_transmissions_endpoint_response(ats_integration_v2, mock_ats_transmissions_response_xml):
    # Mock httpx response for transmissions endpoint
    async with respx.mock(assert_all_called=True) as ats_api_mock:
        pull_config = PullObservationsConfig(
            data_endpoint='http://test.ats.org/Service1.svc/GetPointsAtsIri/1',
            transmissions_endpoint='http://test.ats.org/Service1.svc/GetAllTransmission/1'
        )
        auth_config = AuthenticateConfig(
            username='test',
            password='test'
        )
        ats_api_mock.get(pull_config.transmissions_endpoint).respond(
            status_code=httpx.codes.OK,
            text=mock_ats_transmissions_response_xml
        )
        response = await get_transmissions_endpoint_response(
            integration_id=str(ats_integration_v2.id),
            config=pull_config,
            auth=auth_config,
            parse_response=False,
        )
        assert response == mock_ats_transmissions_response_xml


def test_parse_transmissions_from_xml(mock_ats_transmissions_response_xml, mock_ats_transmissions_parsed):
    result = parse_transmissions_from_xml(mock_ats_transmissions_response_xml)
    assert result == mock_ats_transmissions_parsed


def test_parse_transmissions_from_xml_with_invalid_offset(
        mock_ats_transmissions_response_with_invalid_offsets, mock_ats_transmissions_with_invalid_offsets_parsed
):
    result = parse_transmissions_from_xml(mock_ats_transmissions_response_with_invalid_offsets)
    assert result == mock_ats_transmissions_with_invalid_offsets_parsed  # Invalid offsets are accepted and fixed later


def test_parse_transmissions_from_empty_xml(mock_ats_transmissions_response_empty_xml):
    result = parse_data_points_from_xml(mock_ats_transmissions_response_empty_xml)
    assert result == {}


def test_parse_transmissions_from_xml_with_single_point(
        mock_ats_transmissions_response_single_point_xml,
        mock_ats_transmissions_with_single_point_parsed
):
    # First validate the original XML parse a single point as dict
    parsed_xml = xmltodict.parse(mock_ats_transmissions_response_single_point_xml)
    data_xml_tag = parsed_xml["DataSet"].get("diffgr:diffgram", {})
    data = data_xml_tag.get("NewDataSet", {})

    assert isinstance(data.get("Table", None), dict)

    # Then validate after calling parsing method, the single point is converted to a list and is parsed correctly
    result = parse_transmissions_from_xml(mock_ats_transmissions_response_single_point_xml)
    assert isinstance(result, list)
    assert len(result) == 1  # Ensure the single item was converted to a list
    assert result == mock_ats_transmissions_with_single_point_parsed


def test_parse_transmissions_from_escaped_xml(mock_ats_transmissions_response_escaped_xml):
    result = parse_transmissions_from_xml(mock_ats_transmissions_response_escaped_xml)
    assert result == {}


@pytest.mark.asyncio
async def test_get_data_endpoint_response(ats_integration_v2, mock_ats_data_response_xml):
    # Mock httpx response for data endpoint
    async with respx.mock(assert_all_called=True) as ats_api_mock:
        pull_config = PullObservationsConfig(
            data_endpoint='http://test.ats.org/Service1.svc/GetPointsAtsIri/1',
            transmissions_endpoint='http://test.ats.org/Service1.svc/GetAllTransmission/1'
        )
        auth_config = AuthenticateConfig(
            username='test',
            password='test'
        )
        ats_api_mock.get(pull_config.data_endpoint).respond(
            status_code=httpx.codes.OK,
            text=mock_ats_data_response_xml
        )
        response = await get_data_endpoint_response(
            integration_id=str(ats_integration_v2.id),
            config=pull_config,
            auth=auth_config,
            parse_response=False,
        )
        assert response == mock_ats_data_response_xml


def test_parse_data_points_from_xml(mock_ats_data_response_xml, mock_ats_data_parsed):
    result = parse_data_points_from_xml(mock_ats_data_response_xml)
    assert result == mock_ats_data_parsed


def test_parse_data_points_raises_on_invalid_xml(mock_ats_data_response_with_invalid_xml):
    with pytest.raises(ATSBadXMLException):
        parse_data_points_from_xml(mock_ats_data_response_with_invalid_xml)


def test_parse_data_points_from_empty_xml(mock_ats_data_response_no_points_xml):
    result = parse_data_points_from_xml(mock_ats_data_response_no_points_xml)
    assert result == {}


def test_parse_data_points_from_xml_with_single_point(
        mock_ats_data_response_single_point_xml,
        mock_ats_data_single_point_parsed
):
    # First validate the original XML parse a single point as dict
    parsed_xml = xmltodict.parse(mock_ats_data_response_single_point_xml)
    data_xml_tag = parsed_xml["DataSet"].get("diffgr:diffgram", {})
    data = data_xml_tag.get("NewDataSet", {})

    assert isinstance(data.get("Table", None), dict)

    # Then validate after calling parsing method, the single point is converted to a list and is parsed correctly
    result = parse_data_points_from_xml(mock_ats_data_response_single_point_xml)
    assert isinstance(result["052194"], list)
    assert len(result["052194"]) == 1  # Ensure the single item was converted to a list
    assert result == mock_ats_data_single_point_parsed


def test_parse_data_points_from_escaped_xml(mock_ats_data_response_escaped_xml):
    result = parse_data_points_from_xml(mock_ats_data_response_escaped_xml)
    assert result == {}
