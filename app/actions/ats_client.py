import logging
import httpx
import pydantic
import xmltodict
import stamina

from datetime import datetime, timedelta
from xml.parsers.expat import ExpatError
from typing import List, Optional


logger = logging.getLogger(__name__)


# Pydantic models
class DataResponse(pydantic.BaseModel):
    ats_serial_num: str = pydantic.Field(..., alias="AtsSerialNum")
    longitude: float = pydantic.Field(None, alias='Longitude', ge=-180.0, le=360.0)
    latitude: float = pydantic.Field(None, alias='Latitude', ge=-90.0, le=90.0)
    date_year_and_julian: datetime = pydantic.Field(..., alias="DateYearAndJulian")
    num_sats: Optional[str] = pydantic.Field(None, alias='NumSats')
    hdop: Optional[str] = pydantic.Field(None, alias='Hdop')
    fix_time: Optional[str] = pydantic.Field(None, alias='FixTime')
    dimension: Optional[str] = pydantic.Field(None, alias='Dimension')
    activity: Optional[str] = pydantic.Field(None, alias='Activity')
    temperature: Optional[str] = pydantic.Field(None, alias='Temperature')
    mortality: Optional[bool] = pydantic.Field(None, alias='Mortality')
    low_batt_voltage: Optional[bool] = pydantic.Field(None, alias='LowBattVoltage')

    class Config:
        allow_population_by_field_name = True


class TransmissionsResponse(pydantic.BaseModel):
    date_sent: datetime = pydantic.Field(..., alias="DateSent")
    collar_serial_num: str = pydantic.Field(..., alias="CollarSerialNum")
    number_fixes: Optional[int] = pydantic.Field(None, alias='NumberFixes')
    batt_voltage: Optional[float] = pydantic.Field(None, alias='BattVoltage')
    mortality: Optional[str] = pydantic.Field(None, alias='Mortality')
    break_off: Optional[str] = pydantic.Field(None, alias='BreakOff')
    sat_errors: Optional[str] = pydantic.Field(None, alias='SatErrors')
    year_base: Optional[str] = pydantic.Field(None, alias='YearBase')
    day_base: Optional[str] = pydantic.Field(None, alias='DayBase')
    gmt_offset: Optional[int] = pydantic.Field(None, alias='GmtOffset')
    low_batt_voltage: Optional[bool] = pydantic.Field(None, alias='LowBattVoltage')

    class Config:
        allow_population_by_field_name = True


class PullObservationsDataResponse(pydantic.BaseModel):
    vehicles: List[DataResponse]


class PullObservationsTransmissionsResponse(pydantic.BaseModel):
    transmissions: List[TransmissionsResponse]


class ATSBadXMLException(Exception):
    def __init__(self, error: Exception, message: str, status_code=422):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


def closest_transmission(transmissions, test_date):
    sorted_list = sorted([t.DateSent for t in transmissions])
    previous_date = sorted_list[-1]
    for date in sorted_list:
        if date >= test_date:
            if abs((date - test_date).days) < abs((previous_date - test_date).days):
                return [t for t in transmissions if t.DateSent == date][0]
            else:
                return [t for t in transmissions if t.DateSent == previous_date][0]
        previous_date = date
    return [t for t in transmissions if t.DateSent == sorted_list[-1]][0]


def parse_data_points_from_xml(xml):
    result = {}
    try:
        logger.info(f"-- Parsing response (xmltodict) --")
        parsed_xml = xmltodict.parse(xml)
    except (xmltodict.ParsingInterrupted, ExpatError) as e:
        msg = f"Invalid XML."
        logger.exception(msg)
        raise ATSBadXMLException(message=msg, error=e)

    try:
        data_xml_tag = parsed_xml["DataSet"].get("diffgr:diffgram", {})
        data = data_xml_tag.get("NewDataSet", {})
    except KeyError as e:
        msg = f"Dataset or NewDataSet tag not found in XML."
        logger.exception(msg)
        raise ATSBadXMLException(message=msg, error=e)

    if data:
        # Add a validator for checking if "Table" variable within data is a list, if not, convert it to a list
        if not isinstance(data.get("Table", None), list):
            data["Table"] = [data.get("Table", {})]

        try:
            parsed_response = PullObservationsDataResponse.parse_obj(
                {"vehicles": data.get("Table", [])}
            )
        except pydantic.ValidationError as e:
            msg = f"Error building 'PullObservationsTransmissionsResponse'."
            logger.exception(msg)
            raise ATSBadXMLException(message=msg, error=e)

        response_per_device = {}
        # save data points per serial num
        serial_nums = set([v.ats_serial_num for v in parsed_response.vehicles])
        for serial_num in serial_nums:
            response_per_device[serial_num] = [
                point for point in parsed_response.vehicles if serial_num == point.ats_serial_num
            ]
            logger.info(
                f"-- Extracted {len(response_per_device[serial_num])} data points for device {serial_num} --")
        result = response_per_device

    return result


@stamina.retry(on=httpx.HTTPError, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_data_endpoint_response(integration_id, config, auth, parse_response=False):
    endpoint = config.data_endpoint
    async with httpx.AsyncClient(timeout=120) as session:
        logger.info(f"-- Getting data points for integration ID: {integration_id} Endpoint: {endpoint} --")
        response = await session.get(endpoint, auth=(auth.username, auth.password.get_secret_value()))
        response.raise_for_status()
        if parse_response:
            return parse_data_points_from_xml(xml=response.text)
        else:
            return response.text


def parse_transmissions_from_xml(xml):
    result = {}
    try:
        logger.info(f"-- Parsing transmissions XML (xmltodict) --")
        parsed_xml = xmltodict.parse(xml)
    except (xmltodict.ParsingInterrupted, ExpatError) as e:
        msg = f"Invalid XML."
        logger.exception(msg)
        raise ATSBadXMLException(message=msg, error=e)

    try:
        transmissions_xml_tag = parsed_xml["DataSet"].get("diffgr:diffgram", {})
        transmissions = transmissions_xml_tag.get("NewDataSet", {})
    except KeyError as e:
        msg = f"Dataset or NewDataSet tag not found in XML."
        logger.exception(msg)
        raise ATSBadXMLException(message=msg, error=e)

    if transmissions:
        # Add a validator for checking if "Table" variable within transmissions is a list, if not, convert it to a list
        if not isinstance(transmissions.get("Table", None), list):
            transmissions["Table"] = [transmissions.get("Table", {})]

        try:
            parsed_response = PullObservationsTransmissionsResponse.parse_obj(
                {"transmissions": transmissions.get("Table", [])}
            )
        except pydantic.ValidationError as e:
            msg = f"Error building 'PullObservationsTransmissionsResponse'."
            logger.exception(msg)
            raise ATSBadXMLException(message=msg, error=e)
        else:
            result = parsed_response.transmissions
    return result


@stamina.retry(on=httpx.HTTPError, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_transmissions_endpoint_response(integration_id, config, auth, parse_response=False):
    endpoint = config.transmissions_endpoint
    async with httpx.AsyncClient(timeout=120) as session:
        logger.info(f"-- Getting transmissions for integration ID: {integration_id} Endpoint: {endpoint} --")
        response = await session.get(endpoint, auth=(auth.username, auth.password.get_secret_value()))
        response.raise_for_status()
        if parse_response:
            return parse_transmissions_from_xml(xml=response.text)
        else:
            return response.text
