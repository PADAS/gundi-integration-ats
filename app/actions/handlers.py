import datetime
from enum import Enum
import httpx
import logging
import stamina
import aiofiles
import app.actions.ats_client as client
import app.services.gundi as gundi_tools
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.state import IntegrationStateManager
from app.services.file_storage import CloudFileStorage
from .configurations import PullObservationsConfig, ProcessObservationsConfig, get_auth_config, get_pull_config

logger = logging.getLogger(__name__)


state_manager = IntegrationStateManager()
file_storage = CloudFileStorage()


PENDING_FILES = "ats_pending_files"
PROCESSED_FILES = "ats_processed_files"


class FileStatus(Enum):
    PENDING = "pending"
    PROCESSED = "processed"


def extract_gmt_offsets(transmissions, integration_id):
    if transmissions:
        accumulator = {}
        for item in transmissions:
            accumulator.setdefault(item.collar_serial_num, item.gmt_offset)
        return accumulator
    else:
        logger.warning(f"No transmissions were pulled for integration ID: {integration_id}.")
        logger.warning(f"-- Setting GMT offset to 0 for devices in integration ID: {integration_id}.")
        return {}


async def filter_and_transform(serial_num, vehicles, gmt_offset, integration_id, action_id):
    transformed_data = []
    main_data = ["ats_serial_num", "date_year_and_julian", "latitude", "longitude"]

    # check and log invalid GMT offset
    if abs(gmt_offset) > 24:
        message = f"GMT offset invalid for device '{serial_num}' value '{gmt_offset}'. Defaulting to UTC."
        logger.warning(message)
        await log_action_activity(
            integration_id=integration_id,
            action_id=action_id,
            title=message,
            level="WARNING"
        )
        gmt_offset = 0

    for vehicle in vehicles:
        # Get GmtOffset for this device
        time_delta = datetime.timedelta(hours=gmt_offset)
        timezone_object = datetime.timezone(time_delta)

        date_year_and_julian_with_tz = vehicle.date_year_and_julian.replace(tzinfo=timezone_object)

        vehicle.date_year_and_julian = date_year_and_julian_with_tz

        data = {
            "source": vehicle.ats_serial_num,
            "source_name": vehicle.ats_serial_num,
            'type': 'tracking-device',
            "recorded_at": vehicle.date_year_and_julian,
            "location": {
                "lat": vehicle.latitude,
                "lon": vehicle.longitude
            },
            "additional": {
                key: value for key, value in vehicle.dict().items()
                if key not in main_data and value is not None
            }
        }
        transformed_data.append(data)

    return transformed_data


async def retrieve_transmissions(integration_id, auth_config, pull_config, file_prefix):
    logger.info(f"Retrieving transmissions for integration '{integration_id}'...")
    transmissions_raw_xml = await client.get_transmissions_endpoint_response(
        integration_id=integration_id,
        config=pull_config,
        auth=auth_config
    )

    transmissions_file_name = f"{file_prefix}_transmissions.xml"
    logger.info(f"Saving transmissions for integration '{integration_id}' to file '{transmissions_file_name}'...")
    async with aiofiles.open(f"/tmp/{transmissions_file_name}", "w") as f:
        await f.write(transmissions_raw_xml)

    logger.info(f"Uploading transmissions file {transmissions_file_name} to cloud storage...")
    await file_storage.upload_file(
        integration_id=integration_id,
        local_file_path=f"/tmp/{transmissions_file_name}",
        destination_blob_name=transmissions_file_name,
        metadata={
            "integration_id": integration_id,
            "ats_username": auth_config.username,
            "status": "pending"
        }
    )

    logger.info(f"Transmissions file {transmissions_file_name} saved.")
    return transmissions_file_name


async def retrieve_data_points(integration_id, auth_config, pull_config, file_prefix):
    logger.info(f"Retrieving data points for integration '{integration_id}'...")
    data_points_raw_xml = await client.get_data_endpoint_response(
        integration_id=integration_id,
        config=pull_config,
        auth=auth_config
    )

    data_points_file_name = f"{file_prefix}_data_points.xml"
    logger.info(f"Saving data points for integration '{integration_id}' to file '{data_points_file_name}'...")
    async with aiofiles.open(f"/tmp/{data_points_file_name}", "w") as f:
        await f.write(data_points_raw_xml)

    logger.info(f"Uploading data points file {data_points_file_name} to cloud storage...")
    await file_storage.upload_file(
        integration_id=integration_id,
        local_file_path=f"/tmp/{data_points_file_name}",
        destination_blob_name=data_points_file_name,
        metadata={
            "integration_id": integration_id,
            "ats_username": auth_config.username,
            "status": "pending"
        }
    )

    # Add it to the list of pending files to be processed
    await state_manager.group_add(
        group_name=PENDING_FILES,
        values=[data_points_file_name]
    )
    logger.info(f"Data points file {data_points_file_name} saved.")
    return data_points_file_name


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(
        f"Executing pull_observations action with integration {integration} and action_config {action_config}..."
    )
    integration_id = str(integration.id)
    auth_config = get_auth_config(integration)
    pull_config = action_config
    timestamp = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y%m%d%H%M%S%f")
    file_prefix = f"{timestamp}_{integration_id}"
    transmissions_file = await retrieve_transmissions(
        integration_id=integration_id,
        auth_config=auth_config,
        pull_config=pull_config,
        file_prefix=file_prefix
    )
    data_points_file = await retrieve_data_points(
        integration_id=integration_id,
        auth_config=auth_config,
        pull_config=pull_config,
        file_prefix=file_prefix
    )
    logger.info(f"-- Observations pulled with success for integration ID: {str(integration.id)}.")

    return {"transmissions_file": transmissions_file, "data_points_file": data_points_file}


async def process_data_file(file_name, integration, process_config):
    logger.info(f"Processing data file {file_name} for integration {integration}...")
    transmissions = {}
    data_points_per_device = {}
    observations_processed = 0
    integration_id = str(integration.id)

    logger.info(f"Downloading data file {file_name} from cloud storage...")
    local_data_file_path = f"/tmp/{file_name}"
    await file_storage.download_file(
        integration_id=integration_id,
        source_blob_name=file_name,
        destination_file_path=local_data_file_path
    )
    logger.info(f"Data file {file_name} downloaded.")

    # Try to get the related transmissions file
    timestamp, integration_id, *_ = file_name.split("_")
    transmissions_file_name = f"{timestamp}_{integration_id}_transmissions.xml"
    local_transmissions_file_path = f"/tmp/{transmissions_file_name}"
    try:
        await file_storage.download_file(
            integration_id=integration_id,
            source_blob_name=transmissions_file_name,
            destination_file_path=local_transmissions_file_path
        )
    except httpx.HTTPStatusError as e:
        msg = f"Error downloading transmissions file {transmissions_file_name}."
        logger.warning(msg)
        await log_action_activity(
            integration_id=integration_id,
            action_id="process_observations",
            title=msg,
            level="WARNING"
        )
    logger.info(f"Transmissions file {transmissions_file_name} downloaded.")

    # Try to parse the transmissions file to get tz offsets
    async with aiofiles.open(local_transmissions_file_path, "r") as f:
        transmissions_xml_content = await f.read()
        try:
            transmissions = client.parse_transmissions_from_xml(xml=transmissions_xml_content)
        except Exception as e:
            msg = f"Error parsing '{transmissions_file_name}': {e}. Integration ID: {integration_id}."
            logger.exception(msg)
            await log_action_activity(
                integration_id=integration_id,
                action_id="process_observations",
                title=msg,
                level="WARNING"
            )

    # Extract GMT offsets from transmissions (if possible)
    gmt_offsets = extract_gmt_offsets(transmissions, integration.id)
    logger.info(f"-- Integration ID: {str(integration.id)}, GMT offsets: {gmt_offsets} --")

    logger.info(f"Processing data points from file {file_name}...")
    async with aiofiles.open(local_data_file_path, "r") as f:
        data_points_xml_content = await f.read()
        try:
            data_points_per_device = client.parse_data_points_from_xml(xml=data_points_xml_content)
        except Exception as e:
            msg = f"Error parsing '{file_name}': {e}. Integration ID: {integration_id}."
            logger.exception(msg)
            await log_action_activity(
                integration_id=integration_id,
                action_id="process_observations",
                title=msg,
                level="ERROR"
            )
            raise e

    if not data_points_per_device:
        msg = f"No data points were extracted from '{file_name}'. Integration ID: {integration_id}."
        logger.warning(msg)

    for serial_num, data_points in data_points_per_device.items():
        logger.info(f"Processing data points for device {serial_num}, integration {integration_id}...")
        transformed_data = await filter_and_transform(
            serial_num,
            data_points,
            gmt_offsets.get(serial_num, 0),
            str(integration.id),
            "pull_observations"
        )

        if transformed_data:
            # Send transformed data to Sensors API V2
            def generate_batches(iterable, n=process_config.observations_per_request):
                for i in range(0, len(iterable), n):
                    yield iterable[i: i + n]

            for i, batch in enumerate(generate_batches(transformed_data)):
                logger.info(
                    f'Sending observations batch #{i}: {len(batch)} observations. Device: {serial_num}'
                )
                await gundi_tools.send_observations_to_gundi(
                    observations=batch,
                    integration_id=integration.id
                )
                observations_processed += len(batch)
        else:
            message = f"No observations after transformation for device {serial_num}, integration {integration_id}."
            logger.warning(message)

    # Set the file status as processed
    await state_manager.group_move(
        from_group=PENDING_FILES,
        to_group=PROCESSED_FILES,
        values=[file_name]
    )
    # Update metadata to see it in the gcp console
    await file_storage.update_file_metadata(
        integration_id=integration_id,
        blob_name=file_name,
        metadata={"status": FileStatus.PROCESSED.value}
    )
    logger.info(f"Data file {file_name} processed.")
    await file_storage.delete_file(integration_id=integration_id, blob_name=file_name)
    logger.info(f"Data file {file_name} deleted.")
    await file_storage.delete_file(integration_id=integration_id, blob_name=transmissions_file_name)
    logger.info(f"Transmissions file {file_name} deleted.")
    return observations_processed


@activity_logger()
async def action_process_observations(integration, action_config: ProcessObservationsConfig):
    logger.info(f"Executing process_observations action with integration {integration} and action_config {action_config}...")
    observations_processed = 0
    integration_id = str(integration.id)
    pending_files = await state_manager.group_get(PENDING_FILES)
    pending_files_for_integration = [
        file_name for file_name in pending_files if file_name.endswith(f"{integration_id}_data_points.xml")
    ]
    for file_name in pending_files_for_integration:
        try:
            observations_processed += await process_data_file(
                file_name=file_name,
                integration=integration,
                process_config=action_config
            )
        except Exception as e:
            msg = f"Error processing data file {file_name} for integration {integration} (skipped): {e}."
            logger.exception(msg)
            await log_action_activity(  # Log the error so the connection is flagged as unhealthy
                integration_id=integration_id,
                action_id="process_observations",
                title=msg,
                level="ERROR"
            )
            continue  # Keep processing as many files as possible
    logger.info(f"-- Observations processed with success for integration '{integration_id}'.")
    return {'observations_processed': observations_processed}
