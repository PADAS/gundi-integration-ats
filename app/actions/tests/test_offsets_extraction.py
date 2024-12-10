from app.actions.ats_client import PullObservationsTransmissionsResponse
from app.actions.handlers import extract_gmt_offsets


def test_extract_gmt_offsets_from_transmissions(ats_integration_v2, mock_ats_transmissions_parsed):
    integration_id = str(ats_integration_v2.id)
    transmissions = mock_ats_transmissions_parsed
    expected_gmt_offsets = {i.collar_serial_num: i.gmt_offset for i in transmissions}

    extracted_gmt_offsets = extract_gmt_offsets(transmissions, integration_id)

    assert extracted_gmt_offsets == expected_gmt_offsets


def test_extract_gmt_offsets_from_transmissions_with_invalid_offsets(
        ats_integration_v2, mock_ats_transmissions_with_invalid_offsets_parsed
):
    integration_id = str(ats_integration_v2.id)
    transmissions = mock_ats_transmissions_with_invalid_offsets_parsed
    expected_gmt_offsets = {
        i.collar_serial_num: i.gmt_offset
        for i in transmissions
        if i.gmt_offset is not None and -12 <= i.gmt_offset <= 14
    }

    extracted_gmt_offsets = extract_gmt_offsets(transmissions, integration_id)

    assert extracted_gmt_offsets == expected_gmt_offsets


def test_extract_gmt_offsets_with_no_transmissions(ats_integration_v2, caplog):
    integration_id = str(ats_integration_v2.id)
    transmissions = {}
    extracted_gmt_offsets = extract_gmt_offsets(transmissions, integration_id)

    # No offsets extracted
    assert extracted_gmt_offsets == {}

    log_to_test = f'No transmissions were pulled for integration ID: {integration_id}.'
    assert log_to_test in [r.message for r in caplog.records]
