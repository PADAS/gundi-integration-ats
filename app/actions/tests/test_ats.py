from app.actions.client import PullObservationsTransmissionsResponse
from app.actions.handlers import extract_gmt_offsets


TRANSMISSIONS_RESPONSE = [
    {
        '@diffgr:id': 'Table1',
        '@msdata:rowOrder': '0',
        'DateSent': '2024-10-31T20:24:34.67+00:00',
        'CollarSerialNum': '053506',
        'NumberFixes': '0',
        'BattVoltage': '7.056',
        'Mortality': 'No',
        'BreakOff': 'No',
        'SatErrors': '5',
        'YearBase': '20',
        'DayBase': '0',
        'GmtOffset': '0',
        'Event': 'None,None,None',
        'evCondition': 'None,None,None',
        'LowBattVoltage': 'false'
    },
    {
        '@diffgr:id': 'Table2',
        '@msdata:rowOrder': '1',
        'DateSent': '2024-10-30T20:36:19.653+06:00',
        'CollarSerialNum': '053507',
        'NumberFixes': '0',
        'BattVoltage': '6.984',
        'Mortality': 'No',
        'BreakOff': 'No',
        'SatErrors': '6',
        'YearBase': '20',
        'DayBase': '0',
        'GmtOffset': '6',
        'Event': 'None,None,None',
        'evCondition': 'None,None,None',
        'LowBattVoltage': 'false'
    },
    {
        '@diffgr:id': 'Table3',
        '@msdata:rowOrder': '2',
        'DateSent': '2024-10-27T20:16:09.653+07:00',
        'CollarSerialNum': '053508',
        'NumberFixes': '0',
        'BattVoltage': '6.984',
        'Mortality': 'No',
        'BreakOff': 'No',
        'SatErrors': '6',
        'YearBase': '20',
        'DayBase': '0',
        'GmtOffset': '7',
        'Event': 'None,None,None',
        'evCondition': 'None,None,None',
        'LowBattVoltage': 'false'
    }
]


def test_extract_gmt_offsets_from_transmissions():
    integration_id = "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0"
    transmissions = PullObservationsTransmissionsResponse.parse_obj({"transmissions": TRANSMISSIONS_RESPONSE})
    test_gmt_offsets = {i.collar_serial_num: i.gmt_offset for i in transmissions.transmissions}
    extracted_gmt_offsets = extract_gmt_offsets(transmissions.transmissions, integration_id)

    assert test_gmt_offsets == extracted_gmt_offsets

def test_extract_gmt_offsets_with_no_transmissions(caplog):
    integration_id = "779ff3ab-5589-4f4c-9e0a-ae8d6c9edff0"
    transmissions = {}
    extracted_gmt_offsets = extract_gmt_offsets(transmissions, integration_id)

    # No offsets extracted
    assert extracted_gmt_offsets == {}

    log_to_test = f'No transmissions were pulled for integration ID: {integration_id}.'
    assert log_to_test in [r.message for r in caplog.records]
