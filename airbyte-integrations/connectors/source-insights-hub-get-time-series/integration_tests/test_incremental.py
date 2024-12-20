import json
import pytest
from airbyte_cdk.models import SyncMode, DestinationSyncMode
from source_insights_hub_get_time_series.source import SourceInsightsHubGetTimeSeries


@pytest.fixture(name="config")
def config_fixture():
    with open("secrets/config.json", "r") as f:
        return json.loads(f.read())


@pytest.fixture(name="configured_catalog")
def configured_catalog_fixture():
    return {
        "streams": [
            {
                "stream": {
                    "name": "machine_metrics",
                    "json_schema": {},
                    "supported_sync_modes": ["incremental"],
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_time"]
                },
                "sync_mode": "incremental",
                "cursor_field": ["_time"],
                "destination_sync_mode": "append"
            }
        ]
    }


def test_incremental_sync_state(config, configured_catalog):
    source = SourceInsightsHubGetTimeSeries()

    # Read with initial state
    with open("integration_tests/sample_state.json", "r") as f:
        state = json.loads(f.read())

    records = list(source.read(
        logger=None,
        config=config,
        catalog=configured_catalog,
        state=state
    ))

    # Verify records are filtered based on state
    for record in records:
        if record.type == "RECORD":
            assert record.record.data["_time"] >= state["machine_metrics"]["_time"]
        elif record.type == "STATE":
            assert record.state.data["machine_metrics"]["_time"] >= state["machine_metrics"]["_time"]
