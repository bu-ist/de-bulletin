import httpx
import os
import json
from dagster import (
    sensor,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    DefaultSensorStatus,
    define_asset_job,
)
from de_datalake_bulletin_dataload.defs.assets import (
    fetch_export_pages_data,
    fetch_export_media_data,
)


bulletin_raw_job = define_asset_job(
    name="bulletin_raw_job",
    selection=[fetch_export_pages_data, fetch_export_media_data],
    tags={"dagster/asset_group": "bulletin_raw"},
    description="Job to materialize bulletin_raw asset group for Bulletin data lake.",
)


def fetch_latest_modified_date(endpoint: str, config: dict) -> str:
    """Fetch the most recent modified date from Bulletin Wordpress API for an endpoint.

    The sensor parameter defines the query to get the latest modified record. The response
    is compared against the last known modified date stored in the sensor cursor.

    Args:
        endpoint (str): API endpoint to query (e.g., 'pages', 'media').
        config (dict): Configuration dictionary containing base_url and latest_modified_param.

    Returns:
        str: Datetime string of the latest modified date, or '2000-01-01T00:00:00' if fetch fails.
    """
    base_url = config.get("base_url", "")
    sensor_param = config.get("latest_modified_param", "")
    url = f"{base_url}{endpoint}{sensor_param}"

    try:
        response = httpx.get(url, timeout=30.0)
        response.raise_for_status()
        data = response.json()

        if data and len(data) > 0:
            return data[0]["modified"]
    except Exception as e:
        print(f"Error fetching latest modified date for {endpoint}: {e}")

    return "2000-01-01T00:00:00"


@sensor(
    job=bulletin_raw_job,
    minimum_interval_seconds=86400,
    default_status=DefaultSensorStatus.RUNNING,
)
def bulletin_data_sensor(context: SensorEvaluationContext):
    """Check for new or modified content in Bulletin API and trigger assets if found.

    Uses cursor to store last known modified dates for both pages and media endpoints
    in the format 'pages_modified|media_modified'. Runs every 24 hours.

    Args:
        context (SensorEvaluationContext): Dagster SensorEvaluationContext for logging.

    Returns:
        RunRequest: If new data is detected, triggers asset materialization with updated cursor.
        SkipReason: If no new data is detected, skips the run with a reason.
    """
    config_path = os.getenv("CONFIG_PATH", "/app/config/config.json")
    with open(config_path, "r") as f:
        config = json.load(f)

    cursor = context.cursor or "2000-01-01T00:00:00|2000-01-01T00:00:00"
    last_pages_modified, last_media_modified = cursor.split("|")

    latest_pages = fetch_latest_modified_date("pages", config)
    latest_media = fetch_latest_modified_date("media", config)

    context.log.info(
        f"Last known - Pages: {last_pages_modified}, Media: {last_media_modified}"
    )
    context.log.info(f"Current latest - Pages: {latest_pages}, Media: {latest_media}")

    has_new_data = (latest_pages > last_pages_modified) or (
        latest_media > last_media_modified
    )

    if has_new_data:
        context.log.info("New data detected! Triggering asset materialization.")

        context.update_cursor(f"{latest_pages}|{latest_media}")

        return RunRequest(
            run_key=f"{latest_pages}_{latest_media}",
            run_config={
                "ops": {
                    "bulletin_raw_pages": {
                        "config": {
                            "upload_to_s3": False,
                            "full_refresh": False,
                        }
                    },
                    "bulletin_raw_media": {
                        "config": {
                            "upload_to_s3": False,
                            "full_refresh": False,
                        }
                    },
                }
            },
            tags={
                "source": "bulletin_data_sensor",
                "dagster-k8s/config": {
                    "container_config": {
                        "image": "de-bulletin:latest",
                        "resources": {
                            "requests": {"cpu": "500m", "memory": "1Gi"},
                            "limits": {"cpu": "1000m", "memory": "2Gi"},
                        },
                    },
                    "pod_spec_config": {"imagePullSecrets": [{"name": "ghcr-creds"}]},
                },
            },
        )

    context.log.info("No new data detected. Skipping run.")
    return SkipReason(
        f"No new data. Latest - Pages: {latest_pages}, Media: {latest_media}"
    )
