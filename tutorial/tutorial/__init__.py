from dagster import (
    AssetSelection,
    Definitions,
    FilesystemIOManager,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_duckdb_pandas import DuckDBPandasIOManager

from . import assets
from .resources import DataGeneratorResource

all_assets = load_assets_from_modules([assets])

# Define a job that will materialize the assets
hackernews_job = define_asset_job(
    "hackernews_job", selection=AssetSelection.all()
)

# ScheduleDefinition: the job it should run and a cron schedule of how
# frequently to run it
hackernews_schedule = ScheduleDefinition(
    job=hackernews_job,
    cron_schedule="0 * * * *",  # every hour
)

io_manager = FilesystemIOManager(
    base_dir="data",  # Path is built relative to where `dagster dev` is run
)

database_io_manager = DuckDBPandasIOManager(database="analytics.hackernews")

datagen = DataGeneratorResource(num_days=365)

defs = Definitions(
    assets=all_assets,
    schedules=[hackernews_schedule],
    resources={
        "io_manager": io_manager,
        "database_io_manager": database_io_manager,
        "hackernews_api": datagen,
    },
)
