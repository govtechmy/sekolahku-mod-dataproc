from dagster import Definitions, load_assets_from_modules
from sekolahku_pipelines import assets, jobs

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[
        jobs.ingestion_job,
        jobs.staging_job,
        jobs.promotion_job,
        jobs.analytics_job,
        jobs.full_pipeline_job,
    ],
)

