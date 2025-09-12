from dagster import define_asset_job
from sekolahku_pipelines import assets

# Ingestion job (from GSheet or CSV to raw collection)
ingestion_job = define_asset_job(
    name="ingestion_job",
    selection=[assets.gsheet_data, assets.gsheet_schema_validated, assets.raw_collection]
)

# Staging job (transform raw -> staging)
staging_job = define_asset_job(
    name="staging_job",
    selection=[assets.stg_collection]
)

# Validation job (run rules on staging)
validation_job = define_asset_job(
    name="validation_job",
    selection=[assets.validation_checks]
)

# Promotion job (promote staging -> production)
promotion_job = define_asset_job(
    name="promotion_job",
    selection=[assets.prod_collection]
)

# Analytics job (aggregations on production)
analytics_job = define_asset_job(
    name="analytics_job",
    selection=[assets.analytics_enrolment_by_state]
)

# Full pipeline (end-to-end)
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection="*"
)
