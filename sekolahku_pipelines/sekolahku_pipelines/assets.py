import gspread
from google.oauth2.service_account import Credentials
from pymongo import MongoClient
from dagster import asset, multi_asset, AssetOut, Output, MetadataValue, Failure
import os
from dotenv import load_dotenv
from sekolahku_pipelines.rules import load_rules


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
GSHEET_ID = os.getenv("GSHEET_ID")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME")

RAW_COLLECTION = os.getenv("RAW_COLLECTION")
STG_COLLECTION= os.getenv("STG_COLLECTION")
PROD_COLLECTION= os.getenv("PROD_COLLECTION")

rules = load_rules()

# ---------- DB Helper ----------
def get_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

# ---------- GSheet Helper ----------
def get_gsheet_client():
    creds = Credentials.from_service_account_file(
        "service_account.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

# ---------- Assets ----------
@asset
def gsheet_data():
    client = get_gsheet_client()
    sheet = client.open_by_key(GSHEET_ID).worksheet(WORKSHEET_NAME)
    return sheet.get_all_records()

@asset(deps=["gsheet_data"])
def gsheet_schema_validated(context, gsheet_data):
    if not gsheet_data:
        raise Failure(description="No rows found in Google Sheet")

    required_cols = rules.required_columns()
    header = list(gsheet_data[0].keys())
    missing = [c for c in required_cols if c not in header]

    if missing:
        context.log.error(f"Missing required columns: {missing}")
        raise Failure(description="Schema validation failed")

    return gsheet_data

@asset(deps=["gsheet_schema_validated"])
def raw_collection(context, gsheet_schema_validated):
    db = get_db()
    coll = db[RAW_COLLECTION]
    coll.delete_many({})
    for r in gsheet_schema_validated:
        r["run_id"] = context.run_id
    coll.insert_many(gsheet_schema_validated)
    return f"{len(gsheet_schema_validated)} rows loaded"

@asset(deps=[raw_collection])
def stg_collection(context):
    db = get_db()
    raw = db[RAW_COLLECTION].find()
    stg = db[STG_COLLECTION]
    stg.delete_many({})

    transformed = [rules.transform_row(r, context) for r in raw]
    if transformed:
        stg.insert_many(transformed)

    return f"{len(transformed)} docs transformed"

@multi_asset(
    outs={
        "validation_passed": AssetOut(is_required=True),
        "validation_errors": AssetOut(is_required=True),
        "validation_warnings": AssetOut(is_required=True),
    },
    deps=["stg_collection"]
)
def validation_checks(context):
    """
    Run validation with 3 outputs:
       - validation_passed (bool)
       - validation_errors (int)
       - validation_warnings (int)
    """
    db = get_db()
    stg = list(db[STG_COLLECTION].find())

    if not stg:
        context.log.error("Validation failed: no docs in staging")
        yield Output(False, "validation_passed", metadata={"errors": MetadataValue.text("No documents found")})
        yield Output(1, "validation_errors")
        yield Output(0, "validation_warnings")
        return

    error_count, warning_count = 0, 0
    error_ids, warning_ids = [], []

    for doc in stg:
        _id = doc.get("_id", "(no id)")

        errors, warnings = rules.validate_row(doc, context)

        if errors:
            error_count += len(errors)
            error_ids.append(_id)
            for e in errors:
                context.log.error(f"{_id}: {e}")

        if warnings:
            warning_count += len(warnings)
            warning_ids.append(_id)
            for w in warnings:
                context.log.warning(f"_id: {_id}: {w}")

    passed = error_count == 0

    yield Output(
        passed,
        "validation_passed",
        metadata={
            "error_count": error_count,
            "warning_count": warning_count,
            "sample_errors": MetadataValue.json(error_ids[:10]),
            "sample_warnings": MetadataValue.json(warning_ids[:10]),},)
    yield Output(error_count, "validation_errors", metadata={"error_count": error_count})
    yield Output(warning_count, "validation_warnings", metadata={"warning_count": warning_count})


@asset(deps=["validation_passed"])
def prod_collection(context, validation_passed):
    if not validation_passed:
        return "Skipped promotion"

    db = get_db()
    db[STG_COLLECTION].aggregate([
        {"$match": {}},
        {"$out": PROD_COLLECTION}
    ])
    count = db[PROD_COLLECTION].count_documents({})
    return f"{count} docs promoted"


@asset(deps=["prod_collection"])
def analytics_enrolment_by_state(context):
    """Aggregated enrolment by state"""
    db = get_db()
    result = db[PROD_COLLECTION].aggregate([
        {
            "$group": {
                "_id": "$administration.negeri",
                "total_schools": {"$sum": 1},
                "total_enrolment": {"$sum": "$enrolment.enrolmen"}
            }
        },
        {"$out": "analytics_enrolment_by_state"}
    ])

    context.log.info("Aggregated enrolment by state into analytics_enrolment_by_state")
    return "analytics_enrolment_by_state updated"
