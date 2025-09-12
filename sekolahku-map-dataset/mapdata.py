import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import certifi

# -- Load environment variables --
load_dotenv()

GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
SHEET_NAME = os.getenv("GSHEET_URL").strip('"')   # Remove extra quotes
SHEET_TAB = os.getenv("SHEET_TAB").strip('"')     # Remove extra quotes

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION")

# -- Connect to Google Sheets --
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Load Google credentials
creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scope)
client = gspread.authorize(creds) 

# Fetch data from Google Sheets
try:
    sheet = client.open_by_url(SHEET_NAME).worksheet(SHEET_TAB)
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    print(f"✅ Fetched {len(df)} rows from Google Sheets")
except Exception as e:
    print(f"❌ Error fetching data from Google Sheets: {e}")
    exit(1)


df["NOTELEFON"] = df["NOTELEFON"].astype(str)
# Replace "TIADA" with None or an empty string to signify missing phone numbers
df["NOTELEFON"] = df["NOTELEFON"].replace("TIADA", "")

# -- Convert to GeoJSON --
def convert_to_geojson(df, lat_col="KOORDINATYY", lon_col="KOORDINATXX"):
    features = []
    for _, row in df.iterrows():
        if pd.isnull(row[lat_col]) or pd.isnull(row[lon_col]):
            continue
        
        # If phone number is available, prepend "+6" to the number
        if row["NOTELEFON"]:
            phone_number = "+60" + (row["NOTELEFON"])
        else:
            phone_number = ""

        features.append({
            "type": "Feature",
            "properties":{
            "kodsekolah": row["KODSEKOLAH"],
            "namasekolah": row["NAMASEKOLAH"],
            "alamat": {
                "alamatsurat": row["ALAMATSURAT"], 
                "poskodsurat": row["POSKODSURAT"],
                "bandarsurat": row["BANDARSURAT"]
            },
            "jenislabel": row["JENIS/LABEL"],
            "bilsesi": row["BILSESI"],
            "sesi": row["SESI"],
            "notelefon": phone_number,
            "email": row["EMAIL"],
            "dun": row["DUN"],
            "parlimen": row["PARLIMEN"],
            "ppd": row["PPD"],
            "negeri": row["NEGERI"]
            },
            "geometry": {
                "type": "Point",
                "coordinates": [row[lon_col], row[lat_col]]
            }
        })
    return {"type": "FeatureCollection", "features": features}
try:
    geojson_data = convert_to_geojson(df)
    print(f"🌍 Converted to GeoJSON with {len(geojson_data['features'])} features")

except Exception as e:
    print(f"❌ Error converting to GeoJSON: {e}")
    exit(1)

# -- Upload to MongoDB --
try:
    if MONGO_URI.startswith("mongodb+srv://"):
        mongo_client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    else:
        mongo_client = MongoClient(MONGO_URI)

    db = mongo_client[DB_NAME]
    collection = db[COLLECTION_NAME]

# Replace old data
    collection.delete_many({})
    if geojson_data["features"]:
        collection.insert_many(geojson_data["features"])
        print(f"📤 Uploaded {len(geojson_data['features'])} records to MongoDB: {DB_NAME}.{COLLECTION_NAME}")
    else:
        print("⚠️ No features to upload.")

except Exception as e:
    print(f"❌ Error uploading to a MongoDB: {e}")
    exit(1)