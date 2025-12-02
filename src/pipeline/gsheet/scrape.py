import requests


def fetch_csv_data(gsheet_id: str, gid: str) -> bytes:
    url = f"https://docs.google.com/spreadsheets/d/{gsheet_id}/export?format=csv&gid={gid}"
    print(f'Fetching CSV data from URL: {url}')

    response = requests.get(url)
    response.raise_for_status()
    return response.content