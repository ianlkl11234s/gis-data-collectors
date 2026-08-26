"""Manual Taiwan-egress preflight; no database writes."""
import requests

URL = "https://isohe.ihmt.gov.tw/opendata/Wave?port=TP&format=JSON"

if __name__ == "__main__":
    response = requests.get(URL, timeout=30)
    response.raise_for_status()
    print(f"ISOHE TP wave: HTTP {response.status_code}, {len(response.content)} bytes")
