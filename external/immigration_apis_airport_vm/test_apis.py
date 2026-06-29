"""Quick connectivity test for immigration APIS from HiCloud VM."""
import socket
import time
import urllib.request
import ssl

HOST = "opendata.immigration.gov.tw"
URL = f"https://{HOST}/APIS/TPE1"

print(f"DNS: {socket.gethostbyname(HOST)}")

t = time.time()
s = socket.create_connection((HOST, 443), timeout=10)
print(f"TCP OK in {time.time()-t:.2f}s")
s.close()

# 忽略 SSL 驗證（部分政府站憑證鏈不完整）
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

req = urllib.request.Request(
    URL,
    headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://opendata.immigration.gov.tw/",
        "Origin": "https://opendata.immigration.gov.tw",
    },
)
t = time.time()
r = urllib.request.urlopen(req, timeout=20, context=ctx)
body = r.read()
print(f"HTTP {r.status} in {time.time()-t:.2f}s, body {len(body):,} B")
if body[:1] == b"[":
    print(f"JSON array OK, first 200 chars: {body[:200].decode('utf-8','replace')}")
