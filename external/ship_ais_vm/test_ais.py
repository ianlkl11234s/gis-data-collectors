"""Quick connectivity test for ship_ais API from HiCloud VM."""
import socket
import time
import urllib.request

HOST = "mpbais.motcmpb.gov.tw"
URL = f"https://{HOST}/aismpb/tools/geojsonais.ashx"

print("DNS:", socket.gethostbyname(HOST))

t = time.time()
s = socket.create_connection((HOST, 443), timeout=10)
print(f"TCP OK in {time.time()-t:.2f}s")
s.close()

req = urllib.request.Request(
    URL,
    headers={"User-Agent": "Mozilla/5.0", "Referer": f"https://{HOST}/aismpb/"},
)
r = urllib.request.urlopen(req, timeout=20)
body = r.read()
print(f"HTTP: {r.status}, {len(body)} bytes")

ip = urllib.request.urlopen("https://api.ipify.org", timeout=10).read().decode()
print("egress IP:", ip)
