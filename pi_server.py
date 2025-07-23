import requests
from requests_ntlm import HttpNtlmAuth

PI_WEB_API = "usgei-piap02.methanex.com/piwebapi"

# Integrated Windows / Kerberos
session = requests.Session()
session.auth = HttpNtlmAuth("+soaS/fW544dPk0e5eoO/Oi2DwAlU709", "dZdQw0vcImsWhYLLr4y9BA==")  # leave out password for full Kerberos SSO
session.verify = True          # keep HTTPS; install the PI Web API SSL cert if needed

# Test call: get a tag by path
path = r"\\USGEIPIDA\SINUSOID"
r = session.get(f"{PI_WEB_API}", params={"path": path})
r.raise_for_status()
point = r.json()
print(point["Name"], point["WebId"])
