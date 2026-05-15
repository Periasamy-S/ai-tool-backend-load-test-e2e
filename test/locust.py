"""
Locust load testing script for EU-UserApp.
 
Usage:
locust -f locustfile.py --host http://localhost:8000
 
Or with specific options:
locust -f locustfile.py \
--host http://localhost:8000 \
--users 100 \
--spawn-rate 10 \
--run-time 5m \
--headless
 
Web UI:
Open http://localhost:8089 when running without --headless
 
Encryption:
This script supports dynamic payload encryption using RSA + AES-GCM.
Encrypted endpoints will automatically:
1. Try environment variables (e.g., LOCUST_LOGIN_KEY, LOCUST_LOGIN_DATA)
2. Fall back to hardcoded defaults (DEFAULT_ENCRYPTED_PAYLOADS)
3. Generate fresh encrypted payloads dynamically if needed
 
"""
 
import time
import random
import string
import os
import base64
from typing import Dict, Any
import requests
import re
 
from locust import HttpUser, task, between, events
from loguru import logger
import json
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
 
# ==================== Encryption Configuration ====================
PUBLIC_KEY = "
 
# Configure logging
logger.add(
"locustfile.log",
level="DEBUG",
format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
rotation="500 MB",
)
 
API_PREFIX = os.getenv("LOCUST_API_PREFIX", "").rstrip("/")
X_API_CLIENT = os.getenv(
"LOCUST_X_API_CLIENT", "CZgbPYnmcj5iyEH9tg0GYvB4lm9gGQ9qs6jQwllV"
)
DEFAULT_DEVICE_ID = os.getenv(
"LOCUST_DEVICE_ID", "oje630e0-20d0-46e2-80b8-cbf6f2b393b4"
)
DEFAULT_PLATFORM = os.getenv("LOCUST_PLATFORM", "web")
DEFAULT_COUNTRY = os.getenv("LOCUST_COUNTRY", "US")
DEFAULT_APP_VERSION = os.getenv("LOCUST_APP_VERSION", "1.0.0")
LOAD_TEST_BYPASS_SECRET = os.getenv("LOCUST_LOAD_TEST_BYPASS_SECRET", "")
DEFAULT_AUTH_TOKEN = os.getenv(
"LOCUST_AUTH_TOKEN",
"eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Inl2U1LXFFbzExZjUwY1FsWWtKV3hndm9OWSJ9.eyJhdWQiOiJlZTdhMzA3Yy1jZTYxLTQzY2YtOTdlOC1hZGFjYjQ1NmMzNDgiLCJleHAiOjE3NzcxMzUyNTgsImlhdCI6MTc3NDU0MzI1OCwiaXNzIjoiaHR0cDovLzM0LjQ3LjIxNC4xMTE6OTAxMS8iLCJzdWIiOiIxZjNiNmQyNi03YWY2LTQ5YjAtOGIzZi03NTdhMmIyYzE5YTYiLCJqdGkiOiI3NmM0MzAyOS04NGIxLTQ5OTktOTdhNi02NjBhMmI2OWQ4OTQiLCJyb2xlcyI6W10sImRldmljZV9pZCI6IjA4Yzg4YjcxLTBlOWEtNDk0Ni05MWM1LWMxOWI2ZDM2MjhlYiIsInR5cGUiOiJyZWZyZXNoIiwiYW1yIjpbIm5vbmUiXX0.PGeKNCcgHpRrPB08OGKNG0Xe165FFWNvbaIC7KXZ06UcidPhsZVDmC_iz-TWEwegyCnynRmabvOrz4zJeTYN_tmXy5kffmsl-gxOwL3W_GoK4ZIBxtxrqm03D_fDtFCA6fYtEFc3PXaTp1QcNM3aQB8X_HbT0U527CDF8S2spXGMpAehm-pMJ1ITfufO7nLBwCXJWWAqW0EhC7tvB4WVXSDQ9iHdDw7jbxFTrDfNMrKwuXA_VmeCIUPMbYHHLjYYSpIFzCBleqdOzV3RwXMvyXPVNPHRe2UdKFi-ohRs6YhgWzH_fVprbiveMBxdb8zihmiKD4_cM_1NRo57FGXb4w",
)
DEFAULT_REFRESH_TOKEN = os.getenv(
"LOCUST_REFRESH_TOKEN",
"eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6I2U19iLXFFbzExZjUwY1FsWWtKV3hndm9OWSJ9.eyJhdWQiOiJlZTdhMzA3Yy1jZTYxLTQzY2YtOTdlOC1hZGFjYjQ1NmMzNDgiLCJleHAiOjE3NzcxMzUyNTgsImlhdCI6MTc3NDU0MzI1OCwiaXNzIjoiaHR0cDovLzM0LjQ3LjIxNC4xMTE6OTAxMS8iLCJzdWIiOiIxZjNiNmQyNi03YWY2LTQ5YjAtOGIzZi03NTdhMmIyYzE5YTYiLCJqdGkiOiI3NmM0MzAyOS04NGIxLTQ5OTktOTdhNi02NjBhMmI2OWQ4OTQiLCJyb2xlcyI6W10sImRldmljZV9pZCI6IjA4Yzg4YjcxLTBlOWEtNDk0Ni05MWM1LWMxOWI2ZDM2MjhlYiIsInR5cGUiOiJyZWZyZXNoIiwiYW1yIjpbIm5vbmUiXX0.PGeKNCcgHpRrPB08OGKNG0Xe165FFWNvbaIC7KXZ06UcidPhsZVDmC_iz-TWEwegyCnynRmabvOrz4zJeTYN_tmXy5kffmsl-gxOwL3W_GoK4ZIBxtxrqm03D_fDtFCA6fYtEFc3PXaTp1QcNM3aQB8X_HbT0U527CDF8S2spXGMpAehm-pMJ1ITfufO7nLBwCXJWWAqW0EhC7tvB4WVXSDQ9iHdDw7jbxFTrDfNMrKwuXA_VmeCIUPMbYHHLjYYSpIFzCBleqdOzV3RwXMvyXPVNPHRe2UdKFi-ohRs6YhgWzH_fVprbiveMBxdb8zihmiKD4_cM_1NRo57FGXb4w",
)
 
DEFAULT_ENCRYPTED_PAYLOADS = {
"LOCUST_DEVICE_REG": {
"key": "k8qJIcK33BVC/oKNcahMzalYjNS3sgepTOmEOi/w1cYi6UHJRYprikbhlwTDsKiJ38uzSVuRujWwIwkZd+ohr7bNDGVYq5GQhx/FgGUf+9lhw8bk4+UahQzUgQ8Htfio0oeM/RR+DvFdKhID5nXF0NZHF+dXe2fNGMRHluDss1Q7gd86gWz/40mChiokdPbK0D/3G4UrR2i4fDh1/sxHpJARzGhciHVPaIBNK4RyFSwmHzrCbAodtqnCThg07McfE9fgZpQ1H0fPwKiTs1QDoofgMwqCyLIHS48BMmP4F6a9CpKAyzblgXNzn77/WFWb6iGRiXlo13Lfzo0gfDeQ==",
"data": "BA8mZUlnB9fS97Am2VTrilCGSxzx5lRMavqkQ9bu1RqppoQYFUrzxa40MqmBSIdfJ2kkFBTm/MH+dOSlRErPSr1Npgc6SzhADGdwjhfqNus/uqWYCEa9URenscv+0YKTKP+ABvrUDg8YatWGo6vg==",
},
"LOCUST_WAITLIST": {
"key": "VlBgHjRA+IASvqa6YwY81KrA6I1N7QYqZp8GfPwIKgFqkdAQukUTO8aLQyuWNGccxfFioOoIBAJrgxvu7i9oD9apEdKXC3w2zZnEEcjgyRGPoVhqIPwMDH360wMqKK+yVncVzfmLrPa/bTdEa5XiaT6S4pBki8kewpoR0mGA2gs7qbTh+Hvd3yUEjiIB6Ruw7BtBkqvt/HR2mlS3mQXbZZ8dCuZfFQxT9nqk7jeaIkwWWg61rlW+vdD2/ln0288YiTttO/0Jl3s+kSgb1OT8nWS9+Fy9Yk558RAYR5X7ECTnuIoAddwz5ZPlPI253cZ9LVDOXo5E2Ve3WlORTmaw==",
"data": "a9V9Uw4R84Qp+/8GHR3a9CZCSkfYrmpcTg+lQRkcjhJOIikTmAKQFlJce75EiaHcoeF1TWgyUwcROdjnfHUAh3uxshKZf2Bb/ub6c07HvsigW31dd6kNW13tsA02tAaivhdc88u5XHG3ae1Y1/Y9xZDQxVIB057SlM9PMLJQygdC62VCHJxhEo2of0a3dUZr3eR1KEnnKmeiuQ32dD5Oi6BA5oiUJDNA==",
},
"LOCUST_WAITLIST_VERIFY": {
"key": "hMB9YFRp57A6piKIqJJDR5yUjnZc9i2likgNF3DzM6IfHbjn3ON7uCTacHpFWUbiGNmeMMTDRFbXAtT2B5hX7yefpqr11ttM18y/S14pYmgHGpCtLUZo+EEc0+ZkPAyOj6HUo40sDoI0aoA3AfbHLREyO+ZLBcf2bBpTzkKH6JDCeOfjJW+0SifokILjhbCpemRPvv+6DI1CnB5CA6smRAb0xafpYvEv+K6eT7Ofnk5I6d+hmtimqt0Dg/x5Zdt38sZL4EZqN286Hs2H7kxYZKQOeqSedw6ZH7eHIE8AQii+AxY7w42rPtfI2Vs+ogdRUZXKuk+V8LHvqq49Q46w==",
"data": "y1lTZ0im/fp2w5MKGWHc7c1w+dfwdEH8J7diWO6d+ZnZPsGISFKW4HcjAKG8UxZ2FIRU5A19S7v9OOxIbRyn0FZlHVWDm8dKjDaCenG1HLk4GN/J8TivBU2DiDjtmfAM5u0z1OjBF9JlaEzonlEbOAOQwmglTxWrjQCagqrqkHS8HVcuU2iqvcfmOkqp9QSjHN6A79k4AhqCM=",
},
"LOCUST_INVITE_DEVICE": {
"key": "ZTwhv3oixZ5U221a0RTF0d2j+VmK7J/lYsKBu6QAmEACmeLbsmFhJKlg2M93xjklrKAfRGhr8dYQdwexFmNeNpPe2G5gf0otPiEPXyWMMCvu9AdOF1xehwN4chUndm+W6Ir4qaWUHYN0VMz4VyFEXQ8MDGIMmQCF5iXNwwr6mRQYKKNRt3YmVl05HPcKCZslMwFtvOfsmSILGzX9UkOnyEVAnzmezDIMZJTjakOLsABJ0F3leNNwM8XkvIsZ0sUx5U8j1kY7nZ3+TYJjZgbGIi/4Z+Ms/q4RmE5IjWK/1X+IqdQDNb0OCVK6lHAsoSVfiVE0DXdorxmPV4C+tOPA==",
"data": "TaFcbuSigCJINxjl90x9/2tr680SdyH9V3aZMWyZtesWyauyGqDCiCFYrfcAK/E8cJGSaiw+oXuq9VisI6YwHPmsz7/C+dhtBDvQDja9S98fEmHOLBhNnL9ixs1SuduEVcUR6dXQ+N8xIDMzB7D59fHKRqN2DafWapwiJz1bmdrGvB",
},
"LOCUST_WAITLIST_RESEND": {
"key": "gScnAldBeQQRUkXIXt6IQxNRJMLsj5uHP1bmm8Heu+bN1RMelzljjHZV+0GbMUh+o+CLRs1M8ZTr7zcgp3z9baGmj9NugikCYY6bfA4mrkb3PWy77Hv/0Pa+I9tb4lao2w2gC8+7uSOWF0SLycOzrFdiaYU4M3LIob/Lq+cT1poo6J359dwV7zSi6QobiOJf7tYKyiL2Jj4wF0TlKAZudBJ0rQ7cMJQlHqOra6ORamxzCKJVmRnrT/hxOSiSM8shohGIsS0U/xyTOfqzAsWecuKkXwAc8/ObfLejMDrjzGFkYvqD9SKRCpi+5w78wdJnXBjiUkozj7f+osDhfZsQ==",
"data": "1rIZkuQthoy6If6BJbvTRbV6MkA8xsKxX33+HdhwYFefjbxe0BjnbvUZxS+JL8N+i9/anEICiBxzsCbJ3xrntLOPAPiN+N/K0py6qBEdL/j3+h6MIOeYGp9pNJqdT3opUp2O1MAFsEelPQTg==",
},
"LOCUST_REGISTER_PROFILE": {
"key": "VefSlbB/f9DsSIYNMEM2j7C/Jv8WPt7hPKpI6GMjQIJ9LUZrc/IayGn2Wes9vIY5c/Gsq6LQZ9TE8Vu4ObVVSMOnOiFE+DTMQunDO9BfE9zZs5uCTPDiiA1JgsrMZJbmZ9QVj/Yp7V74lBCd/SQfSKX8YgFtyhAoR7Bryvm4MRYqx61o30WlYHbE/22Ec03YmbRn7JuXPWGsEqcyzOf8JyiIuirreSBqdpDVa+QJ+JxWpRQsvfGcLxbGO9jN65kQWwooClb5tAMA1xgcXfyp4IXRMpDeFg7fGrP2DKCKgzUjaiBOiIp469xSYHUxmZzFmQHmYoFJICOUE6cm85vA==",
"data": "+Yb7A/T1rQRBOKYS5ijVXfQUHi79O6MqnUYFdvpOWfp+YoREpvjE0zPq76NxnutDdW/fS1rpJNl0JWHX+nyjE2QG8MU2b8ZhPguNDcYW5uWYwKwQxBqxMlc2iGDRNB8rZz/Ejykmf3iMATZW/q7YGQ1y8YdJzzOYXdwV6qsWo=",
},
"LOCUST_RESEND_OTP": {
"key": "Fq/IDkc4IN6IwsDBuwCvAbjmQPwodgDIc5fxXImHcMQosM3uZm9UVL8PhiF48JPCM6n2BEUPrKOALariNRsIdRZk/Y0pi3CV1Pd3SbS0P8jPrS68O0bOaGueNYz1RtOXumzAbYVQKHjB8WvD7XdFEt+USNfnV21TCPAH3cAnHYpObyZckj9T6Kw2Pw2QBxNfv/tBITPGJQ/ugbnSfJT3LpYKVYb216iQixDEGDTOBIx1pA7GFKzn0wd4KSsM9TAxSsWRYu5a9Acm9kGVC7gg3zh92/2H6/FlHaBd8EMrH2ONGs4fM3vmGli8yBm/az9DT001q3T/0Q3aOae9KmWQ==",
"data": "3jZbUzh48/oCl5FpYyfgere8PryGMjCoWCl7N0Fx1d9+FDA1RTLN7maXnT8MxVKWlLHnM2tubS4+qZV5636CwL8GKr9XExAtZow5mnvipp41n2Mr0GiVaCaQPo9qH+3Chi3bpbU2E0cGJIxi22kjQ=",
},
"LOCUST_VERIFY_OTP": {
"key": "rCb1vjyZHwAcRq193/Qxs6P5Tm3aj8TNpL9wCem+PKIRPUupDuKzW0l0R3pwqxcrruAp9VjT4VCXFecPGP7MYEZWa4o7n0tuEQmKZlfV+cH2ZrqU1kWPzZieYtchd0aH5AMbtclXuNZUvGtphfmy5HWWpmUHHlcFlEmoeh+nkGgM5RS3bsHdWUDTtvXjMXjBF8pYgLCxqR7gyheL+s3EaEYpO8SbcfN8LuOFSBzoJ7G1EaqaoLBmSsexp0ZRPxAh0s+zssalGmeQCliJVkqSH7x/xj2y3X5LNpuJX1L346/n3pp9DlFnVU7K4WiN5tLid05iXKov5uX3FqBeQvyg==",
"data": "dyLhmAaTU6SBnrraH3vadqGVU2TR4LETYouZGmCbWQ+klM6FJ6hrkxtOebfEzMt2oMu8sSWImpuTgyIUh3WJlvt3tvuQLLeJ0dVY+hhBNAbMNOTlfsVy6Cw86SgYYejBlajWYmtAhnSLWoVgN0fOtij4aHQ1kcMHOkrbEzqThgdrkI+Fx2LHx4OAhemcL1MoU7NT76W8o=",
},
"LOCUST_LOGIN": {
"key": "Z6rgSRYIsEibutHJj+pSWjlvNZoFR2DVaw5DiHBA8HIhj5tAqD741d7Cm5YfWxXqDEewusnX/1lDkMme2Ql24Wce7/iB1aS12DK+mFpA9LuhZetshyKquN1cTROIQ8pcu0xLXGFhoXfEpJCmg7fS71zeuuf1BWRkgQwzJNGk+ApwdPF2tWMQKmqQl8qWi1bkJv8V8YfxcKOsIv5CSOL7952WY91Px70K2O5z3a+QzLCmYVzSWCYFuSJr3jnDKCofkgkt9r2MIGNgqYZ+bVAwtD3MuMeNSZX7ZtSamk+hP4URZ7vEpqhxKUOhh2ucd3SxQupHqigQKDx5+LH/cxLg==",
"data": "igUDQUyB9Pi5TuiODtXeNsuNGk34F8QaGi5ETRIZ/hPz+znyx6koXzjP2RACUKrSYenebKV/Ts/pDz3twH1XtC8NfQvvs3TRQMxluKwspxtcfHrt2sBNuMp8QvwtfQ//bZkSryS6kqmCthA5SI",
},
"LOCUST_FORGOT_PASSWORD": {
"key": "oiLgl0HKaRDLDydxVuFr+bFo6B6QyLh/JXQCkFZPHhy2xpMSHGjex3kOvY03T6mcADtoDuaGVaMK8Tjgch71Fpt2fgpahSlF7IgfQMLz79APd5NrzUuFIYrQA8IE+ljyXic19cFZFNvt+xV+2hb9MQAXnWW1zmj19wMjeXzsWtvJ+0bKG1vMEsZc0ffGaWhWCn9Ndkr6WBv2fBvZcs45ge/1PXAKITF5LTHYTtH9mC2MUlxPnmLprlkcy4/7w9YYqhhUwzqlJOEGgktKKl3I35mMlPR6xm+ciT0yJk8EK09AwgGRZSPjomT4yF4xuNrmtRxuAWY8t8KuiFq+Jb5Q==",
"data": "wBVZDlyPQk4mEKzwWB9hFOUXvzL4HCt2LWlkiClX2pdsPh0Ld5X4w1PQA6zvWAHkVtLCHqj0QVaFZCp7Bw5oQ+t7Jv3bN0GILwf9vnAtL+74n8/bii",
},
"LOCUST_VERIFY_OTP_FORGOT": {
"key": "uoSr+qAQf/qVlTkAYNNAwrKlOsbVv2FBASCtsmIVGjzygKZDK1W78ItZxitbdD/QNq20FwtkMBirT5bhMcpKFJ6AlkEEAGYzTWTyHu6wEhv0CKOSVuj3hxa0+qLtPqPwnwQoUdZC450mraB4tu7tTIGbd/tukMn3r/UskJIK5QWHElx9H7qmv8rL5t4bTrgIfQJX5NBax0mO+yuCssi1EybypGj2rWEYWNrrUFw58Y8wh4mWZSPAP2iDb5F5VkEJcyOMN1kZsZVyBjyr4YSFolUXhoEnbRvPGOzs2OObjOaqmx0CQVv7OkYbjwo3A/RsjJjHX7YKraaHi5GVcdjQ==",
"data": "yt3uY+0dc9CTjjYRaPc1edxw2OEgT90VvGwYbahK8kmlwgE/7MM1aQvvyA3FsYjFg/Q9ObYAHOHC9HAsUkctggcXShxCvHyADF28xiGJQTJSe4/mEr10dUiKoxrH0tLc6DYqhAj/bb7SY0LjIjXF8cOvELUFuwaanKJIoCDe7cn3GhfbhxKYJ5ygjYYqKVYmXRg9PVcCeZwEE=",
},
"LOCUST_SET_FORGOT_PASSWORD": {
"key": "O0+wmVKANM3ldbpljvZ2CF8p71aczyRWxDlQS+VDHzBlgoSHULkyg6bTa94FuRniiLudttzv+n4hB6oxBR5KmhVEq2UBO2HtU3FpKkEDyvp1kWlMbUzqWhUy3wayfJ2miu9usykKjvrgIUcO0rLNcB7WZQIm8cSoqvGWNNGCn8JyL8sDTV57xIOfDceWjgwd0F0+zb+ryHp7nVizg3hjAHmv3wIXQ8dvilpuI2cACEPUqdtoTxyNt/1CvTY9nMD0Jf4wxtJlJj8gZH+HeTAKjDu79yiYhzLHqGozxxk9Lh1AManRoL4SywRLlBjpMwmuZNgPD9mPHPGJXXsU8/vQ==",
"data": "8ihDbw6EHCX7PLoxU9eAD8zJZ+jPle9H0byaEz3VNREsmy/aDKS5unBFROgxBpPeX92YXK9+urTGx8RMYvT4s8GxNbfRKYsS7OpjQasS975vkal8jgyhUUOd6HGQvTxK01XhaRkzlzPVa5HNCJ9Q==",
},
}
 
 
def _as_bool(value: str | None, default: bool = False) -> bool:
if value is None:
return default
return value.strip().lower() in {"1", "true", "yes", "y", "on"}
 
 
def _jwt_exp_epoch(token: str) -> int | None:
try:
token = (token or "").strip()
token_match = re.search(
r"([A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)",
token,
)
if token_match:
token = token_match.group(1)
token_parts = token.split(".")
if len(token_parts) < 2:
return None
payload = token_parts[1]
payload += "=" * (-len(payload) % 4)
decoded = base64.urlsafe_b64decode(payload.encode("utf-8")).decode("utf-8")
claims = json.loads(decoded)
exp = claims.get("exp")
return int(exp) if exp is not None else None
except Exception:
return None
 
 
def _is_token_exp