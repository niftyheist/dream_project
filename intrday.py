import requests
import json

url = "https://api.dhan.co/v2/charts/intraday"
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "access-token": access_token
}
body = {
    "securityId": "53028",  # Example, use your actual value
    "exchangeSegment": "NSE_FNO",
    "instrument": "FUTSTK",
    "interval": "60",
    "fromDate": "2025-07-22 09:30:00",
    "toDate": "2025-07-22 13:00:00"
}

print("Request Headers:", headers)
print("Request Body:", json.dumps(body, indent=2))

response = requests.post(url, headers=headers, json=body)
print("Status Code:", response.status_code)
print("Response Text:", response.text)