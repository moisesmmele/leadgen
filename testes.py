import requests
import os
import json

auth_key = os.environ.get("DATAFORSEO_API_KEY")

if auth_key is None:
    print("API_KEY environment variable is not set.")
else:
    print("Using API key:", auth_key)


def get_data(keyword):
    url = "https://api.dataforseo.com/v3/business_data/google/my_business_info/live"
    payload = [{
        "keyword": f"{keyword}",
        "location_code": 1031967,
        "language_code": "pt-BR"
    }]
    payload_json = json.dumps(payload)

    headers = {
        'Authorization': f'Basic {auth_key}',
        'Content-Type': 'application/json'
    }
    data = requests.post(url, headers=headers, data=payload_json)
    return data


json_data = get_data("JV&F ESTACIONAMENTO").json()


'''phone = json_data["tasks"][0]["result"][0]["items"][0]["phone"]
print(phone)
'''
with open("dump.json", 'w') as f:
    json.dump(json_data, f, indent=4)
