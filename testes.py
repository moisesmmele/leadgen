import requests
import json
from json import loads

auth_key = os.environ.get("DATAFORSEO_API_KEY")

if auth_key is None:
    print("API_KEY environment variable is not set.")
else:
    print("Using API key:", auth_key)

url = "https://api.dataforseo.com/v3/business_data/google/my_business_info/live"
payload = '[{"keyword":"MATERIAL DE CONSTRUCAO NOVA VIDA",  "location_code":1031967,  "language_code":"pt-BR"}]'
headers = {
    'Authorization': f'Basic {auth_key}',
    'Content-Type': 'application/json'
}

data = requests.post(url, headers=headers, data=payload)
print(data.text)

if 'tasks' in loads(data.text) and len(loads(data.text)['tasks']) > 0:
    task = loads(data.text)['tasks'][0]
    result = task['result'][0]
    phone_number = result.get('address', {}).get('phone')
    print(phone_number)