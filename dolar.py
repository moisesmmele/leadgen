import requests
import json

url = "https://economia.awesomeapi.com.br/last/USD-BRL"
response = requests.get(url)

data = response.json()
cotacao = data["USDBRL"]["ask"]
print(cotacao)