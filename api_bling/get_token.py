import requests
import base64

# PREENCHA AQUI
CLIENT_ID = '12f3238c25ead9eeea221408a195caa388b7c98e'
CLIENT_SECRET = '0b5218c05e82c467d373e35ef5da9a4be9a3a7fad8c7fa2abfb326713f73'
CODE = 'd6fb9ea4051b7a9950401e59c32fd0d4ae4a6959' # Já coloquei o da foto

url = "https://www.bling.com.br/Api/v3/oauth/token"
credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
encoded_credentials = base64.b64encode(credentials.encode()).decode()

payload = {
    'grant_type': 'authorization_code',
    'code': CODE
}
headers = {
    'Authorization': f'Basic {encoded_credentials}',
    'Content-Type': 'application/x-www-form-urlencoded'
}

response = requests.post(url, data=payload, headers=headers)
print(response.json())