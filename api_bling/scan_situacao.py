import requests
import base64
import os

# --- SUAS CREDENCIAIS ---
CLIENT_ID = '12f3238c25ead9eeea221408a195caa388b7c98e'
CLIENT_SECRET = '0b5218c05e82c467d373e35ef5da9a4be9a3a7fad8c7fa2abfb326713f73'
ARQUIVO_TOKEN = 'refresh_token.txt'

def get_token():
    with open(ARQUIVO_TOKEN, 'r') as f: return f.read().strip()

def save_token(new_token):
    with open(ARQUIVO_TOKEN, 'w') as f: f.write(new_token)

# 1. AUTENTICAÇÃO
print("Autenticando...")
try:
    refresh_token = get_token()
    url_auth = "https://www.bling.com.br/Api/v3/oauth/token"
    credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
    encoded = base64.b64encode(credentials.encode()).decode()
    
    payload = {'grant_type': 'refresh_token', 'refresh_token': refresh_token}
    headers_auth = {'Authorization': f'Basic {encoded}', 'Content-Type': 'application/x-www-form-urlencoded'}
    
    resp = requests.post(url_auth, data=payload, headers=headers_auth)
    token_data = resp.json()
    access_token = token_data['access_token']
    if 'refresh_token' in token_data:
        save_token(token_data['refresh_token'])
except Exception as e:
    print(f"Erro de login: {e}")
    exit()

# 2. O DETETIVE
print("\n" + "="*40)
print("   DETETIVE DE IDs DE SITUAÇÃO")
print("="*40)

while True:
    numero_pedido = input("\nDigite o NÚMERO do pedido (ou 'sair'): ").strip()
    if numero_pedido.lower() == 'sair': break
    
    url = "https://www.bling.com.br/Api/v3/pedidos/vendas"
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'numero': numero_pedido} # Busca pelo número exato
    
    print(f"Investigando pedido {numero_pedido}...")
    resp = requests.get(url, headers=headers, params=params)
    data = resp.json()
    
    if 'data' in data and len(data['data']) > 0:
        pedido = data['data'][0]
        sit = pedido['situacao']
        print(f"\n✅ ENCONTRADO!")
        print(f"Nome da Situação: {sit['valor']}") # O nome (Ex: FULL Aprovado)
        print(f"ID da Situação:   {sit['id']}")    # <--- O NÚMERO QUE VOCÊ QUER!
        print("-" * 30)
    else:
        print("❌ Pedido não encontrado. Verifique o número.")