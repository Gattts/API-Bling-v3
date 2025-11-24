import requests
import json
import base64
import os

# --- CONFIGURAÇÕES ---
CLIENT_ID = '12f3238c25ead9eeea221408a195caa388b7c98e'
CLIENT_SECRET = '0b5218c05e82c467d373e35ef5da9a4be9a3a7fad8c7fa2abfb326713f73'
TXT_TOKEN_PATH = 'refresh_token.txt'
PEDIDO_ALVO = '2000009257357562' # O pedido da sua foto

def get_token():
    with open(TXT_TOKEN_PATH, 'r') as f:
        refresh_token = f.read().strip()
    
    # Auth básica
    creds = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64 = base64.b64encode(creds.encode()).decode()
    headers = {'Authorization': f'Basic {b64}', 'Content-Type': 'application/x-www-form-urlencoded'}
    
    resp = requests.post('https://www.bling.com.br/Api/v3/oauth/token', headers=headers, data={'grant_type': 'refresh_token', 'refresh_token': refresh_token})
    data = resp.json()
    
    with open(TXT_TOKEN_PATH, 'w') as f:
        f.write(data['refresh_token'])
    return data['access_token']

def main():
    token = get_token()
    headers = {'Authorization': f'Bearer {token}'}
    
    print(f"1. Buscando ID interno do pedido {PEDIDO_ALVO}...")
    # 1. Acha o ID
    resp = requests.get(f'https://www.bling.com.br/Api/v3/pedidos/vendas?numero={PEDIDO_ALVO}', headers=headers)
    id_interno = resp.json()['data'][0]['id']
    
    print(f"2. Baixando JSON bruto do ID {id_interno}...")
    # 2. Pega o JSON Completo
    resp_detalhe = requests.get(f'https://www.bling.com.br/Api/v3/pedidos/vendas/{id_interno}', headers=headers)
    data = resp_detalhe.json()
    
    # 3. Salva em arquivo
    nome_arquivo = 'detalhe_completo.txt'
    with open(nome_arquivo, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
        
    print(f"✅ PRONTO! Abra o arquivo '{nome_arquivo}'")
    print(f"🔎 Procure pelo valor '9.24' ou '9,24' dentro dele e me mostre o trecho.")

if __name__ == "__main__":
    main()