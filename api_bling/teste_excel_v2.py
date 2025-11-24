import requests
import pandas as pd
import base64
import os
import time
from datetime import datetime, timedelta

# --- CONFIGURAÇÕES ---
CLIENT_ID = '12f3238c25ead9eeea221408a195caa388b7c98e'
CLIENT_SECRET = '0b5218c05e82c467d373e35ef5da9a4be9a3a7fad8c7fa2abfb326713f73'
TXT_TOKEN_PATH = 'refresh_token.txt'

# DATA ALVO: ONTEM
DATA_ALVO = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
# DATA SEGUINTE (Para pegar o transbordo de fuso horário)
DATA_SEGUINTE = datetime.now().strftime('%Y-%m-%d') 

# IDs
STATUS_LEVES = [6, 9, 12, 15, 18, 24, 447539]
STATUS_PESADOS = [452827]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def get_token():
    if not os.path.exists(TXT_TOKEN_PATH):
        log("ERRO: refresh_token.txt não encontrado.")
        exit()
    with open(TXT_TOKEN_PATH, 'r') as f:
        refresh_token = f.read().strip()
    creds = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64 = base64.b64encode(creds.encode()).decode()
    headers = {'Authorization': f'Basic {b64}', 'Content-Type': 'application/x-www-form-urlencoded'}
    try:
        resp = requests.post('https://www.bling.com.br/Api/v3/oauth/token', headers=headers, data={'grant_type': 'refresh_token', 'refresh_token': refresh_token}, timeout=10)
        if resp.status_code != 200: return None
        data = resp.json()
        with open(TXT_TOKEN_PATH, 'w') as f:
            f.write(data['refresh_token'])
        return data['access_token']
    except: return None

def main():
    token = get_token()
    if not token: return

    print(f"🛡️ TESTE DE HORIZONTE EXPANDIDO (48H)")
    print(f"🎯 Procurando pedidos datados de: {DATA_ALVO}")
    print(f"🌍 Varrendo faixas de tempo de: {DATA_ALVO} e {DATA_SEGUINTE}")
    print("---------------------------------------------------")

    todos_pedidos = []
    ids_unicos = set()

    # 1. STATUS LEVES (Mantém igual, pois funcionou bem)
    print("1️⃣  Processando Status Leves...")
    for id_sit in STATUS_LEVES:
        page = 1
        count = 0
        while True:
            # Adicionei timeout aqui também
            url = f'https://www.bling.com.br/Api/v3/pedidos/vendas?page={page}&limit=100&dataInclusaoInicial={DATA_ALVO}&idsSituacoes[]={id_sit}'
            try:
                resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=20)
                if resp.status_code == 429: time.sleep(2); continue
                dados = resp.json().get('data', [])
                if not dados: break

                for p in dados:
                    if p['data'] != DATA_ALVO: continue
                    if p['id'] in ids_unicos: continue
                    
                    ids_unicos.add(p['id'])
                    count += 1
                    todos_pedidos.append({'Numero': p['numero'], 'Status': id_sit, 'Origem': 'Leve'})

                if dados[-1]['data'] < DATA_ALVO: break
                page += 1
            except: break
        print(f"   Status {id_sit}: {count} pedidos.")

    # 2. STATUS PESADO (Horizonte Expandido)
    print("\n2️⃣  Processando Status Pesado (452827) - 48 Horas...")
    
    headers_api = {
        'Authorization': f'Bearer {token}',
        'User-Agent': 'Mozilla/5.0'
    }

    # Lista de dias para varrer (Alvo + Dia Seguinte)
    dias_para_varrer = [DATA_ALVO, DATA_SEGUINTE]

    for dia_corrente in dias_para_varrer:
        print(f"\n📅 Varrendo dia {dia_corrente} (Buscando perdidos)...")
        
        for hora in range(24):
            hora_ini = f"{hora:02d}:00:00"
            hora_fim = f"{hora:02d}:59:59"
            ts_ini = f"{dia_corrente} {hora_ini}"
            ts_fim = f"{dia_corrente} {hora_fim}"
            
            count_hora = 0
            sucesso = False
            tentativas = 0
            
            while not sucesso and tentativas < 3:
                try:
                    url = (f'https://www.bling.com.br/Api/v3/pedidos/vendas?page=1&limit=100'
                           f'&dataAlteracaoInicial={ts_ini}&dataAlteracaoFinal={ts_fim}'
                           f'&idsSituacoes[]={452827}')
                    
                    print(f"   ⏳ {dia_corrente} {hora_ini} (T{tentativas+1})...", end='\r')
                    
                    resp = requests.get(url, headers=headers_api, timeout=30)
                    
                    if resp.status_code == 429:
                        time.sleep(3); continue

                    dados = resp.json().get('data', [])
                    sucesso = True 
                    
                    for p in dados:
                        # O FILTRO DE OURO:
                        # Baixamos dados de hoje e ontem, mas SÓ salvamos se a data do pedido for ONTEM.
                        if p['data'] != DATA_ALVO: continue
                        
                        if p['id'] in ids_unicos: continue
                        
                        ids_unicos.add(p['id'])
                        count_hora += 1
                        todos_pedidos.append({'Numero': p['numero'], 'Status': 452827, 'Origem': f'{dia_corrente} {hora}h'})

                except Exception as e:
                    tentativas += 1
                    time.sleep(5)
            
            if sucesso and count_hora > 0:
                print(f"   ✅ {dia_corrente} {hora_ini}: +{count_hora} pedidos (Resgatados!).   ")

    # RESULTADO
    print("\n" + "="*40)
    print(f"📊 CONTAGEM FINAL REAL ({DATA_ALVO})")
    print(f"🔢 Total Encontrado: {len(todos_pedidos)}")
    print(f"   (Meta: ~357)")
    print("="*40)

    if todos_pedidos:
        df = pd.DataFrame(todos_pedidos)
        caminho = os.path.join(os.path.expanduser('~'), 'Downloads', 'BI', 'Stopcell', f'VALIDACAO_EXPANDIDA_{DATA_ALVO}.xlsx')
        df.to_excel(caminho, index=False)

if __name__ == "__main__":
    main()