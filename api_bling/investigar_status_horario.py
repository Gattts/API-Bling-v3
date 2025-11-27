import requests
import pandas as pd
from sqlalchemy import create_engine, text
import base64
import time
from datetime import datetime, timedelta

# --- CONFIGURAÇÃO DO BANCO ---
DB_USER = 'sigmacomti'
DB_PASS = 'Sigma#com13ti2025'
DB_HOST = '177.153.209.166' 
DB_NAME = 'sigmacomti'
DB_CONN = f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:3306/{DB_NAME}'

# DATA DO PROBLEMA
DATA_ALVO = '2025-11-26'

# IDs QUE JÁ CONHECEMOS
IDS_CONHECIDOS = [6, 9, 12, 15, 18, 24, 95, 447539, 452827]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def get_token_from_db():
    print("   1. Conectando ao MySQL...")
    try:
        engine = create_engine(DB_CONN)
        with engine.connect() as conn:
            print("   2. Buscando credenciais...")
            res = conn.execute(text("SELECT client_id, client_secret, refresh_token FROM empresas WHERE id = 1")).mappings().first()
        
        if not res:
            print("   ❌ Nenhuma empresa ID 1 encontrada.")
            return None

        print("   3. Renovando token no Bling...")
        creds = f"{res['client_id']}:{res['client_secret']}"
        b64 = base64.b64encode(creds.encode()).decode()
        headers = {'Authorization': f'Basic {b64}', 'Content-Type': 'application/x-www-form-urlencoded'}
        
        # Adicionei TIMEOUT para não travar aqui
        resp = requests.post(
            'https://www.bling.com.br/Api/v3/oauth/token', 
            headers=headers, 
            data={'grant_type': 'refresh_token', 'refresh_token': res['refresh_token']},
            timeout=10
        )
        
        if resp.status_code == 200:
            print("   ✅ Token renovado com sucesso!")
            return resp.json()['access_token']
        else:
            print(f"   ❌ Erro ao renovar token: {resp.text}")
            
    except Exception as e:
        print(f"   ❌ Erro crítico na conexão: {e}")
    return None

def main():
    log(f"🕵️‍♂️ INVESTIGAÇÃO HORA-A-HORA - Dia {DATA_ALVO}")
    
    token = get_token_from_db()
    if not token: return

    stats_por_status = {}
    headers_api = {'Authorization': f'Bearer {token}'}

    print(f"\n🚀 Iniciando varredura das 24h...")
    
    for hora in range(24):
        hora_ini = f"{hora:02d}:00:00"
        hora_fim = f"{hora:02d}:59:59"
        ts_ini = f"{DATA_ALVO} {hora_ini}"
        ts_fim = f"{DATA_ALVO} {hora_fim}"
        
        url = (f'https://www.bling.com.br/Api/v3/pedidos/vendas?page=1&limit=100'
               f'&dataAlteracaoInicial={ts_ini}&dataAlteracaoFinal={ts_fim}')
        
        print(f"   ⏳ Lendo faixa {hora_ini}...", end='\r')
        
        try:
            resp = requests.get(url, headers=headers_api, timeout=15)
            if resp.status_code == 429: time.sleep(2); continue
            
            dados = resp.json().get('data', [])
            
            for p in dados:
                if p['data'] == DATA_ALVO:
                    id_sit = p.get('situacao', {}).get('id')
                    stats_por_status[id_sit] = stats_por_status.get(id_sit, 0) + 1
            
            time.sleep(0.2)
        except: pass

    print("\n\n📊 RELATÓRIO FINAL")
    print(f"{'ID STATUS':<12} | {'QTD':<10} | {'SITUAÇÃO'}")
    print("-" * 50)
    
    total_geral = 0
    ids_novos = []

    for id_sit, qtd in stats_por_status.items():
        total_geral += qtd
        if id_sit in IDS_CONHECIDOS:
            status_msg = "✅ Já mapeado"
        else:
            status_msg = "❌ NOVO! ADICIONAR!"
            ids_novos.append(id_sit)
            
        print(f"{id_sit:<12} | {qtd:<10} | {status_msg}")

    print("-" * 50)
    print(f"TOTAL DE PEDIDOS: {total_geral}")
    
    if ids_novos:
        print(f"\n🚨 ADICIONAR NA LISTA: {ids_novos}")

if __name__ == "__main__":
    main()