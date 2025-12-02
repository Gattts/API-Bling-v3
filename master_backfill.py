import requests
import pandas as pd
from sqlalchemy import create_engine, text
import base64
import time
from datetime import datetime, timedelta
from urllib.parse import quote_plus
import sys

# --- CONFIGURAÇÃO ---
DB_USER = 'sigmacomti'
DB_PASS_RAW = 'Sigma#com13ti2025'
DB_HOST = '177.153.209.166'
DB_NAME = 'sigmacomti'
DB_CONN = f'mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS_RAW)}@{DB_HOST}:3306/{DB_NAME}?connect_timeout=10'

# IDs PARA TRADUÇÃO VISUAL
SITUACOES_MAP = {
    6: 'Em aberto', 9: 'Atendido', 12: 'Cancelado', 15: 'Verificado',
    18: 'Loja Virtual', 24: 'Efetuado', 95: 'Impresso', 106002: 'Desconhecido',
    447539: 'Importado 3S', 452827: 'FULL Aprovado'
}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def get_db_engine():
    return create_engine(DB_CONN)

def get_valid_token(engine, empresa_id):
    with engine.connect() as conn:
        res = conn.execute(text("SELECT client_id, client_secret, refresh_token FROM empresas_bling WHERE id = :id"), {'id': empresa_id}).mappings().first()
    
    if not res: return None

    creds = f"{res['client_id']}:{res['client_secret']}"
    headers = {'Authorization': f'Basic {base64.b64encode(creds.encode()).decode()}', 'Content-Type': 'application/x-www-form-urlencoded'}
    
    try:
        resp = requests.post('https://www.bling.com.br/Api/v3/oauth/token', headers=headers, data={'grant_type': 'refresh_token', 'refresh_token': res['refresh_token']}, timeout=10)
        if resp.status_code == 200:
            new_token = resp.json()
            # Atualiza no banco
            with engine.begin() as conn:
                conn.execute(text("UPDATE empresas_bling SET refresh_token = :rt WHERE id = :id"), {'rt': new_token['refresh_token'], 'id': empresa_id})
            return new_token['access_token']
    except Exception as e:
        log(f"Erro Auth: {e}")
    return None

def listar_lojas(token):
    mapa = {}
    page = 1
    try:
        while True:
            resp = requests.get(f'https://www.bling.com.br/Api/v3/lojas?page={page}&limit=100', headers={'Authorization': f'Bearer {token}'}, timeout=10)
            if resp.status_code != 200: break
            dados = resp.json().get('data', [])
            if not dados: break
            for l in dados: mapa[l['id']] = l['nome']
            page += 1
    except: pass
    
    # Backup fixo
    backups = {205192288: 'Amazon - MV', 205184076: 'Kabum - MV', 205282672: 'Mercado Livre - FULL', 204524992: 'Mercado Livre - MV', 204762556: 'Shopee - MV'}
    for k,v in backups.items(): 
        if k not in mapa: mapa[k] = v
    return mapa

def salvar_dados(engine, dados, token, mapa_lojas, empresa_id):
    lote_p, lote_i, ids = [], [], []
    
    for r in dados:
        # Busca detalhe
        for _ in range(3):
            try:
                det = requests.get(f'https://www.bling.com.br/Api/v3/pedidos/vendas/{r["id"]}', headers={'Authorization': f'Bearer {token}'}, timeout=15).json().get('data')
                if det: 
                    p = det[0] if isinstance(det, list) else det
                    break
            except: time.sleep(1)
        else: continue # Falhou detalhe

        ids.append(p['id'])
        
        # Parse
        tot = float(p.get('total', 0))
        tax = p.get('taxas', {})
        val_com = float(tax.get('taxaComissao', 0))
        val_frete = float(tax.get('custoFrete', 0))
        liq = tot - val_com - val_frete
        
        id_loja = p.get('loja', {}).get('id')
        nome_loja = mapa_lojas.get(id_loja, f"Loja {id_loja}")
        
        # Tenta pegar nome do status, se não tiver, usa o mapa, se não tiver, usa ID
        sit_id = p.get('situacao', {}).get('id')
        sit_nome = p.get('situacao', {}).get('nome') or SITUACOES_MAP.get(sit_id, str(sit_id))

        lote_p.append({
            'empresa_id': empresa_id, 'id': p['id'], 'numero': str(p.get('numero')), 'data': p['data'],
            'id_loja': id_loja, 'nome_loja': nome_loja,
            'total': tot, 'valor_frete': val_frete, 'taxa_marketplace': val_com, 'valor_liquido': liq, 
            'situacao': sit_nome, 'id_situacao': sit_id
        })
        
        for i in p.get('itens', []):
            lote_i.append({
                'empresa_id': empresa_id, 'pedido_id': p['id'], 'codigo_produto': str(i.get('codigo')),
                'descricao': i.get('descricao'), 'quantidade': float(i.get('quantidade', 0)),
                'valor_unitario': float(i.get('valor', 0)), 
                'total_item': float(i.get('quantidade', 0)) * float(i.get('valor', 0))
            })

    if lote_p:
        with engine.begin() as conn:
            df_p = pd.DataFrame(lote_p)
            df_p.to_sql('temp_etl', conn, if_exists='replace', index=False)
            conn.execute(text("""
                INSERT INTO pedidos_vendas (empresa_id, id, numero, data, id_loja, nome_loja, total, valor_frete, taxa_marketplace, valor_liquido, situacao, id_situacao)
                SELECT empresa_id, id, numero, data, id_loja, nome_loja, total, valor_frete, taxa_marketplace, valor_liquido, situacao, id_situacao FROM temp_etl
                ON DUPLICATE KEY UPDATE 
                total=VALUES(total), taxa_marketplace=VALUES(taxa_marketplace), valor_liquido=VALUES(valor_liquido), situacao=VALUES(situacao), updated_at=NOW()
            """))
            conn.execute(text("DROP TABLE temp_etl"))
            
            # Itens
            if lote_i:
                ids_str = ','.join(map(str, ids))
                conn.execute(text(f"DELETE FROM pedidos_itens WHERE empresa_id={empresa_id} AND pedido_id IN ({ids_str})"))
                pd.DataFrame(lote_i).to_sql('pedidos_itens', conn, if_exists='append', index=False)
    
    return len(lote_p)

# --- FASE 1: ARRASTÃO POR ALTERAÇÃO (Pega 90%) ---
def fase_1_arrastao(engine, token, empresa_id, data_alvo, mapa_lojas):
    print(f"   🔄 Fase 1: Arrastão por Alteração...")
    total_fase1 = 0
    
    # Varre Dia Alvo + Dia Seguinte (48h de margem)
    dias = [data_alvo, (datetime.strptime(data_alvo, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')]
    
    for dia in dias:
        # Fatiador de 4 em 4 horas para ser rápido
        for h in range(0, 24, 4):
            ts_ini = f"{dia} {h:02d}:00:00"
            ts_fim = f"{dia} {h+3:02d}:59:59"
            
            try:
                # Filtra TUDO que foi alterado nesse horário (Sem filtro de loja/status para não bugar)
                url = f'https://www.bling.com.br/Api/v3/pedidos/vendas?page=1&limit=100&dataAlteracaoInicial={ts_ini}&dataAlteracaoFinal={ts_fim}'
                resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=15)
                
                if resp.status_code == 200:
                    dados = resp.json().get('data', [])
                    # Filtra no Python: Só salva se for DATA_ALVO
                    validos = [p for p in dados if p['data'] == data_alvo]
                    if validos:
                        salvos = salvar_dados(engine, validos, token, mapa_lojas, empresa_id)
                        total_fase1 += salvos
                        print(f"      + {salvos} pedidos (Alterados em {dia} {h}h)", end='\r')
            except: pass
            time.sleep(0.2)
            
    print(f"      ✅ Fase 1 Concluída. Base: {total_fase1} pedidos.")

# --- FASE 2: RESGATE SEQUENCIAL (Pega os 10% faltantes) ---
def fase_2_sequencial(engine, token, empresa_id, data_alvo, mapa_lojas):
    print(f"   🔄 Fase 2: Resgate Sequencial (Gap Filling)...")
    
    # Descobre range
    with engine.connect() as conn:
        sql = f"SELECT CAST(numero AS UNSIGNED) as num FROM pedidos_vendas WHERE data = '{data_alvo}' AND empresa_id = {empresa_id} AND numero REGEXP '^[0-9]+$'"
        numeros = sorted([row[0] for row in conn.execute(text(sql)).fetchall()])
    
    if not numeros:
        print("      ⚠️ Sem dados para calcular sequência. Pulando.")
        return

    min_n = min(numeros)
    max_n = max(numeros)
    
    # Expande margem para pegar pontas
    inicio = min_n - 100
    fim = max_n + 100
    
    set_temos = set(map(str, numeros))
    total_checar = fim - inicio
    cnt_resgatados = 0
    
    # Varre buracos
    print(f"      🎯 Verificando intervalo: {inicio} a {fim} ({total_checar} números)")
    
    for num in range(inicio, fim + 1):
        s_num = str(num)
        if s_num in set_temos: continue # Já tem
        
        try:
            url = f'https://www.bling.com.br/Api/v3/pedidos/vendas?numero={s_num}'
            resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=5)
            
            if resp.status_code == 200:
                dados = resp.json().get('data', [])
                if dados:
                    # Achou! Verifica se é da data
                    if dados[0]['data'] == data_alvo:
                        salvos = salvar_dados(engine, dados, token, mapa_lojas, empresa_id)
                        cnt_resgatados += salvos
                        print(f"      🔥 RESGATADO: {s_num}")
            
            time.sleep(0.1) # Rápido
        except: pass

    print(f"      ✅ Fase 2 Concluída. Resgatados: {cnt_resgatados}")

def main():
    print("--- 🏆 SUPER BACKFILL (SET/OUT/NOV) ---")
    i_ini = input("Data Início (YYYY-MM-DD): ")
    i_fim = input("Data Fim    (YYYY-MM-DD): ")
    
    try:
        dt_ini = datetime.strptime(i_ini, "%Y-%m-%d")
        dt_fim = datetime.strptime(i_fim, "%Y-%m-%d")
    except: return print("Data inválida")

    engine = get_db_engine()
    
    # Loop Dias
    curr = dt_ini
    while curr <= dt_fim:
        d_str = curr.strftime("%Y-%m-%d")
        print(f"\n📆 PROCESSANDO: {d_str}")
        
        # Pega empresa 1 (Hardcoded para facilitar, mas pode ser loop)
        token = get_valid_token(engine, 1)
        if token:
            lojas = listar_lojas(token)
            
            # 1. Arrastão (Rápido)
            fase_1_arrastao(engine, token, 1, d_str, lojas)
            
            # 2. Pente Fino (Preenche buracos)
            fase_2_sequencial(engine, token, 1, d_str, lojas)
        
        curr += timedelta(days=1)

if __name__ == "__main__":
    main()