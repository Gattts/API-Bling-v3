import requests
import pandas as pd
from sqlalchemy import create_engine, text
import base64
import time
import os
from datetime import datetime, timedelta

# --- CONFIGURAÇÕES PESSOAIS ---
CLIENT_ID = 'SEU_CLIENT_ID_AQUI'
CLIENT_SECRET = 'SEU_CLIENT_SECRET_AQUI'
TXT_TOKEN_PATH = 'refresh_token.txt'
DB_CONN = 'mysql+pymysql://root:SUA_SENHA@localhost:3306/bling_db'

# DATA ALVO: SEMPRE O DIA DE ONTEM (D-1)
DATA_ALVO = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
# DATA SEGUINTE: HOJE (Para pegar o transbordo de fuso horário)
DATA_SEGUINTE = datetime.now().strftime('%Y-%m-%d')

# CLASSIFICAÇÃO DE STATUS
STATUS_LEVES = [6, 9, 12, 15, 18, 24, 447539]
STATUS_PESADOS = [452827] # O Vilão (Full Aprovado)

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
        resp = requests.post('https://www.bling.com.br/Api/v3/oauth/token', headers=headers, data={'grant_type': 'refresh_token', 'refresh_token': refresh_token}, timeout=20)
        if resp.status_code != 200: return None
        data = resp.json()
        with open(TXT_TOKEN_PATH, 'w') as f:
            f.write(data['refresh_token'])
        return data['access_token']
    except: return None

def get_mapa_situacoes(token):
    mapa = {}
    try:
        resp = requests.get('https://www.bling.com.br/Api/v3/situacoes', headers={'Authorization': f'Bearer {token}'}, timeout=15)
        for s in resp.json().get('data', []):
            mapa[s['id']] = s['nome']
    except: pass
    return mapa

def buscar_detalhe_completo(id_pedido, token):
    """Busca itens e taxas financeiras com retry simples."""
    for _ in range(2):
        try:
            resp = requests.get(f'https://www.bling.com.br/Api/v3/pedidos/vendas/{id_pedido}', headers={'Authorization': f'Bearer {token}'}, timeout=15)
            if resp.status_code == 200:
                data = resp.json().get('data')
                return data[0] if isinstance(data, list) else data
            elif resp.status_code == 429:
                time.sleep(2)
        except: pass
    return None

def salvar_banco(lista_pedidos, lista_itens, engine, ids_lote):
    if not lista_pedidos: return

    df_p = pd.DataFrame(lista_pedidos)
    df_i = pd.DataFrame(lista_itens)

    with engine.begin() as conn:
        # 1. Pedidos (Upsert)
        df_p.to_sql('temp_stg', conn, if_exists='replace', index=False)
        conn.execute(text("""
            INSERT INTO pedidos_vendas (id, numero, data, total, valor_frete, taxa_marketplace, valor_liquido, situacao)
            SELECT id, numero, data, total, valor_frete, taxa_marketplace, valor_liquido, situacao FROM temp_stg
            ON DUPLICATE KEY UPDATE
                total=VALUES(total), 
                taxa_marketplace=VALUES(taxa_marketplace), 
                valor_liquido=VALUES(valor_liquido), 
                situacao=VALUES(situacao), 
                updated_at=NOW()
        """))
        conn.execute(text("DROP TABLE temp_stg"))

        # 2. Itens (Delete & Insert)
        ids_str = ','.join(map(str, ids_lote))
        conn.execute(text(f"DELETE FROM pedidos_itens WHERE pedido_id IN ({ids_str})"))
        if not df_i.empty:
            df_i.to_sql('pedidos_itens', conn, if_exists='append', index=False)

def processar_lote_bruto(dados_api, token, mapa_sit, id_sit_ref):
    """Transforma JSON da API em listas para o Banco."""
    lote_p, lote_i, ids = [], [], []
    
    for r in dados_api:
        # O GRANDE FILTRO: Só aceita se a data do pedido for EXATAMENTE o alvo
        if r['data'] != DATA_ALVO: continue
        
        # Busca Financeiro
        p = buscar_detalhe_completo(r['id'], token)
        if not p: continue
        
        ids.append(p['id'])
        
        # Cálculos Financeiros
        tot = float(p.get('total', 0))
        tax = p.get('taxas', {})
        val_com = float(tax.get('taxaComissao', 0))
        val_frete = float(tax.get('custoFrete', 0))
        liq = tot - val_com - val_frete
        
        nome_sit = mapa_sit.get(p.get('situacao', {}).get('id'), str(id_sit_ref))

        lote_p.append({
            'id': p['id'], 'numero': str(p.get('numero')), 'data': p['data'],
            'total': tot, 'valor_frete': val_frete, 'taxa_marketplace': val_com,
            'valor_liquido': liq, 'situacao': nome_sit
        })
        
        for i in p.get('itens', []):
            lote_i.append({
                'pedido_id': p['id'], 'codigo_produto': str(i.get('codigo')),
                'descricao': i.get('descricao'), 'quantidade': float(i.get('quantidade', 0)),
                'valor_unitario': float(i.get('valor', 0)), 
                'total_item': float(i.get('quantidade', 0)) * float(i.get('valor', 0))
            })
            
    return lote_p, lote_i, ids

def main():
    log(f"🚀 INICIANDO ETL PRODUÇÃO BLINDADO")
    log(f"🎯 Meta: Resgatar pedidos de {DATA_ALVO}")
    
    token = get_token()
    if not token: return
    
    engine = create_engine(DB_CONN)
    mapa = get_mapa_situacoes(token)
    
    total_geral_salvo = 0
    ids_processados_global = set()

    # --- PARTE 1: STATUS LEVES (Via Inclusão) ---
    for id_sit in STATUS_LEVES:
        nome = mapa.get(id_sit, str(id_sit))
        log(f"🔹 Processando Leve: {nome}...")
        page = 1
        while True:
            url = f'https://www.bling.com.br/Api/v3/pedidos/vendas?page={page}&limit=100&dataInclusaoInicial={DATA_ALVO}&idsSituacoes[]={id_sit}'
            try:
                resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=20)
                if resp.status_code == 429: time.sleep(2); continue
                
                dados = resp.json().get('data', [])
                if not dados: break
                if dados[-1]['data'] < DATA_ALVO: break # Freio de data antiga

                # Processa e Salva
                l_p, l_i, ids = processar_lote_bruto(dados, token, mapa, id_sit)
                
                # Remove duplicatas globais
                l_p_clean, l_i_clean, ids_clean = [], [], []
                for idx, ped in enumerate(l_p):
                    if ped['id'] not in ids_processados_global:
                        ids_processados_global.add(ped['id'])
                        l_p_clean.append(ped)
                        ids_clean.append(ids[idx])
                        # Adiciona os itens correspondentes a este pedido
                        pid = ped['id']
                        l_i_clean.extend([x for x in l_i if x['pedido_id'] == pid])

                if l_p_clean:
                    salvar_banco(l_p_clean, l_i_clean, engine, ids_clean)
                    total_geral += len(l_p_clean)
                    print(f"   Salvos: {len(l_p_clean)} pedidos.", end='\r')
                
                page += 1
            except Exception as e:
                log(f"Erro Leve {id_sit}: {e}"); break

    # --- PARTE 2: STATUS PESADOS (Horizonte Expandido 48h + Fatiador) ---
    for id_sit in STATUS_PESADOS:
        nome = mapa.get(id_sit, str(id_sit))
        log(f"\n🔸 Processando Pesado: {nome} (Varredura 48h)...")
        
        headers_api = {
            'Authorization': f'Bearer {token}',
            'User-Agent': 'Mozilla/5.0' # Disfarce para evitar cache agressivo
        }

        # Varre dia Alvo E dia Seguinte
        dias_varredura = [DATA_ALVO, DATA_SEGUINTE]
        
        for dia_corrente in dias_varredura:
            print(f"   📅 Varrendo dia {dia_corrente}...")
            for hora in range(24):
                hora_ini = f"{hora:02d}:00:00"
                hora_fim = f"{hora:02d}:59:59"
                ts_ini = f"{dia_corrente} {hora_ini}"
                ts_fim = f"{dia_corrente} {hora_fim}"
                
                sucesso = False
                tentativas = 0
                
                while not sucesso and tentativas < 3:
                    try:
                        url = (f'https://www.bling.com.br/Api/v3/pedidos/vendas?page=1&limit=100'
                               f'&dataAlteracaoInicial={ts_ini}&dataAlteracaoFinal={ts_fim}'
                               f'&idsSituacoes[]={id_sit}')
                        
                        resp = requests.get(url, headers=headers_api, timeout=30)
                        if resp.status_code == 429: time.sleep(3); continue
                        
                        dados = resp.json().get('data', [])
                        sucesso = True
                        
                        if dados:
                            l_p, l_i, ids = processar_lote_bruto(dados, token, mapa, id_sit)
                            
                            # Remove duplicatas globais
                            l_p_clean, l_i_clean, ids_clean = [], [], []
                            for idx, ped in enumerate(l_p):
                                if ped['id'] not in ids_processados_global:
                                    ids_processados_global.add(ped['id'])
                                    l_p_clean.append(ped)
                                    ids_clean.append(ids[idx])
                                    pid = ped['id']
                                    l_i_clean.extend([x for x in l_i if x['pedido_id'] == pid])

                            if l_p_clean:
                                salvar_banco(l_p_clean, l_i_clean, engine, ids_clean)
                                total_geral += len(l_p_clean)
                                print(f"      ✅ {hora_ini}: +{len(l_p_clean)} novos pedidos.", end='\r')
                        
                        time.sleep(0.2)

                    except Exception as e:
                        tentativas += 1
                        time.sleep(5)
                        if tentativas == 3:
                            log(f"      ❌ Falha na faixa {hora_ini}: {e}")

    print("\n" + "="*40)
    log(f"🏁 FINALIZADO COM SUCESSO")
    log(f"📊 Total Gravado no Banco (Dia {DATA_ALVO}): {total_geral}")

if __name__ == "__main__":
    main()