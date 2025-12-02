import requests
import pandas as pd
from sqlalchemy import create_engine, text
import base64
import time
from datetime import datetime, timedelta

# --- CONFIGURAÇÃO ---
SITUACOES_PADRAO = [
    {'id': 6, 'nome': 'Em aberto'},
    {'id': 9, 'nome': 'Atendido'},
    {'id': 12, 'nome': 'Cancelado'},
    {'id': 15, 'nome': 'Verificado'},
    {'id': 18, 'nome': 'Loja Virtual'},
    {'id': 24, 'nome': 'Efetuado'},
    {'id': 95, 'nome': 'Impresso'},
    {'id': 106002, 'nome': 'Desconhecido'}, 
    {'id': 447539, 'nome': 'Importado 3S'},
    {'id': 452827, 'nome': 'FULL Aprovado'}
]

# Apenas para tradução de nomes
LOJAS_BACKUP = {
    205192288: 'Amazon - MV',
    205184076: 'Kabum - MV',
    205475035: 'Loja Física - Mvirtuar',
    205392863: 'Loja Própria - MV',
    205282672: 'Mercado Livre - FULL',
    204524992: 'Mercado Livre - MV',
    204762556: 'Shopee - MV'
}

# Status pesados usam lógica recursiva (Data Alteração + 48h)
IDS_PESADOS = [452827, 447539]

def log(msg, empresa_id):
    # Limpa a linha anterior para não encavalar
    print(f"\r{' ' * 100}\r", end='') 
    prefix = f"[{datetime.now().strftime('%H:%M:%S')}][Empresa {empresa_id}]"
    print(f"{prefix} {msg}")

def get_valid_token(creds):
    b64 = base64.b64encode(f"{creds['client_id']}:{creds['client_secret']}".encode()).decode()
    headers = {'Authorization': f'Basic {b64}', 'Content-Type': 'application/x-www-form-urlencoded'}
    try:
        resp = requests.post(
            'https://www.bling.com.br/Api/v3/oauth/token', 
            headers=headers, 
            data={'grant_type': 'refresh_token', 'refresh_token': creds['refresh_token']}, 
            timeout=10
        )
        if resp.status_code == 200: return resp.json()
    except: pass
    return None

def listar_lojas_da_conta(token, empresa_id):
    mapa = {}
    page = 1
    try:
        while True:
            url = f'https://www.bling.com.br/Api/v3/lojas?page={page}&limit=100'
            resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=10)
            if resp.status_code != 200: break
            dados = resp.json().get('data', [])
            if not dados: break
            for l in dados: mapa[l['id']] = l['nome']
            page += 1
    except: pass
    
    if not mapa:
        return LOJAS_BACKUP
    
    log(f"🏪 Lojas mapeadas: {len(mapa)}.", empresa_id)
    return mapa

def buscar_detalhe_financeiro(id_pedido, token):
    for _ in range(3):
        try:
            resp = requests.get(f'https://www.bling.com.br/Api/v3/pedidos/vendas/{id_pedido}', headers={'Authorization': f'Bearer {token}'}, timeout=15)
            if resp.status_code == 200:
                d = resp.json().get('data')
                return d[0] if isinstance(d, list) else d
            elif resp.status_code == 429: time.sleep(1)
        except: pass
        time.sleep(0.5)
    return None

def salvar_lote(empresa_id, lote_p, lote_i, engine, ids_lote):
    if not lote_p: return
    df_p = pd.DataFrame(lote_p)
    df_i = pd.DataFrame(lote_i)
    df_p['empresa_id'] = empresa_id
    df_i['empresa_id'] = empresa_id
    
    with engine.begin() as conn:
        df_p.to_sql('temp_etl_pedidos', conn, if_exists='replace', index=False)
        conn.execute(text("""
            INSERT INTO pedidos_vendas (
                empresa_id, id, numero, data, id_loja, nome_loja, 
                total, valor_frete, taxa_marketplace, valor_liquido, 
                situacao, id_situacao
            )
            SELECT 
                empresa_id, id, numero, data, id_loja, nome_loja, 
                total, valor_frete, taxa_marketplace, valor_liquido, 
                situacao, id_situacao 
            FROM temp_etl_pedidos
            ON DUPLICATE KEY UPDATE
                id_loja=VALUES(id_loja), nome_loja=VALUES(nome_loja),
                total=VALUES(total), taxa_marketplace=VALUES(taxa_marketplace), 
                valor_liquido=VALUES(valor_liquido), situacao=VALUES(situacao), 
                id_situacao=VALUES(id_situacao), updated_at=NOW()
        """))
        conn.execute(text("DROP TABLE temp_etl_pedidos"))
        ids_str = ','.join(map(str, ids_lote))
        conn.execute(text(f"DELETE FROM pedidos_itens WHERE empresa_id = {empresa_id} AND pedido_id IN ({ids_str})"))
        if not df_i.empty:
            df_i.to_sql('pedidos_itens', conn, if_exists='append', index=False)

def processar_lista_bruta(dados_api, token, engine, empresa_id, situacao_obj, mapa_lojas, ids_vistos_global):
    lote_p, lote_i, ids_proc = [], [], []
    for r in dados_api:
        if r['id'] in ids_vistos_global: continue 
        
        p = buscar_detalhe_financeiro(r['id'], token)
        if not p: continue
        
        ids_proc.append(p['id'])
        ids_vistos_global.add(p['id'])
        
        tot = float(p.get('total', 0))
        tax = p.get('taxas', {})
        val_com = float(tax.get('taxaComissao', 0))
        val_frete = float(tax.get('custoFrete', 0))
        liq = tot - val_com - val_frete
        
        id_loja = p.get('loja', {}).get('id')
        nome_loja = mapa_lojas.get(id_loja, f"Loja {id_loja}" if id_loja else "Loja Física / Manual")

        lote_p.append({
            'id': p['id'], 'numero': str(p.get('numero')), 'data': p['data'],
            'id_loja': id_loja, 'nome_loja': nome_loja,
            'total': tot, 'valor_frete': val_frete, 'taxa_marketplace': val_com,
            'valor_liquido': liq, 
            'situacao': situacao_obj['nome'], 'id_situacao': situacao_obj['id']
        })
        for i in p.get('itens', []):
            lote_i.append({
                'pedido_id': p['id'], 'codigo_produto': str(i.get('codigo')),
                'descricao': i.get('descricao'), 'quantidade': float(i.get('quantidade', 0)),
                'valor_unitario': float(i.get('valor', 0)), 
                'total_item': float(i.get('quantidade', 0)) * float(i.get('valor', 0))
            })
    if lote_p: salvar_lote(empresa_id, lote_p, lote_i, engine, ids_proc)
    return len(lote_p)

# --- RECURSIVIDADE GLOBAL (VISUAL ATIVO) ---
def processar_tempo_recursivo_global(token, engine, empresa_id, situacao_obj, ts_ini, ts_fim, mapa_lojas, data_alvo, ids_vistos_global):
    # VISUAL: Mostra o intervalo exato que está sendo analisado
    hora_show = f"{ts_ini.split(' ')[1][:5]} até {ts_fim.split(' ')[1][:5]}"
    print(f"      ⏳ Verificando {hora_show}...", end='\r')

    # Filtra SÓ por Data Alteração e Status
    url = (f'https://www.bling.com.br/Api/v3/pedidos/vendas?page=1&limit=100'
           f'&dataAlteracaoInicial={ts_ini}&dataAlteracaoFinal={ts_fim}'
           f'&idsSituacoes[]={situacao_obj["id"]}')
    
    dados = []
    for _ in range(3): # Retry
        try:
            resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=20)
            if resp.status_code == 200:
                dados = resp.json().get('data', [])
                break
            elif resp.status_code == 429: time.sleep(2)
        except: time.sleep(1)
    
    qtd = len(dados)
    
    dt_ini_obj = datetime.strptime(ts_ini, "%Y-%m-%d %H:%M:%S")
    dt_fim_obj = datetime.strptime(ts_fim, "%Y-%m-%d %H:%M:%S")
    diff_min = (dt_fim_obj - dt_ini_obj).total_seconds() / 60

    # CRITÉRIO DE CORTE: Se coube na página (100) OU o intervalo já é minúsculo (5min), salva.
    if qtd < 100 or diff_min < 5:
        if qtd > 0:
            # Filtra para salvar APENAS data_alvo (Anti-Fuso Horário)
            validos = [p for p in dados if p['data'] == data_alvo]
            if validos:
                salvos = processar_lista_bruta(validos, token, engine, empresa_id, situacao_obj, mapa_lojas, ids_vistos_global)
                if salvos > 0:
                    print(f"      ✅ +{salvos} pedidos salvos ({hora_show})           ")
                return salvos
        return 0
    
    # SE LOTOU (100), DIVIDE AO MEIO
    meio = dt_ini_obj + (dt_fim_obj - dt_ini_obj) / 2
    meio_str = meio.strftime("%Y-%m-%d %H:%M:%S")
    meio_mais_um = (meio + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
    
    t1 = processar_tempo_recursivo_global(token, engine, empresa_id, situacao_obj, ts_ini, meio_str, mapa_lojas, data_alvo, ids_vistos_global)
    t2 = processar_tempo_recursivo_global(token, engine, empresa_id, situacao_obj, meio_mais_um, ts_fim, mapa_lojas, data_alvo, ids_vistos_global)
    return t1 + t2

def processar_status(token, engine, empresa_id, situacao, data_alvo, mapa_lojas):
    id_sit = situacao['id']
    nome_sit = situacao['nome']
    
    # Se for ID Pesado OU se o script tiver mudado a flag no loop anterior
    usar_recursivo = id_sit in IDS_PESADOS
    total_status = 0
    ids_vistos_global = set()

    if not usar_recursivo:
        log(f"🔎 Varrendo '{nome_sit}'...", empresa_id)
        page = 1
        ultimo_id = None
        while True:
            url = f'https://www.bling.com.br/Api/v3/pedidos/vendas?page={page}&limit=100&dataInclusaoInicial={data_alvo}&idsSituacoes[]={id_sit}'
            try:
                resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=15)
                if resp.status_code == 429: time.sleep(2); continue
                dados = resp.json().get('data', [])
                if not dados: break 
                
                if dados[0]['id'] == ultimo_id:
                    log(f"⚠️ Loop em '{nome_sit}'. Ativando Recursivo.", empresa_id)
                    usar_recursivo = True
                    break
                ultimo_id = dados[0]['id']
                
                if dados[-1]['data'] < data_alvo:
                    validos = [p for p in dados if p['data'] == data_alvo]
                    if validos: 
                        processar_lista_bruta(validos, token, engine, empresa_id, situacao, mapa_lojas, ids_vistos_global)
                        total_status += len(validos)
                    break 

                validos = [p for p in dados if p['data'] == data_alvo]
                if validos:
                    processar_lista_bruta(validos, token, engine, empresa_id, situacao, mapa_lojas, ids_vistos_global)
                    total_status += len(validos)
                page += 1
            except: break

    if usar_recursivo:
        log(f"⚔️ Recursivo Global (48h) em '{nome_sit}'...", empresa_id)
        dias = [data_alvo, (datetime.strptime(data_alvo, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')]
        
        for dia in dias:
            ts_ini = f"{dia} 00:00:00"
            ts_fim = f"{dia} 23:59:59"
            total_status += processar_tempo_recursivo_global(token, engine, empresa_id, situacao, ts_ini, ts_fim, mapa_lojas, data_alvo, ids_vistos_global)

    if total_status > 0:
        print(f"      ✅ Concluído: {total_status} pedidos.                   ")
    
    return total_status

def executar_etl_empresa(empresa_id, creds_dict, engine, data_alvo=None):
    if not data_alvo:
        data_alvo = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    log(f"Iniciando ETL. Alvo: {data_alvo}", empresa_id)
    
    tokens_novos = get_valid_token(creds_dict)
    if not tokens_novos:
        log("Falha crítica de autenticação.", empresa_id)
        return False, None
    
    token = tokens_novos['access_token']
    mapa_lojas = listar_lojas_da_conta(token, empresa_id)
    
    total_geral = 0
    for sit in SITUACOES_PADRAO:
        qtd = processar_status(token, engine, empresa_id, sit, data_alvo, mapa_lojas)
        total_geral += qtd
    
    log(f"SUCESSO. Total importado: {total_geral}", empresa_id)
    return True, tokens_novos['refresh_token']