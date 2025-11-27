import requests
import pandas as pd
from sqlalchemy import create_engine, text
import base64
import time
from datetime import datetime, timedelta

# --- LISTA DE SEGURANÇA (Fallback) ---
# Só será usada se a API de descoberta falhar totalmente (ex: permissão negada)
BACKUP_IDS = [6, 9, 12, 15, 18, 24, 95, 447539, 452827]

# Status conhecidos por serem pesados (Full Aprovado)
# Se descobrirmos um status com este ID, já marcamos como "Pesado"
IDS_PESADOS_CONHECIDOS = [452827]

def log(msg, empresa_id):
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
            timeout=15
        )
        if resp.status_code == 200:
            return resp.json()
    except: pass
    return None

# --- NOVA LÓGICA DE DESCOBERTA ---
def descobrir_situacoes_reais(token, empresa_id):
    """
    1. Busca os módulos disponíveis.
    2. Encontra o módulo de 'Vendas'.
    3. Lista os status desse módulo.
    """
    lista_final = []
    id_modulo_vendas = None
    
    # PASSO 1: Descobrir o ID do Módulo de Vendas
    try:
        log("🕵️‍♂️ Mapeando módulos da conta...", empresa_id)
        url_modulos = 'https://www.bling.com.br/Api/v3/situacoes/modulos'
        resp = requests.get(url_modulos, headers={'Authorization': f'Bearer {token}'}, timeout=15)
        
        if resp.status_code == 200:
            modulos = resp.json().get('data', [])
            for m in modulos:
                # Procura por algo que pareça "Vendas" ou "Pedidos"
                nome = m.get('nome', '').lower()
                if 'venda' in nome or 'pedido' in nome:
                    id_modulo_vendas = m.get('id')
                    log(f"   -> Módulo de Vendas encontrado: ID {id_modulo_vendas} ({m['nome']})", empresa_id)
                    break
    except Exception as e:
        log(f"⚠️ Erro ao buscar módulos: {e}", empresa_id)

    # Se não achou via API, tenta os IDs padrões do Bling para Vendas (comum ser 98546 ou similar, mas varia)
    # Se id_modulo_vendas for None, vamos tentar listar sem filtro ou usar backup
    
    # PASSO 2: Listar situações usando o ID do módulo
    if id_modulo_vendas:
        url_sit = f'https://www.bling.com.br/Api/v3/situacoes?idModulo={id_modulo_vendas}'
        try:
            resp = requests.get(url_sit, headers={'Authorization': f'Bearer {token}'}, timeout=15)
            if resp.status_code == 200:
                dados = resp.json().get('data', [])
                for s in dados:
                    lista_final.append({'id': s['id'], 'nome': s['nome']})
                log(f"✅ Mapeamento concluído: {len(lista_final)} status dinâmicos encontrados.", empresa_id)
                return lista_final
        except: pass

    # PASSO 3: Fallback (Se a descoberta falhar)
    log("⚠️ Não foi possível descobrir status dinamicamente. Usando lista de segurança.", empresa_id)
    # Retorna lista de segurança formatada
    return [{'id': x, 'nome': f'Status {x}'} for x in BACKUP_IDS]

def buscar_detalhe_financeiro(id_pedido, token):
    for _ in range(3):
        try:
            resp = requests.get(f'https://www.bling.com.br/Api/v3/pedidos/vendas/{id_pedido}', headers={'Authorization': f'Bearer {token}'}, timeout=20)
            if resp.status_code == 200:
                d = resp.json().get('data')
                return d[0] if isinstance(d, list) else d
            elif resp.status_code == 429: time.sleep(2)
        except: pass
        time.sleep(1)
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
            INSERT INTO pedidos_vendas (empresa_id, id, numero, data, total, valor_frete, taxa_marketplace, valor_liquido, situacao, id_situacao)
            SELECT empresa_id, id, numero, data, total, valor_frete, taxa_marketplace, valor_liquido, situacao, id_situacao 
            FROM temp_etl_pedidos
            ON DUPLICATE KEY UPDATE
                total=VALUES(total), taxa_marketplace=VALUES(taxa_marketplace), 
                valor_liquido=VALUES(valor_liquido), situacao=VALUES(situacao), 
                id_situacao=VALUES(id_situacao), updated_at=NOW()
        """))
        conn.execute(text("DROP TABLE temp_etl_pedidos"))
        ids_str = ','.join(map(str, ids_lote))
        conn.execute(text(f"DELETE FROM pedidos_itens WHERE empresa_id = {empresa_id} AND pedido_id IN ({ids_str})"))
        if not df_i.empty:
            df_i.to_sql('pedidos_itens', conn, if_exists='append', index=False)

def processar_lista_bruta(dados_api, token, engine, empresa_id, situacao_obj):
    lote_p, lote_i, ids_proc = [], [], []
    for r in dados_api:
        p = buscar_detalhe_financeiro(r['id'], token)
        if not p: continue
        ids_proc.append(p['id'])
        
        tot = float(p.get('total', 0))
        tax = p.get('taxas', {})
        val_com = float(tax.get('taxaComissao', 0))
        val_frete = float(tax.get('custoFrete', 0))
        liq = tot - val_com - val_frete
        
        lote_p.append({
            'id': p['id'], 'numero': str(p.get('numero')), 'data': p['data'],
            'total': tot, 'valor_frete': val_frete, 'taxa_marketplace': val_com,
            'valor_liquido': liq, 'situacao': situacao_obj['nome'], 'id_situacao': situacao_obj['id']
        })
        for i in p.get('itens', []):
            lote_i.append({
                'pedido_id': p['id'], 'codigo_produto': str(i.get('codigo')),
                'descricao': i.get('descricao'), 'quantidade': float(i.get('quantidade', 0)),
                'valor_unitario': float(i.get('valor', 0)), 
                'total_item': float(i.get('quantidade', 0)) * float(i.get('valor', 0))
            })
    if lote_p: salvar_lote(empresa_id, lote_p, lote_i, engine, ids_proc)

def processar_status_inteligente(token, engine, empresa_id, situacao, data_alvo):
    id_sit = situacao['id']
    nome_sit = situacao['nome']
    
    # Verifica se é um status pesado conhecido
    usar_modo_seguro = id_sit in IDS_PESADOS_CONHECIDOS
    total_status = 0
    
    if not usar_modo_seguro:
        log(f"🔎 Varrendo '{nome_sit}' (Modo Rápido)...", empresa_id)
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
                    log(f"⚠️ Loop detectado em '{nome_sit}'. Mudando para Modo Seguro.", empresa_id)
                    usar_modo_seguro = True
                    break
                ultimo_id = dados[0]['id']
                
                if dados[-1]['data'] < data_alvo:
                    validos = [p for p in dados if p['data'] == data_alvo]
                    if validos: processar_lista_bruta(validos, token, engine, empresa_id, situacao)
                    break 

                lote_valido = [p for p in dados if p['data'] == data_alvo]
                if lote_valido: processar_lista_bruta(lote_valido, token, engine, empresa_id, situacao)
                page += 1
            except: break

    if usar_modo_seguro:
        log(f"⚔️ Modo Seguro (Fatiador 48h) para '{nome_sit}'...", empresa_id)
        dias = [data_alvo, (datetime.strptime(data_alvo, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')]
        
        for dia in dias:
            for hora in range(24):
                ts_ini = f"{dia} {hora:02d}:00:00"
                ts_fim = f"{dia} {hora:02d}:59:59"
                for _ in range(3):
                    try:
                        url = (f'https://www.bling.com.br/Api/v3/pedidos/vendas?page=1&limit=100'
                               f'&dataAlteracaoInicial={ts_ini}&dataAlteracaoFinal={ts_fim}'
                               f'&idsSituacoes[]={id_sit}')
                        resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=30)
                        if resp.status_code != 200: time.sleep(2); continue
                        dados = resp.json().get('data', [])
                        if dados:
                            lote_valido = [p for p in dados if p['data'] == data_alvo]
                            if lote_valido:
                                processar_lista_bruta(lote_valido, token, engine, empresa_id, situacao)
                                total_status += len(lote_valido)
                                print(f"   + {len(lote_valido)} pedidos ({ts_ini})", end='\r')
                        break
                    except: time.sleep(5)
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
    
    # 1. DESCOBERTA DINÂMICA
    situacoes = descobrir_situacoes_reais(token, empresa_id)
    
    total_geral = 0
    
    # 2. EXECUÇÃO DINÂMICA
    for sit in situacoes:
        qtd = processar_status_inteligente(token, engine, empresa_id, sit, data_alvo)
        total_geral += qtd
    
    log(f"SUCESSO. Total importado: {total_geral}", empresa_id)
    
    return True, tokens_novos['refresh_token']