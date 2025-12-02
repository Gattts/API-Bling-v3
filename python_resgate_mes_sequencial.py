import requests
import pandas as pd
from sqlalchemy import create_engine, text
import base64
import time
import sys
from datetime import datetime
from urllib.parse import quote_plus

# --- CONFIGURAÇÃO DO BANCO ---
DB_USER = 'sigmacomti'
DB_PASS_RAW = 'Sigma#com13ti2025'
DB_HOST = '177.153.209.166'
DB_NAME = 'sigmacomti'
DB_CONN = f'mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS_RAW)}@{DB_HOST}:3306/{DB_NAME}?connect_timeout=10'

EMPRESA_ID = 1

# Tenta importar funções, se falhar avisa
try:
    from etl_core_saas import buscar_detalhe_financeiro, salvar_lote, listar_lojas_da_conta, get_valid_token
except ImportError:
    print("❌ ERRO: O arquivo 'etl_core_saas.py' não está na mesma pasta!")
    sys.exit()

def main():
    # INICIALIZA VARIÁVEIS ANTES DE TUDO (Para evitar erro no final se quebrar)
    cnt_salvos = 0
    
    print(f"--- 📅 RESGATE DE MÊS POR SEQUÊNCIA NUMÉRICA ---")
    
    try:
        # 1. Coleta dos Limites
        print("\n👉 Vá no Bling > Vendas > Filtre por Data (Ex: 01/09 a 30/09)")
        print("👉 Ordene por Número. Pegue o PRIMEIRO e o ÚLTIMO da lista.")
        
        try:
            n_str_ini = input("Digite o NÚMERO do PRIMEIRO pedido: ").strip()
            n_str_fim = input("Digite o NÚMERO do ÚLTIMO pedido:   ").strip()
            
            if not n_str_ini or not n_str_fim: return
            
            n_inicio = int(n_str_ini)
            n_fim    = int(n_str_fim)
        except ValueError:
            print("❌ Digite apenas números.")
            return

        if n_inicio >= n_fim:
            print("❌ O número final deve ser maior que o inicial.")
            return
        
        total_range = n_fim - n_inicio + 1
        print(f"\n📊 Intervalo total: {total_range} números.")

        # 2. Conexão e Auth
        engine = create_engine(DB_CONN)
        
        print("🔌 Autenticando...")
        with engine.connect() as conn:
            res = conn.execute(text("SELECT client_id, client_secret, refresh_token FROM empresas_bling WHERE id = :id"), {'id': EMPRESA_ID}).mappings().first()
        
        creds = {'client_id': res['client_id'], 'client_secret': res['client_secret'], 'refresh_token': res['refresh_token']}
        tokens = get_valid_token(creds)
        if not tokens: 
            print("❌ Erro Auth")
            return
        
        token = tokens['access_token']
        
        with engine.begin() as conn:
             conn.execute(text("UPDATE empresas_bling SET refresh_token = :rt WHERE id = 1"), {'rt': tokens['refresh_token']})

        mapa_lojas = listar_lojas_da_conta(token, EMPRESA_ID)
        
        # 3. Análise de Gap
        print("📥 Analisando o que já temos no banco...")
        with engine.connect() as conn:
            q = text(f"""
                SELECT CAST(numero AS UNSIGNED) 
                FROM pedidos_vendas 
                WHERE empresa_id = {EMPRESA_ID} 
                AND CAST(numero AS UNSIGNED) BETWEEN {n_inicio} AND {n_fim}
            """)
            res_db = conn.execute(q).fetchall()
            numeros_ja_temos = set(row[0] for row in res_db)
        
        print(f"   ✅ {len(numeros_ja_temos)} pedidos já estão salvos.")
        
        # Calcula faltantes
        todos_os_numeros = set(range(n_inicio, n_fim + 1))
        numeros_faltantes = sorted(list(todos_os_numeros - numeros_ja_temos))
        
        qtd_falta = len(numeros_faltantes)
        print(f"   🔥 FALTAM {qtd_falta} PEDIDOS.")
        
        if qtd_falta == 0:
            print("🎉 Nada para baixar!")
            return

        if input(f"👉 Iniciar resgate? (S/N): ").upper() != 'S': return

        # 4. O Loop de Resgate
        cnt_nao_existe = 0
        
        for i, num in enumerate(numeros_faltantes):
            str_num = str(num)
            
            # Barra de progresso
            prog = ((i + 1) / qtd_falta) * 100
            print(f"⏳ {prog:.1f}% | Buscando: {str_num} | ✅ Salvos: {cnt_salvos} | ⚪ 404: {cnt_nao_existe}", end='\r', flush=True)
            
            try:
                url = f'https://www.bling.com.br/Api/v3/pedidos/vendas?numero={str_num}'
                resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=8)
                
                if resp.status_code == 200:
                    dados = resp.json().get('data', [])
                    
                    if dados:
                        # Achou! Baixa detalhes e salva
                        lote_p, lote_i, ids_proc = [], [], []
                        
                        for r in dados:
                            p = buscar_detalhe_financeiro(r['id'], token)
                            if not p: continue
                            ids_proc.append(p['id'])
                            
                            tot = float(p.get('total', 0))
                            tax = p.get('taxas', {})
                            val_com = float(tax.get('taxaComissao', 0))
                            val_frete = float(tax.get('custoFrete', 0))
                            liq = tot - val_com - val_frete
                            id_loja = p.get('loja', {}).get('id')
                            nome_loja = mapa_lojas.get(id_loja, f"Loja {id_loja}")
                            
                            sit_obj = p.get('situacao', {})
                            sit_nome = sit_obj.get('valor', sit_obj.get('nome', 'Resgatado'))
                            sit_id = sit_obj.get('id')

                            lote_p.append({
                                'id': p['id'], 'numero': str(p.get('numero')), 'data': p['data'],
                                'id_loja': id_loja, 'nome_loja': nome_loja,
                                'total': tot, 'valor_frete': val_frete, 'taxa_marketplace': val_com,
                                'valor_liquido': liq, 
                                'situacao': sit_nome, 'id_situacao': sit_id
                            })
                            
                            for item in p.get('itens', []):
                                lote_i.append({
                                    'pedido_id': p['id'], 'codigo_produto': str(item.get('codigo')),
                                    'descricao': item.get('descricao'), 'quantidade': float(item.get('quantidade', 0)),
                                    'valor_unitario': float(item.get('valor', 0)), 
                                    'total_item': float(item.get('quantidade', 0)) * float(item.get('valor', 0))
                                })
                        
                        if lote_p:
                            salvar_lote(EMPRESA_ID, lote_p, lote_i, engine, ids_proc)
                            cnt_salvos += 1
                    else:
                        cnt_nao_existe += 1
                
                elif resp.status_code == 429:
                    time.sleep(2)
                
                time.sleep(0.15)
                
            except Exception:
                pass

    except Exception as e:
        print(f"\n\n❌ Erro Geral: {e}")
        print("Dica: Verifique se você rodou 'pip install pymysql' no Codespaces.")

    print(f"\n\n🏁 FIM. Novos pedidos inseridos: {cnt_salvos}")

if __name__ == "__main__":
    main()