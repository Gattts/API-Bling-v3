from sqlalchemy import create_engine, text
from etl_core_saas import executar_etl_empresa
from datetime import datetime, timedelta
import sys
from urllib.parse import quote_plus

# --- CONFIGURAÇÃO ---
DB_USER = 'sigmacomti'
DB_PASS_RAW = 'Sigma#com13ti2025'
DB_HOST = '177.153.209.166'
DB_NAME = 'sigmacomti'
DB_CONN = f'mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS_RAW)}@{DB_HOST}:3306/{DB_NAME}?connect_timeout=10'

# --- INTERVALO DO BACKFILL ---
DATA_INICIO = '2025-09-01'
DATA_FIM    = '2025-11-30'

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def main():
    print(f"--- 📅 BACKFILL TRIMESTRAL ({DATA_INICIO} a {DATA_FIM}) ---")
    
    try:
        engine = create_engine(DB_CONN)
    except Exception as e:
        print(f"Erro de conexão: {e}"); return

    start = datetime.strptime(DATA_INICIO, "%Y-%m-%d")
    end = datetime.strptime(DATA_FIM, "%Y-%m-%d")

    for single_date in daterange(start, end):
        data_alvo_str = single_date.strftime("%Y-%m-%d")
        print(f"\n🔄 DATA: {data_alvo_str}")

        with engine.connect() as conn:
            # Busca empresas ativas
            result = conn.execute(text("SELECT id, nome_empresa, client_id, client_secret, refresh_token FROM empresas_bling WHERE ativo = 1"))
            empresas = result.mappings().all()

        if not empresas:
            print("⚠️ Nenhuma empresa ativa.")
            break

        for emp in empresas:
            print(f"   🏢 {emp['nome_empresa'] or emp['id']}...")
            
            creds = {
                'client_id': emp['client_id'],
                'client_secret': emp['client_secret'],
                'refresh_token': emp['refresh_token']
            }
            
            # Roda o ETL do dia
            sucesso, novo_refresh = executar_etl_empresa(emp['id'], creds, engine, data_alvo_str)
            
            if sucesso and novo_refresh:
                try:
                    with engine.begin() as update_conn:
                        update_conn.execute(
                            text("UPDATE empresas_bling SET refresh_token = :rt, updated_at = NOW() WHERE id = :id"), 
                            {'rt': novo_refresh, 'id': emp['id']}
                        )
                except: pass
            else:
                print("   ⚠️ Falha ao processar empresa. Tentando próxima data...")

    print("\n--- ✅ BACKFILL CONCLUÍDO ---")

if __name__ == "__main__":
    main()