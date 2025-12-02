from sqlalchemy import create_engine, text
from etl_core_saas import executar_etl_empresa
from datetime import datetime

# --- CONFIG ---
DB_USER = 'sigmacomti'
DB_PASS = 'Sigma#com13ti2025'
DB_HOST = '177.153.209.166' 
DB_NAME = 'sigmacomti'
DB_CONN = f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:3306/{DB_NAME}'

def main():
    data_hoje = datetime.now().strftime('%Y-%m-%d')
    print(f"--- 🚀 ATUALIZAÇÃO HOJE: {data_hoje} ---")
    
    try:
        engine = create_engine(DB_CONN)
        with engine.connect() as conn:
            # ALTERAÇÃO AQUI
            empresas = conn.execute(text("SELECT * FROM empresas_bling WHERE ativo = 1")).mappings().all() # <--- ALTERADO
    except Exception as e:
        print(f"Erro banco: {e}"); return

    for emp in empresas:
        print(f"🏢 {emp['nome_empresa']}...")
        creds = {'client_id': emp['client_id'], 'client_secret': emp['client_secret'], 'refresh_token': emp['refresh_token']}
        
        sucesso, novo_refresh = executar_etl_empresa(emp['id'], creds, engine, data_hoje)
        
        if sucesso and novo_refresh:
            with engine.begin() as conn:
                # ALTERAÇÃO AQUI
                conn.execute(text("UPDATE empresas_bling SET refresh_token = :rt, updated_at = NOW() WHERE id = :id"), # <--- ALTERADO
                             {'rt': novo_refresh, 'id': emp['id']})

if __name__ == "__main__":
    main()