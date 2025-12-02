from sqlalchemy import create_engine, text
from etl_core_saas import executar_etl_empresa
import time

# --- CONFIGURAÇÃO ---
DB_USER = 'sigmacomti'
DB_PASS = 'Sigma#com13ti2025'
DB_HOST = '177.153.209.166' 
DB_NAME = 'sigmacomti'
DB_CONN = f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:3306/{DB_NAME}'

def main():
    print("--- 🚀 INICIANDO SISTEMA SAAS (MULTI-EMPRESA) ---")
    
    try:
        engine = create_engine(DB_CONN)
        conn = engine.connect()
        print("✅ Conexão estabelecida!")
    except Exception as e:
        print(f"❌ Erro conexão: {e}"); return

    print("📥 Buscando empresas ativas...")
    try:
        # ALTERAÇÃO AQUI: Tabela agora é 'empresas_bling'
        query = text("SELECT id, nome_empresa, client_id, client_secret, refresh_token FROM empresas_bling WHERE ativo = 1") # <--- ALTERADO
        result = conn.execute(query)
        empresas = result.mappings().all()
    except Exception as e:
        print(f"❌ Erro SQL: {e}")
        conn.close(); return
    
    conn.close()

    if not empresas:
        print("⚠️ Nenhuma empresa ativa.")
        return

    for emp in empresas:
        nome = emp['nome_empresa'] or f"Empresa {emp['id']}"
        print(f"\n🏢 Processando: {nome} (ID: {emp['id']})...")
        
        creds = {
            'client_id': emp['client_id'],
            'client_secret': emp['client_secret'],
            'refresh_token': emp['refresh_token']
        }
        
        sucesso, novo_refresh = executar_etl_empresa(emp['id'], creds, engine)
        
        if sucesso and novo_refresh:
            print(f"   💾 Atualizando token...")
            try:
                with engine.begin() as update_conn:
                    # ALTERAÇÃO AQUI: Update na tabela 'empresas_bling'
                    update_conn.execute(
                        text("UPDATE empresas_bling SET refresh_token = :rt, updated_at = NOW() WHERE id = :id"), # <--- ALTERADO
                        {'rt': novo_refresh, 'id': emp['id']}
                    )
                print("   ✅ Token salvo.")
            except Exception as e:
                print(f"   ❌ Erro ao salvar token: {e}")
        else:
            print(f"   ❌ Falha na empresa {emp['id']}.")

    print("\n--- 🏁 FIM DO CICLO ---")

if __name__ == "__main__":
    main()