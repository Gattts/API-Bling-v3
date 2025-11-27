from sqlalchemy import create_engine, text
from etl_core_saas import executar_etl_empresa
import time

# --- CONFIGURAÇÃO DO BANCO DE DADOS REMOTO ---
# Preenchido com base no seu print anterior
DB_USER = 'sigmacomti'
DB_PASS = 'Sigma#com13ti2025'
DB_HOST = '177.153.209.166' 
DB_NAME = 'sigmacomti'

# String de Conexão (SQLAlchemy)
DB_CONN = f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:3306/{DB_NAME}'

def main():
    print("--- 🚀 INICIANDO SISTEMA SAAS (MULTI-EMPRESA) ---")
    
    try:
        # 1. Conecta no Banco Central
        engine = create_engine(DB_CONN)
        conn = engine.connect()
        print("✅ Conexão com o banco de dados estabelecida!")
    except Exception as e:
        print(f"❌ Erro crítico ao conectar no banco: {e}")
        return

    # 2. Busca empresas ativas
    print("📥 Buscando empresas ativas...")
    try:
        query = text("SELECT id, nome_empresa, client_id, client_secret, refresh_token FROM empresas WHERE ativo = 1")
        result = conn.execute(query)
        empresas = result.mappings().all()
    except Exception as e:
        print(f"❌ Erro ao ler tabela empresas: {e}")
        conn.close()
        return
    
    conn.close() # Libera a conexão principal, o ETL cria a dele

    if not empresas:
        print("⚠️ Nenhuma empresa ativa encontrada na fila.")
        return

    print(f"📋 {len(empresas)} empresas na fila para processamento.")

    # 3. Loop de Execução (Itera sobre cada cliente)
    for emp in empresas:
        nome = emp['nome_empresa'] or f"Empresa {emp['id']}"
        print(f"\n🏢 Processando: {nome} (ID: {emp['id']})...")
        
        creds = {
            'client_id': emp['client_id'],
            'client_secret': emp['client_secret'],
            'refresh_token': emp['refresh_token']
        }
        
        # --- CHAMA O CORE (A inteligência do ETL) ---
        # Ele vai descobrir status, decidir entre modo rápido/seguro e salvar os dados
        sucesso, novo_refresh = executar_etl_empresa(
            empresa_id=emp['id'], 
            creds_dict=creds, 
            engine=engine
        )
        
        # 4. Atualização do Token (Rotatividade de Segurança)
        if sucesso and novo_refresh:
            print(f"   💾 Sucesso! Atualizando token no banco...")
            try:
                with engine.begin() as update_conn:
                    update_conn.execute(
                        text("UPDATE empresas SET refresh_token = :rt, updated_at = NOW() WHERE id = :id"), 
                        {'rt': novo_refresh, 'id': emp['id']}
                    )
                print("   ✅ Token salvo. Empresa atualizada e pronta para a próxima.")
            except Exception as e:
                print(f"   ❌ Erro grave ao salvar novo token no banco: {e}")
        else:
            print(f"   ❌ Falha ao processar a empresa {emp['id']}. O token antigo foi mantido (pode estar expirado).")

    print("\n--- 🏁 FIM DO CICLO ---")

if __name__ == "__main__":
    main()