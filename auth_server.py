from flask import Flask, request, redirect, render_template_string
import requests
import base64
from sqlalchemy import create_engine, text

app = Flask(__name__)

# --- CONFIGURAÇÃO DO BANCO DE DADOS ---
DB_USER = 'sigmacomti'
DB_PASS = 'Sigma#com13ti2025'
DB_HOST = '177.153.209.166' 
DB_NAME = 'sigmacomti'
DB_CONN = f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:3306/{DB_NAME}'

# --- HTML SIMPLES PARA A INTERFACE ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Painel de Autenticação SaaS</title>
    <style>
        body { font-family: Arial, sans-serif; padding: 40px; background-color: #f4f4f9; }
        .container { max-width: 600px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        h2 { color: #333; }
        select { width: 100%; padding: 10px; margin: 10px 0; font-size: 16px; }
        button { width: 100%; padding: 12px; background-color: #28a745; color: white; border: none; font-size: 16px; cursor: pointer; border-radius: 4px; }
        button:hover { background-color: #218838; }
        .item { padding: 10px; border-bottom: 1px solid #eee; }
    </style>
</head>
<body>
    <div class="container">
        <h2>🔐 Renovação de Token (Multi-Empresa)</h2>
        <p>Selecione a empresa que precisa reconectar:</p>
        
        <form action="/iniciar_auth" method="POST">
            <select name="empresa_id" required>
                <option value="" disabled selected>-- Escolha uma empresa --</option>
                {% for emp in empresas %}
                    <option value="{{ emp.id }}">{{ emp.nome_empresa }} (ID: {{ emp.id }})</option>
                {% endfor %}
            </select>
            <br><br>
            <button type="submit">Autenticar no Bling</button>
        </form>
    </div>
</body>
</html>
"""

def get_db_connection():
    return create_engine(DB_CONN)

@app.route('/')
def index():
    """Lista as empresas do banco para o usuário escolher."""
    try:
        engine = get_db_connection()
        with engine.connect() as conn:
            # Busca ID e Nome para montar o dropdown
            query = text("SELECT id, nome_empresa FROM empresas_bling")
            result = conn.execute(query)
            empresas = result.mappings().all()
        
        return render_template_string(HTML_TEMPLATE, empresas=empresas)
    except Exception as e:
        return f"Erro ao conectar no banco: {e}"

@app.route('/iniciar_auth', methods=['POST'])
def iniciar_auth():
    """Busca as credenciais da empresa escolhida e redireciona."""
    empresa_id = request.form.get('empresa_id')
    
    engine = get_db_connection()
    with engine.connect() as conn:
        query = text("SELECT client_id FROM empresas_bling WHERE id = :id")
        empresa = conn.execute(query, {'id': empresa_id}).mappings().first()
    
    if not empresa:
        return "Empresa não encontrada."

    client_id = empresa['client_id']
    
    # O SEGREDO: Passamos o ID da empresa no parâmetro 'state'
    # Assim, quando o Bling devolver o usuário, saberemos qual empresa atualizar.
    url_bling = (
        f"https://www.bling.com.br/Api/v3/oauth/authorize?"
        f"response_type=code&"
        f"client_id={client_id}&"
        f"state={empresa_id}" 
    )
    
    return redirect(url_bling)

@app.route('/callback')
def callback():
    """Recebe o código, busca o segredo no banco e gera o token."""
    code = request.args.get('code')
    empresa_id = request.args.get('state') # Recuperamos o ID aqui!
    
    if not code or not empresa_id:
        return "Erro: Código ou ID da empresa não recebidos."

    try:
        engine = get_db_connection()
        
        # 1. Busca o Client Secret dessa empresa específica no banco
        with engine.connect() as conn:
            query = text("SELECT client_id, client_secret FROM empresas_bling WHERE id = :id")
            empresa = conn.execute(query, {'id': empresa_id}).mappings().first()
            
        if not empresa:
            return "Erro crítico: Empresa não encontrada no banco durante o callback."

        # 2. Troca o CODE pelo TOKEN
        creds = f"{empresa['client_id']}:{empresa['client_secret']}"
        headers = {
            'Authorization': f'Basic {base64.b64encode(creds.encode()).decode()}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        payload = {'grant_type': 'authorization_code', 'code': code}
        
        resp = requests.post('https://www.bling.com.br/Api/v3/oauth/token', headers=headers, data=payload)
        
        if resp.status_code != 200:
            return f"<h1>Erro na API Bling</h1><pre>{resp.text}</pre>"
        
        data = resp.json()
        novo_refresh = data['refresh_token']

        # 3. Salva no Banco
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE empresas_bling SET refresh_token = :rt, updated_at = NOW() WHERE id = :id"),
                {'rt': novo_refresh, 'id': empresa_id}
            )
            
        return f"""
        <div style="font-family: Arial; text-align: center; padding-top: 50px;">
            <h1 style="color:green">SUCESSO! ✅</h1>
            <p>O Token da <b>Empresa ID {empresa_id}</b> foi renovado no banco de dados.</p>
            <p>Você já pode fechar esta janela e rodar os scripts de ETL.</p>
            <br>
            <a href="/">Voltar para a lista</a>
        </div>
        """

    except Exception as e:
        return f"Erro interno: {e}"

if __name__ == '__main__':
    print("🚀 Servidor Dinâmico rodando em http://localhost:5000")
    app.run(port=5000)