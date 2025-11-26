import sqlite3
from flask import Flask, request, jsonify
from datetime import datetime

# Importamos apenas o modelo, removendo a importação de helpers/data
from models.Usuario import Usuario


app = Flask(__name__)
DATABASE_NAME = "censoescolar.db"

# REMOVIDO: Variáveis globais em memória (usuarios, instituicoesEnsino)

def is_data_valida(data_string):
    """Verifica se a string está no formato YYYY-MM-DD."""
    try:
        datetime.strptime(data_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False

# --- Endpoints ---

@app.get("/")
def index():
    """Endpoint de saúde/versão."""
    return '{"versao":"2.0.0"}', 200

@app.get("/usuarios")
def getUsuarios():
    """
    Endpoint para buscar todos os usuários no SQLite.
    Migrado: Não usa mais a lista 'usuarios' em memória.
    """
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # Consulta todos os usuários
    statement = "SELECT id, nome, cpf, nascimento FROM tb_usuario"
    cursor.execute(statement)
    resultset = cursor.fetchall()

    conn.close()

    usuarios_response = []
    # Cria objetos Usuario ou dicionários a partir do resultado
    for row in resultset:
        # Assumindo que a classe Usuario possui um to_json()
        u = Usuario(row[0], row[1], row[2], row[3])
        usuarios_response.append(u.to_json()) 

    return jsonify(usuarios_response), 200

@app.get("/usuarios/<int:id>")
def getUsuariosById(id: int):
    """
    Endpoint para buscar um usuário específico pelo ID no SQLite.
    Migrado: Não usa mais a lista 'usuarios' em memória.
    """
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # Consulta um usuário pelo ID
    statement = "SELECT id, nome, cpf, nascimento FROM tb_usuario WHERE id = ?"
    cursor.execute(statement, (id,))
    row = cursor.fetchone()

    conn.close()

    if row is None:
        return {"mensagem": "Usuário não encontrado"}, 404
    
    # Cria objeto Usuario a partir da linha e retorna o JSON
    user = Usuario(row[0], row[1], row[2], row[3])
    return jsonify(user.to_json()), 200

@app.post("/usuarios")
def setUsuario():
    """
    Endpoint para criar um novo usuário, com tratamento de CPF duplicado.
    Refatorado: Não atualiza lista em memória e verifica unicidade do CPF.
    """
    usuarioJson = request.get_json()

    # Validação simples
    nome = usuarioJson.get('nome', '')
    if (not nome) or (not all(part.isalpha() for part in nome.split())):
        return {"mensagem": "O nome do usuário é inválido!"}, 400

    cpf = usuarioJson.get('cpf', '')
    if (not cpf) or (len(cpf) != 11) or (not cpf.isdigit()):
        return {"mensagem": "O cpf do usuário é inválido!"}, 400

    nascimento = usuarioJson.get('nascimento', '')
    if not is_data_valida(nascimento):
        return {"mensagem": "A data de nascimento do usuário é inválida!"}, 400

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    try:
        # 1. Tenta inserir no banco. Se o CPF for UNIQUE (como recomendado), falhará aqui se duplicado.
        statement = "INSERT INTO tb_usuario(nome, cpf, nascimento) values(?, ?, ?)"
        cursor.execute(statement, (nome, cpf, nascimento))
        
        id = cursor.lastrowid
        conn.commit()

        # Adicionar id do registro criado ao usuário de retorno (sem atualizar lista em memória)
        usuarioJson['id'] = id
        return usuarioJson, 201

    except sqlite3.IntegrityError:
        # Captura erro de CPF duplicado (se UNIQUE estiver definido no schema.sql)
        conn.rollback()
        return {"mensagem": "CPF já cadastrado. Usuário duplicado."}, 400
        
    finally:
        conn.close()


@app.get("/instituicoesensino")
def listar_instituicoes_paginadas():
    """
    Endpoint para buscar instituições, agora com PAGINAÇÃO (LIMIT e OFFSET).
    Ajuste: Necessário para lidar com grande volume de dados do Censo.
    """
    try:
        # Parâmetros de paginação, com valores padrão seguros
        limit = int(request.args.get('limit', 50))  # Padrão de 50 registros por página
        offset = int(request.args.get('offset', 0)) # Padrão começa em 0 (primeira página)
        
        # Garante que os valores são válidos e positivos
        limit = max(1, min(limit, 100)) # Limita o máximo para não sobrecarregar
        offset = max(0, offset)

    except ValueError:
        return {"mensagem": "Parâmetros 'limit' e 'offset' devem ser números inteiros."}, 400

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Consulta com LIMIT e OFFSET para paginação
    # Seleciona apenas os campos essenciais para a lista
    statement = (
        f"SELECT id, codigo, nome FROM tb_instituicao LIMIT {limit} OFFSET {offset}"
    )
    cursor.execute(statement)

    resultset = cursor.fetchall()
    conn.close()

    instituicaoEnsinoResponse = []
    for row in resultset:
        instituicaoEnsino = {"id": row[0], "codigo": row[1], "nome": row[2]}
        instituicaoEnsinoResponse.append(instituicaoEnsino)

    return jsonify(instituicaoEnsinoResponse), 200

@app.get("/instituicoesensino/<int:id>")
def getInstituicoesEnsinoById(id: int):
    """
    Endpoint para buscar uma instituição específica pelo ID no SQLite.
    Migrado: Não usa mais a lista 'instituicoesEnsino' em memória.
    """
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    # Consulta a instituição pelo ID (PK)
    statement = (
        "SELECT id, codigo, nome, co_uf, co_municipio, qt_mat_bas, qt_mat_prof, qt_mat_esp "
        "FROM tb_instituicao WHERE id = ?"
    )
    cursor.execute(statement, (id,))
    row = cursor.fetchone()

    conn.close()

    if row is None:
        return {"mensagem": "Instituição não encontrada"}, 404
    
    # Monta a resposta com todos os dados do banco
    ieDict = {
        "id": row[0],
        "codigo": row[1],
        "nome": row[2],
        "co_uf": row[3],
        "co_municipio": row[4],
        "qt_mat_bas": row[5],
        "qt_mat_prof": row[6],
        "qt_mat_esp": row[7],
        # REMOVIDO: qt_mat_eja (inconsistente com o schema)
    }
    return jsonify(ieDict), 200

if __name__ == '__main__':
    # Adicionar host='0.0.0.0' para garantir acessibilidade em alguns ambientes
    app.run(debug=True, host='0.0.0.0')
