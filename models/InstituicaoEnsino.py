class InstituicaoEnsino():

    # REMOVIDO: O campo qt_mat_eja, para ser consistente com o schema.sql
    def __init__(self, codigo, nome, co_uf, co_municipio, qt_mat_bas, qt_mat_prof, qt_mat_esp):
        self.codigo = codigo
        self.nome = nome
        self.co_uf = co_uf
        self.co_municipio = co_municipio
        self.qt_mat_bas = qt_mat_bas
        self.qt_mat_prof = qt_mat_prof
        # Removido qt_mat_eja para consistência
        self.qt_mat_esp = qt_mat_esp

    def __repr__(self):
        return f'<InstituicaoEnsino {self.codigo}>'

    def to_json(self):
        # ATENÇÃO: Se esta classe for usada para retornar dados detalhados
        # ela deveria retornar todos os campos, mas a versão atual retorna só código e nome.
        # Vou manter como estava, mas você pode querer incluir todos os campos de matrícula aqui
        # para a rota GET /instituicoesensino/<id> (que foi resolvida no app.py)
        return {
            "codigo": self.codigo, 
            "nome": self.nome,
            # Se fosse para detalhes, seria:
            # "co_uf": self.co_uf, 
            # "qt_mat_bas": self.qt_mat_bas,
            # ...
        }
