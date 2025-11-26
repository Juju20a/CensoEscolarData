CREATE TABLE IF NOT EXISTS tb_instituicao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT NOT NULL,
        nome TEXT NOT NULL,
        co_uf INTEGER NOT NULL,
        co_municipio INTEGER NOT NULL,
        qt_mat_bas INTEGER NOT NULL,
        qt_mat_prof INTEGER NOT NULL,
        qt_mat_esp INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tb_usuario (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        cpf TEXT NOT NULL UNIQUE,
        nascimento DATE NOT NULL
);

-- Adicionando restrição de unicidade para evitar duplicação por código
CREATE UNIQUE INDEX IF NOT EXISTS idx_tb_instituicao_codigo ON tb_instituicao(codigo);
