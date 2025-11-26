CensoEscolarData — migração CSV -> SQLite (Nordeste)

Objetivo
Importar os microdados do Censo Escolar 2024 em CSV para SQLite, filtrando apenas os estados da região Nordeste.

Pré-requisitos
Python 3.8+
pandas (já listado em requirements.txt)
arquivo microdados_ed_basica_2024.csv no diretório raiz do projeto
schema.sql (já presente no projeto) contém as tabelas tb_instituicao e tb_usuario
Como rodar
No PowerShell (Windows):

1) Criar schema e tabelas (opcional):

```powershell
python initdb.py
```

2) Migrar dados do JSON inicial (opcional):

```powershell
python migrate_json_to_sqlite.py --json data\instituicoesensino.json --db censoescolar.db
```

3) Migrar microdados CSV em chunks para SQLite (opcional):

```powershell
python migrate_csv_to_sqlite.py --csv microdados_ed_basica_2024.csv --db censoescolar.db --chunk 200000 --sep ";" --fast
```

Parâmetros úteis:
- `--sep`: separador do CSV (padrão `;` para microdados do Censo),
- `--fast`: habilita otimizações do SQLite (PRAGMA) para acelerar a importação, mas deve ser usado com cautela.
- `--fast`: habilita otimizações do SQLite (PRAGMA) para acelerar a importação. Em `--fast`:
	- PRAGMAs são aplicadas uma vez no início da importação (WAL, synchronous OFF, temp_store MEMORY) para melhorar throughput.
	- O script ainda fará commits por chunk para reduzir o risco de perder dados caso haja erro; para máxima velocidade, é possível fazer uma única transação para toda a importação (recomendado apenas em importações controladas).
- `--dry-run`: mostra quantos registros seriam inseridos sem realizar a inserção.

O script `migrate_csv_to_sqlite.py` faz leitura paginada (chunks) com pandas, filtra por CO_UF (códigos IBGE 21..29) que correspondem aos estados do Nordeste, e insere os registros na tabela `tb_instituicao`. Ajuste `--chunk` para maior/menor consumo de RAM.

Atenção
O script tenta identificar colunas automaticamente, mas dependendo do CSV, você pode precisar ajustar os nomes de coluna no dicionário CANDIDATE_COLUMNS no migrate_csv_to_sqlite.py.
Ajuste o chunksize se quiser mais ou menos memória (chunk maior = menos chamadas de inserção, maior consumo de RAM).
