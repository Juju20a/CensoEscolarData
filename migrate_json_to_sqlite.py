"""
Script: Migrate JSON file `data/instituicoesensino.json` into SQLite database `censoescolar.db`.

Usage:
    python migrate_json_to_sqlite.py --json data/instituicoesensino.json --db censoescolar.db

- Creates tables using `schema.sql` if they do not exist.
- Inserts unique entries (by `codigo`) into `tb_instituicao`.

"""
import sqlite3
import argparse
import json
import os

DEFAULT_DB = "censoescolar.db"
DEFAULT_JSON = "data/instituicoesensino.json"


def load_schema(db_path: str, schema_file: str = 'schema.sql'):
    conn = sqlite3.connect(db_path)
    with open(schema_file, 'r', encoding='utf-8') as f:
        conn.executescript(f.read())
    conn.close()


def migrate_json(json_file: str, db_path: str):
    if not os.path.exists(json_file):
        raise FileNotFoundError(f"Json file not found: {json_file}")

    # Ensure schema exists
    load_schema(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    inserted = 0
    skipped = 0

    for record in data:
        codigo = record.get('codigo')
        nome = record.get('nome')
        co_uf = int(record.get('co_uf')) if record.get('co_uf') not in [None, ''] else 0
        co_municipio = int(record.get('co_municipio')) if record.get('co_municipio') not in [None, ''] else 0
        qt_mat_bas = int(record.get('qt_mat_bas') or 0)
        qt_mat_prof = int(record.get('qt_mat_prof') or 0)
        qt_mat_esp = int(record.get('qt_mat_esp') or 0)

        # Check if codigo already exists
        cursor.execute("SELECT id FROM tb_instituicao WHERE codigo = ?", (codigo,))
        if cursor.fetchone() is not None:
            skipped += 1
            continue

        cursor.execute(
            "INSERT INTO tb_instituicao(codigo, nome, co_uf, co_municipio, qt_mat_bas, qt_mat_prof, qt_mat_esp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (codigo, nome, co_uf, co_municipio, qt_mat_bas, qt_mat_prof, qt_mat_esp)
        )
        inserted += 1

    conn.commit()
    conn.close()

    print(f"Finished. Inserted: {inserted}, Skipped: {skipped}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Migrate JSON institutions to SQLite')
    parser.add_argument('--json', default=DEFAULT_JSON, help='Path to JSON file with institutions')
    parser.add_argument('--db', default=DEFAULT_DB, help='SQLite database path')

    args = parser.parse_args()
    migrate_json(args.json, args.db)
