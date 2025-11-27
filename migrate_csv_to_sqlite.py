"""
Script: Migrate CSV microdados (Censo Escolar 2024) into SQLite using pandas chunked read.

Usage:
    python migrate_csv_to_sqlite.py --csv microdados_ed_basica_2024.csv --db censoescolar.db --chunk 200000

Notes:
- Filters rows by CO_UF codes 21..29 (Nordeste states) by default.
- Attempts to detect column names; if CSV uses different names adjust the `CANDIDATE_COLUMNS` mapping.
- It will insert only if the `codigo` (entity code) does not already exist.
"""
import argparse
import sqlite3
import os
import pandas as pd

DEFAULT_DB = "censoescolar.db"
DEFAULT_CSV = "microdados_ed_basica_2024.csv"
DEFAULT_CHUNK = 200000

# Candidate column names that might exist in different CSV versions.
CANDIDATE_COLUMNS = {
    'codigo': ['CO_ENTIDADE', 'CO_ENTIDADE_ESCOLA', 'CO_ENTIDADE_MEC', 'COD_ENTIDADE', 'CO_ENTIDADE_ENSINO', 'CO_ENTIDADE_CURSO'],
    'nome': ['NO_ENTIDADE', 'NO_ESCOLA', 'NOME_ENTIDADE'],
    'co_uf': ['CO_UF'],
    'co_municipio': ['CO_MUNICIPIO'],
    'qt_mat_bas': ['QT_MAT_BAS', 'NU_MATRICULAS_BASICA', 'QT_MATRICULAS_BAS'],
    'qt_mat_prof': ['QT_MAT_PROF', 'NU_MATRICULAS_PROF'],
    'qt_mat_esp': ['QT_MAT_ESP', 'NU_MATRICULAS_ESP']
}


def find_column(columns, candidates):
    for c in candidates:
        if c in columns:
            return c
    return None


def load_schema(db_path: str, schema_file: str = 'schema.sql'):
    conn = sqlite3.connect(db_path)
    with open(schema_file, 'r', encoding='utf-8') as f:
        conn.executescript(f.read())
    conn.close()


def migrate_csv(csv_file: str, db_path: str, chunk_size: int = DEFAULT_CHUNK, filter_nordeste=True, sep=';', fast=False, dry_run=False):
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    load_schema(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    inserted_total = 0
    skipped_total = 0
    processed_total = 0

    columns_printed = False
    chunk_idx = 0
    # Apply PRAGMA settings and begin transaction when in fast mode
    if fast:
        # PRAGMA changes at connection scope
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = OFF;")
        conn.execute("PRAGMA temp_store = MEMORY;")

    for chunk in pd.read_csv(csv_file, sep=sep, chunksize=chunk_size, dtype=str, low_memory=True):
        processed_total += len(chunk)
        # Normalize column names
        cols = chunk.columns.tolist()

        codigo_col = find_column(cols, CANDIDATE_COLUMNS['codigo'])
        nome_col = find_column(cols, CANDIDATE_COLUMNS['nome'])
        co_uf_col = find_column(cols, CANDIDATE_COLUMNS['co_uf'])
        co_municipio_col = find_column(cols, CANDIDATE_COLUMNS['co_municipio'])
        qt_mat_bas_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_bas'])
        qt_mat_prof_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_prof'])
        qt_mat_esp_col = find_column(cols, CANDIDATE_COLUMNS['qt_mat_esp'])

        # Print detected mapping once for debugging/confirmation
        if not columns_printed:
            print("Detected CSV columns (first chunk):", cols)
            print("Detected mapping:")
            print(f"  codigo: {codigo_col}")
            print(f"  nome: {nome_col}")
            print(f"  co_uf: {co_uf_col}")
            print(f"  co_municipio: {co_municipio_col}")
            print(f"  qt_mat_bas: {qt_mat_bas_col}")
            print(f"  qt_mat_prof: {qt_mat_prof_col}")
            print(f"  qt_mat_esp: {qt_mat_esp_col}")
            columns_printed = True

        if not codigo_col or not nome_col or not co_uf_col:
            print("Cannot identify essential columns in CSV chunk; skipping chunk. Check CANDIDATE_COLUMNS mapping.")
            continue

        # Filter by CO_UF codes for Nordeste (21..29 inclusive), if required
        if filter_nordeste:
            # co_uf might be string; convert to numeric where possible
            try:
                chunk[co_uf_col] = pd.to_numeric(chunk[co_uf_col], errors='coerce')
                chunk = chunk[chunk[co_uf_col].between(21, 29, inclusive=True)]
            except Exception:
                # If conversion fails, attempt simple string startswith checks
                chunk = chunk[chunk[co_uf_col].astype(str).str.startswith(tuple(str(x) for x in range(21, 30)))]

        # Prepare insertion list
        insert_rows = []
        codes_in_chunk = []

        for idx, row in chunk.iterrows():
            codigo = row.get(codigo_col)
            if pd.isna(codigo):
                skipped_total += 1
                continue

            # Check if it already exists
            cursor.execute("SELECT id FROM tb_instituicao WHERE codigo = ?", (str(codigo),))
            if cursor.fetchone() is not None:
                skipped_total += 1
                continue

            nome = row.get(nome_col) if nome_col else ''
            try:
                co_uf_val = int(row.get(co_uf_col)) if co_uf_col and not pd.isna(row.get(co_uf_col)) else 0
            except Exception:
                co_uf_val = 0
            try:
                co_mun_val = int(row.get(co_municipio_col)) if co_municipio_col and not pd.isna(row.get(co_municipio_col)) else 0
            except Exception:
                co_mun_val = 0
            try:
                qt_mat_bas = int(row.get(qt_mat_bas_col)) if qt_mat_bas_col and not pd.isna(row.get(qt_mat_bas_col)) else 0
            except Exception:
                qt_mat_bas = 0
            try:
                qt_mat_prof = int(row.get(qt_mat_prof_col)) if qt_mat_prof_col and not pd.isna(row.get(qt_mat_prof_col)) else 0
            except Exception:
                qt_mat_prof = 0
            try:
                qt_mat_esp = int(row.get(qt_mat_esp_col)) if qt_mat_esp_col and not pd.isna(row.get(qt_mat_esp_col)) else 0
            except Exception:
                qt_mat_esp = 0

            insert_rows.append((str(codigo), str(nome), co_uf_val, co_mun_val, qt_mat_bas, qt_mat_prof, qt_mat_esp))
            codes_in_chunk.append(str(codigo))

        # For performance: use INSERT OR IGNORE and rely on unique index on tb_instituicao(codigo)
        # This avoids large parameterized IN queries; we measure inserted by checking conn.total_changes.
        dup_count = 0

        # Bulk insert (or dry-run)
        if insert_rows:
            if fast:
                # If we want to reduce the number of commits further, we could begin a long transaction here.
                # Keeping commit per chunk reduces risk of losing lots of data on failure.
                pass
            if not dry_run and insert_rows:
                # Track total changes before and after to know how many rows were actually inserted.
                before_changes = conn.total_changes
                # Use INSERT OR IGNORE to avoid errors on duplicate codigo
                cursor.executemany(
                    "INSERT OR IGNORE INTO tb_instituicao (codigo, nome, co_uf, co_municipio, qt_mat_bas, qt_mat_prof, qt_mat_esp) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    insert_rows
                )
                if fast:
                    # If fast, we'll commit per chunk, but PRAGMAs make it faster
                    conn.commit()
                else:
                    conn.commit()
                after_changes = conn.total_changes
                added = after_changes - before_changes
            else:
                added = 0

            inserted_total += added
            dup_count = len(insert_rows) - added
        # Stats per chunk
        chunk_idx += 1
        total_in_chunk = len(chunk)
        total_filtered = len(chunk.index)
        print(f"Chunk {chunk_idx}: total rows read={total_in_chunk}, after filter={total_filtered}, new_inserts={len(insert_rows)}, duplicates_in_db={dup_count}")

    conn.close()

    print(f"Processed rows: {processed_total}, Inserted: {inserted_total}, Skipped: {skipped_total}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Migrate CSV microdados (Censo Escolar) to SQLite using pandas chunked read')
    parser.add_argument('--csv', required=True, help='Path to CSV file with microdados')
    parser.add_argument('--db', default=DEFAULT_DB, help='SQLite database path')
    parser.add_argument('--chunk', default=DEFAULT_CHUNK, type=int, help='Chunk size for pandas read_csv')
    parser.add_argument('--sep', default=';', help='CSV separator (default: ; )')
    parser.add_argument('--fast', action='store_true', help='Enable fast PRAGMA settings during import')
    parser.add_argument('--dry-run', action='store_true', help='Do not insert rows; only show counts')
    parser.add_argument('--no-filter', dest='filter', action='store_false', help='Do not filter by Northeast (CO_UF 21..29)')

    args = parser.parse_args()
    migrate_csv(args.csv, args.db, chunk_size=args.chunk, filter_nordeste=args.filter, sep=args.sep, fast=args.fast, dry_run=args.dry_run)
