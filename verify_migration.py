"""
Small verification script to check data counts and a sample after import
Usage:
  python verify_migration.py --db censoescolar.db --sample 5 --nordeste
"""
import sqlite3
import argparse

DEFAULT_DB = 'censoescolar.db'


def verify(db_path: str, sample: int = 5, nordeste: bool = True):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM tb_instituicao")
    total = cursor.fetchone()[0]

    if nordeste:
        cursor.execute("SELECT COUNT(*) FROM tb_instituicao WHERE co_uf BETWEEN 21 AND 29")
        total_nordeste = cursor.fetchone()[0]
    else:
        total_nordeste = None

    print(f"Total registros: {total}")
    if nordeste:
        print(f"Total registros Nordeste (CO_UF 21..29): {total_nordeste}")

    print(f"Mostrando {sample} amostras:")
    if nordeste:
        cursor.execute("SELECT id, codigo, nome, co_uf, qt_mat_bas FROM tb_instituicao WHERE co_uf BETWEEN 21 AND 29 LIMIT ?", (sample,))
    else:
        cursor.execute("SELECT id, codigo, nome, co_uf, qt_mat_bas FROM tb_instituicao LIMIT ?", (sample,))

    rows = cursor.fetchall()

    for row in rows:
        print(row)

    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Verify SQLite migration results')
    parser.add_argument('--db', default=DEFAULT_DB, help='SQLite database path')
    parser.add_argument('--sample', default=5, type=int, help='Number of sample records to show')
    parser.add_argument('--nordeste', action='store_true', default=True, help='Show nordeste counts and sample (CO_UF 21..29)')

    args = parser.parse_args()
    verify(args.db, sample=args.sample, nordeste=args.nordeste)
