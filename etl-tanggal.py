import mysql.connector
from datetime import datetime, timedelta

# Fungsi koneksi ke database sumber
def connect_source():
    return mysql.connector.connect(
        host="localhost",
        user="",
        password="",
        database=""
    )

# Fungsi koneksi ke data warehouse
def connect_target():
    return mysql.connector.connect(
        host="localhost",
        user="",
        password="",
        database=""
    )

# Ambil data tglsep unik dari 7 hari terakhir
def fetch_unique_tglsep():
    db = connect_source()
    cursor = db.cursor()
    today = datetime.today()
    seven_days_ago = today - timedelta(days=8)
    query = """
        SELECT DISTINCT tglsep FROM bridging_sep
        WHERE tglsep BETWEEN %s AND %s
        ORDER BY tglsep
    """
    try:
        cursor.execute(query, (seven_days_ago.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')))
        results = [row[0] for row in cursor.fetchall()]
        return results
    finally:
        cursor.close()
        db.close()

# Masukkan data ke dim_tanggal
def insert_into_dim_tanggal(tgl_list):
    db = connect_target()
    cursor = db.cursor()
    inserted_count = 0

    for tgl in tgl_list:
        try:
            cursor.execute("SELECT COUNT(*) FROM dim_tanggal WHERE tanggal = %s", (tgl,))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO dim_tanggal (tanggal) VALUES (%s)", (tgl,))
                inserted_count += 1
        except mysql.connector.Error as err:
            print(f"Error saat insert {tgl}: {err}")

    db.commit()
    cursor.close()
    db.close()
    print(f"{inserted_count} tanggal berhasil dimasukkan ke dim_tanggal.")

if __name__ == "__main__":
    tgl_list = fetch_unique_tglsep()
    if tgl_list:
        insert_into_dim_tanggal(tgl_list)
    else:
        print("Tidak ada tanggal untuk dimasukkan.")
