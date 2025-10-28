import mysql.connector
from datetime import datetime, timedelta

# Koneksi ke database
db_mut_2023 = mysql.connector.connect(
    host="localhost",
    user="",
    password="",
    database=""
)
cursor_mut_2023 = db_mut_2023.cursor(dictionary=True)

# Koneksi ke data warehouse
db_dw_mut = mysql.connector.connect(
    host="localhost",
    user="",
    password="",
    database=""
)
cursor_dw_mut = db_dw_mut.cursor()

def fetch_data_for_last_7_days():
    today = datetime.today()
    seven_days_ago = today - timedelta(days=8)
    
    today_str = today.strftime('%Y-%m-%d')
    seven_days_ago_str = seven_days_ago.strftime('%Y-%m-%d')

    query = """
    SELECT * FROM bridging_sep
    WHERE tglsep BETWEEN %s AND %s
    """

    try:
        cursor_mut_2023.execute(query, (seven_days_ago_str, today_str))
        results = cursor_mut_2023.fetchall()

        if not results:
            print(f"Tidak ada data untuk rentang tanggal {seven_days_ago_str} hingga {today_str}")
            return

        # Insert tanggal ke dim_tanggal 
        date_fields = ['tglsep', 'tglrujukan', 'tglpulang', 'tglkkl']
        unique_dates = set()

        for row in results:
            for field in date_fields:
                if row.get(field):
                    unique_dates.add(row[field])

        insert_dim_query = "INSERT IGNORE INTO dim_tanggal (tanggal) VALUES (%s)"
        for date in unique_dates:
            cursor_dw_mut.execute(insert_dim_query, (date,))

        # Insert / Update ke dw_mutiara sep 
        insert_query = """
        INSERT INTO sep (
            no_sep, no_rawat, tglsep, tglrujukan, no_rujukan, kdppkrujukan,
            nmppkrujukan, kdppkpelayanan, nmppkpelayanan, jnspelayanan, catatan,
            diagawal, nmdiagnosaawal, kdpolitujuan, nmpolitujuan, klsrawat, klsnaik,
            pembiayaan, pjnaikkelas, lakalantas, user, nomr, nama_pasien, tanggal_lahir,
            peserta, jkel, no_kartu, tglpulang, asal_rujukan, eksekutif, cob, notelep,
            katarak, tglkkl, keterangankkl, suplesi, no_sep_suplesi, kdprop, nmprop,
            kdkab, nmkab, kdkec, nmkec, noskdp, kddpjp, nmdpdjp, tujuankunjungan,
            flagprosedur, penunjang, asesmenpelayanan, kddpjplayanan, nmdpjplayanan
        ) VALUES (
            %(no_sep)s, %(no_rawat)s, %(tglsep)s, %(tglrujukan)s, %(no_rujukan)s, %(kdppkrujukan)s,
            %(nmppkrujukan)s, %(kdppkpelayanan)s, %(nmppkpelayanan)s, %(jnspelayanan)s, %(catatan)s,
            %(diagawal)s, %(nmdiagnosaawal)s, %(kdpolitujuan)s, %(nmpolitujuan)s, %(klsrawat)s, %(klsnaik)s,
            %(pembiayaan)s, %(pjnaikkelas)s, %(lakalantas)s, %(user)s, %(nomr)s, %(nama_pasien)s, %(tanggal_lahir)s,
            %(peserta)s, %(jkel)s, %(no_kartu)s, %(tglpulang)s, %(asal_rujukan)s, %(eksekutif)s, %(cob)s, %(notelep)s,
            %(katarak)s, %(tglkkl)s, %(keterangankkl)s, %(suplesi)s, %(no_sep_suplesi)s, %(kdprop)s, %(nmprop)s,
            %(kdkab)s, %(nmkab)s, %(kdkec)s, %(nmkec)s, %(noskdp)s, %(kddpjp)s, %(nmdpdjp)s, %(tujuankunjungan)s,
            %(flagprosedur)s, %(penunjang)s, %(asesmenpelayanan)s, %(kddpjplayanan)s, %(nmdpjplayanan)s
        )
        ON DUPLICATE KEY UPDATE
            tglrujukan=VALUES(tglrujukan),
            no_rujukan=VALUES(no_rujukan),
            kdppkrujukan=VALUES(kdppkrujukan),
            nmppkrujukan=VALUES(nmppkrujukan),
            kdppkpelayanan=VALUES(kdppkpelayanan),
            nmppkpelayanan=VALUES(nmppkpelayanan),
            jnspelayanan=VALUES(jnspelayanan),
            catatan=VALUES(catatan),
            diagawal=VALUES(diagawal),
            nmdiagnosaawal=VALUES(nmdiagnosaawal),
            kdpolitujuan=VALUES(kdpolitujuan),
            nmpolitujuan=VALUES(nmpolitujuan),
            klsrawat=VALUES(klsrawat),
            klsnaik=VALUES(klsnaik),
            pembiayaan=VALUES(pembiayaan),
            pjnaikkelas=VALUES(pjnaikkelas),
            lakalantas=VALUES(lakalantas),
            user=VALUES(user),
            nomr=VALUES(nomr),
            nama_pasien=VALUES(nama_pasien),
            tanggal_lahir=VALUES(tanggal_lahir),
            peserta=VALUES(peserta),
            jkel=VALUES(jkel),
            no_kartu=VALUES(no_kartu),
            tglpulang=VALUES(tglpulang),
            asal_rujukan=VALUES(asal_rujukan),
            eksekutif=VALUES(eksekutif),
            cob=VALUES(cob),
            notelep=VALUES(notelep),
            katarak=VALUES(katarak),
            tglkkl=VALUES(tglkkl),
            keterangankkl=VALUES(keterangankkl),
            suplesi=VALUES(suplesi),
            no_sep_suplesi=VALUES(no_sep_suplesi),
            kdprop=VALUES(kdprop),
            nmprop=VALUES(nmprop),
            kdkab=VALUES(kdkab),
            nmkab=VALUES(nmkab),
            kdkec=VALUES(kdkec),
            nmkec=VALUES(nmkec),
            noskdp=VALUES(noskdp),
            kddpjp=VALUES(kddpjp),
            nmdpdjp=VALUES(nmdpdjp),
            tujuankunjungan=VALUES(tujuankunjungan),
            flagprosedur=VALUES(flagprosedur),
            penunjang=VALUES(penunjang),
            asesmenpelayanan=VALUES(asesmenpelayanan),
            kddpjplayanan=VALUES(kddpjplayanan),
            nmdpjplayanan=VALUES(nmdpjplayanan)
        """

        for row in results:
            cursor_dw_mut.execute(insert_query, row)

        db_dw_mut.commit()
        print(f"Data berhasil dimasukkan/diupdate ke tabel sep untuk rentang {seven_days_ago_str} s.d. {today_str}")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor_mut_2023.close()
        db_mut_2023.close()
        cursor_dw_mut.close()
        db_dw_mut.close()

# Eksekusi fungsi
fetch_data_for_last_7_days()
