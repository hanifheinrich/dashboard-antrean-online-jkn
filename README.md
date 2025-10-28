## Dashboard Monitoring Antrol & SEP BPJS

Dashboard ini digunakan untuk memantau hubungan data **Antrean Online JKN (Antrol)** dengan **Surat Eligibilitas Peserta (SEP)** dari BPJS.
Tujuan utama dashboard ini adalah mendeteksi SEP yang sudah terbit tetapi **tidak tercatat di Antrol**, serta memantau **capaian target pelayanan** sesuai standar BPJS.

### Fitur Utama
- Menampilkan capaian target Antrol (90%) dan MJKN (40%)
- Mendeteksi SEP yang tidak muncul di data antrean
- Mengelola dimensi tanggal dan menghitung persentase otomatis
- Menjalankan proses ETL secara terjadwal menggunakan **Windows Task Scheduler**

### Teknologi
- **Python** untuk proses ETL (Extract, Transform, Load)
- **MySQL** sebagai data warehouse
- **Power BI** untuk visualisasi dashboard

### Preview
<img width="1280" height="2340" alt="image" src="https://github.com/user-attachments/assets/17074bd4-386a-4b8d-a5f2-7b6b1f1cf1ec" />

