import subprocess
import logging
from datetime import datetime
import os

# Pastikan folder log ada
log_folder = "log"
os.makedirs(log_folder, exist_ok=True)

# Setup logging
log_file = os.path.join(log_folder, f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_etl(file_name):
    try:
        logging.info(f"Mulai menjalankan {file_name}")
        subprocess.run(['python', file_name], check=True)
        logging.info(f"Berhasil menjalankan {file_name}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error saat menjalankan {file_name}: {e}")
    except Exception as e:
        logging.exception(f"Exception umum saat menjalankan {file_name}")

# Urutan eksekusi
run_etl('etl-tanggal.py')
run_etl('etl-sep.py')
run_etl('etl-antrean-per-tanggal.py')

logging.info("Semua ETL selesai dijalankan.")
