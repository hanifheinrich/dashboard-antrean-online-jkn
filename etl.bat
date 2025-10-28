@echo off
cd /d E:\Downloads\hanif\dashboard\antrol
python etl.py
echo Menutup dalam 10 detik...
timeout /t 10 >nul
exit
