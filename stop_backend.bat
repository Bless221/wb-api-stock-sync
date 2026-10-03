@echo off
chcp 65001 > nul
title Остановы сервиса синхронизации остатков

echo ============================================================
echo  WB / OZON STOCK SYNC — ОСТАНОВКА СЕРВИСА
echo ============================================================
echo.
echo Ищу активный фоновый процесс робота...

REM Находим процесс python.exe, который был запущен с файлом main.py в командной строке
wmic process where "name='python.exe' and commandline like '%%main.py%%'" call terminate >nul 2>&1
wmic process where "name='pythonw.exe' and commandline like '%%main.py%%'" call terminate >nul 2>&1

echo.
echo [УСПЕШНО] Команда на принудительное завершение отправлена.
echo Фоновый процесс Multi-Marketplace Stock Sync остановлен.
echo Вы можете безопасно обновлять файлы конфигурации или mapping.json.
echo.
echo ============================================================
pause
