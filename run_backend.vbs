Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "cmd /c ""venv\Scripts\activate.bat && python main.py""", 0, False
Set WshShell = Nothing
