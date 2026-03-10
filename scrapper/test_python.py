import pyodbc
try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server};'
        'SERVER=localhost,1433;'
        'UID=sa;'
        'PWD=YourStrongPassword123!;'
        'TrustServerCertificate=yes;'
    )
    print("Koneksi Inti Sukses!")
    cursor = conn.cursor()
    cursor.execute("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'Job_Analyzer') CREATE DATABASE Job_Analyzer")
    print("Database Job_Analyzer dipastikan sudah ada sekarang!")
    conn.close()
except Exception as e:
    print(f"Error Diagnosa: {e}")