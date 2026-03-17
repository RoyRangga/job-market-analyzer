# Menggunakan bitnami spark sebagai base karena sudah ada Spark & Java
FROM bitnami/spark:latest

USER root 

# 1. Install System Dependencies & Java (jika belum ada)
RUN apt-get update && apt-get install -y \
    wget gnupg unzip curl gnupg2 ca-certificates procps \
    libpq-dev gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Google Chrome (Untuk Selenium)
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable

# 3. Install Microsoft ODBC Driver 18 (Untuk SQL Server) - Sesuai Debian 12 (Bookworm)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# 4. Set working directory
WORKDIR /app

# 5. Install Python Libraries
# Pastikan pip sudah ada di image bitnami
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pandas boto3 pyarrow pyodbc sqlalchemy selenium webdriver-manager pyspark

# 6. Salin file proyek
COPY . .

# Kembali ke user non-root demi keamanan (opsional, tapi disarankan)
# USER 1001

CMD ["tail", "-f", "/dev/null"]