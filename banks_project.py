# =========================
# Import Libraries
# =========================
import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

# =========================
# Global Variables
# =========================
URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
EXCHANGE_CSV = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"

OUTPUT_CSV = "./Largest_banks_data.csv"
DB_NAME = "Banks.db"
TABLE_NAME = "Largest_banks"
LOG_FILE = "code_log.txt"


# =========================
# Task 1: Logging Function
# =========================
def log_progress(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_FILE, "a") as file:
        file.write(f"{timestamp} : {message}\n")


# =========================
# Task 2: Extract
# =========================
def extract(url):
    log_progress("Extraction started")

    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    soup = BeautifulSoup(response.text, 'html.parser')

    tables = soup.find_all('table', {'class': 'wikitable'})

    # Find correct table by checking heading
    target_table = None
    for table in tables:
        if "Market cap" in table.text or "market capitalization" in table.text:
            target_table = table
            break

    rows = target_table.find_all('tr')

    data = []
    for row in rows[1:11]:  # Top 10
        cols = row.find_all('td')

        name = cols[1].text.strip()
        mc_usd = cols[2].text.strip()

        mc_usd = float(mc_usd.replace(',', '').replace('\n', ''))

        data.append([name, mc_usd])

    df = pd.DataFrame(data, columns=["Name", "MC_USD_Billion"])

    log_progress("Extraction completed")
    return df


# =========================
# Task 3: Transform
# =========================
def transform(df):
    log_progress("Transformation started")

    exchange_df = pd.read_csv(EXCHANGE_CSV)

    # Convert to dictionary
    rate_dict = dict(zip(exchange_df['Currency'], exchange_df['Rate']))

    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * rate_dict['GBP'], 2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * rate_dict['EUR'], 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * rate_dict['INR'], 2)

    log_progress("Transformation completed")
    return df


# =========================
# Task 4: Load to CSV
# =========================
def load_to_csv(df, output_path):
    log_progress("Loading to CSV started")

    df.to_csv(output_path, index=False)

    log_progress("Loading to CSV completed")


# =========================
# Task 5: Load to Database
# =========================
def load_to_db(df, db_name, table_name):
    log_progress("Loading to DB started")

    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)

    conn.close()

    log_progress("Loading to DB completed")


# =========================
# Task 6: Run Queries
# =========================
def run_queries(db_name, table_name):
    log_progress("Running queries started")

    conn = sqlite3.connect(db_name)

    print("\nTop 5 Records:")
    print(pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", conn))

    print("\nAverage Market Cap:")
    print(pd.read_sql(f"SELECT AVG(MC_USD_Billion) FROM {table_name}", conn))

    print("\nTop Bank:")
    print(pd.read_sql(f"SELECT Name FROM {table_name} ORDER BY MC_USD_Billion DESC LIMIT 1", conn))

    conn.close()

    log_progress("Queries executed successfully")


# =========================
# Main Execution
# =========================
if __name__ == "__main__":
    log_progress("ETL Job Started")

    df = extract(URL)
    print(df)

    df = transform(df)
    print(df)

    load_to_csv(df, OUTPUT_CSV)
    load_to_db(df, DB_NAME, TABLE_NAME)

    run_queries(DB_NAME, TABLE_NAME)

    log_progress("ETL Job Completed")