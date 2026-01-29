import sqlite3
import csv
import requests
import os
import logging

#KUMA SQLite
DB_PATH = "/opt/kaspersky/kuma/core/00000000-0000-0000-0000-000000000000/raft/sm/db"
TABLE = "accounts"
DB_FIELDS = ["object_sid", "display_name", "domain"]
CSV_FIELDS = ["SID", "userName", "domainName"]

OUTPUT_FILE = os.path.join(os.getcwd(), "kuma_export.csv")

#API
URL = "https://:7223/api/v3/dictionaries/update" #Вставьте адрес KUMA
DICTIONARY_ID = "" # ID словаря
TOKEN = "" #API токен

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def export_csv():
    logging.info("Подключение к базе...")
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=30)
    cursor = conn.cursor()

    fields_sql = ", ".join(DB_FIELDS)

    where_sql = """
    object_sid IS NOT NULL AND TRIM(object_sid) != ''
    AND user_principal_name IS NOT NULL AND TRIM(user_principal_name) != ''
    AND domain IS NOT NULL AND TRIM(domain) != ''
    """

    query = f"""
    SELECT DISTINCT {fields_sql}
    FROM {TABLE}
    WHERE {where_sql}
    """

    logging.info(f"SQL запрос:\n{query}")

    cursor.execute(query)

    logging.info("Экспорт CSV...")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(CSV_FIELDS)

        count = 0
        for row in cursor:
            writer.writerow(row)
            count += 1

    conn.close()
    logging.info(f"Готово! Уникальных строк: {count}")
    logging.info(f"CSV файл создан: {OUTPUT_FILE}")

def post_csv():
    logging.info("Отправка CSV на сервер...")
    full_url = f"{URL}?dictionaryID={DICTIONARY_ID}"
    headers = {"Authorization": f"Bearer {TOKEN}"}

    try:
        with open(OUTPUT_FILE, "rb") as f:
            files = {"file": (os.path.basename(OUTPUT_FILE), f, "text/csv")}
            response = requests.post(full_url, headers=headers, files=files, verify=False)

        logging.info(f"Статус код: {response.status_code}")
        logging.info(f"Ответ сервера: {response.text}")

        if response.status_code >= 200 and response.status_code < 300:
            os.remove(OUTPUT_FILE)
            logging.info(f"CSV файл {OUTPUT_FILE} успешно удалён после отправки.")
        else:
            logging.warning(f"CSV файл {OUTPUT_FILE} НЕ удалён, так как сервер вернул статус {response.status_code}")

    except Exception as e:
        logging.error("Ошибка при отправке CSV", exc_info=True)

export_csv()
post_csv()
