import mysql.connector
import json
import os
from faker import Faker
import random

# --- НАЛАШТУВАННЯ ---
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Rak riba21!",
    "database": "airflow_hw_db"
}

fake = Faker()
JSON_FOLDER = "telephony_data"

# Створюємо папку для JSON, якщо її немає
if not os.path.exists(JSON_FOLDER):
    os.makedirs(JSON_FOLDER)


def generate_mock_data():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        print("🚀 Генеруємо 50 працівників...")
        employee_ids = []
        for i in range(1, 51):
            sql = "INSERT IGNORE INTO employees (employee_id, surname, name, role, hire_date) VALUES (%s, %s, %s, %s, %s)"
            val = (i, fake.last_name(), fake.first_name(), fake.job()[:30], fake.date_this_decade())
            cursor.execute(sql, val)
            employee_ids.append(i)

        print("📞 Генеруємо дзвінки та JSON файли...")
        for i in range(100, 121):  # Створимо 20 дзвінків
            call_id = i
            emp_id = random.choice(employee_ids)

            # 1. Запис у MySQL
            sql_call = "INSERT IGNORE INTO calls (call_id, employee_id, phone, direction, status) VALUES (%s, %s, %s, %s, %s)"
            direction = random.choice(['inbound', 'outbound'])
            status = random.choice(['success', 'failed', 'missed'])
            val_call = (call_id, emp_id, fake.phone_number()[:20], direction, status)
            cursor.execute(sql_call, val_call)

            # 2. Створення JSON (для симуляції Телефонії)
            json_data = {
                "call_id": call_id,
                "duration_sec": random.randint(10, 600),
                "short_description": fake.sentence(nb_words=6)
            }

            with open(f"{JSON_FOLDER}/{call_id}.json", "w") as f:
                json.dump(json_data, f)

        conn.commit()
        print("✅ Все готово! Перевір DataGrip та папку telephony_data.")

    except mysql.connector.Error as err:
        print(f"❌ Помилка: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


if __name__ == "__main__":
    generate_mock_data()