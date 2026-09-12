import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="aqi_db",
        user="aqi_user",
        password="aqi_password"
    )
    print("SUCCESS: Connection ho gaya!")
    conn.close()
except Exception as e:
    print("FAILED:", e)