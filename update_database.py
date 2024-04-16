import requests
import psycopg2
from datetime import datetime

# Function to fetch data from the website
def fetch_data():
    url = "http://103.146.217.82:1028/last"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data:", response.status_code)
        return None

# Function to update PostgreSQL database
def update_database(data):
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port="5433",
            database="Timestamp",
            user="postgres",
            password="preetham28"
        )
        cur = conn.cursor()

        sql = """
        UPDATE sensor_data
        SET 
            time = %(time)s,
            "Date_time" = %(Date_time)s,
            "Timestamp" = %(Timestamp)s,
            aqi = %(aqi)s,
            ch2o = %(ch2o)s,
            co = %(co)s,
            co2 = %(co2)s,
            "devID" = %(devID)s::varchar,
            light = %(light)s,
            no = %(no)s,
            no2 = %(no2)s,
            o3 = %(o3)s,
            pm1 = %(pm1)s,
            pm10 = %(pm10)s,
            pm2p5 = %(pm2p5)s,
            pressure = %(pressure)s,
            rain = %(rain)s,
            "rain_d" = %(rain_d)s,
            "rain_total" = %(rain_total)s,
            rh = %(rh)s,
            so2 = %(so2)s,
            sound = %(sound)s,
            temperature = %(temperature)s,
            "timestamp" = %(timestamp)s,
            ts = %(ts)s,
            uva = %(uva)s,
            uvb = %(uvb)s,
            voc = %(voc)s
        WHERE 
            "devID" = %(devID)s::varchar;
        """

        if isinstance(data, list):
            for item in data:
                if item.get("id") == "EMS0017":
                    device_data = item["data"]
                    # Add current timestamp
                    current_timestamp = int(datetime.now().timestamp() * 1000)
                    device_data["timestamp"] = current_timestamp
                    # Execute the SQL query
                    cur.execute(sql, device_data)
                    conn.commit()
                    print("Data updated successfully!")
                    break
        else:
            print("Invalid data format")

    except Exception as e:
        print("Error updating database:", e)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# Fetch data from the website
response = requests.get("http://103.146.217.82:1028/last")
if response.status_code == 200:
    data = response.json()
    update_database(data)
else:
    print("Failed to fetch data:", response.status_code)