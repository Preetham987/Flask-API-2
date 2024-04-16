from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

def fetch_latest_data():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="Timestamp",
            user="postgres",
            password="preetham28"  
        )
        cur = conn.cursor()

        sql = """
        SELECT *
        FROM sensor_data
        WHERE "devID" = 'EMS0017'
        ORDER BY "timestamp" DESC
        LIMIT 1;
        """
        cur.execute(sql)
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchone()

        if data:
            data_dict = dict(zip(columns, data))
            return data_dict
        else:
            return None

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

@app.route('/latest_data', methods=['GET'])
def get_latest_data():
    data = fetch_latest_data()
    if data:
        return jsonify(data), 200
    else:
        return jsonify({"error": "Data not found"}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
