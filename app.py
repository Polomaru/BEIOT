import os
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, jsonify, request
import mysql.connector

app = Flask(__name__)
app.secret_key = ".."

load_dotenv()

class DBConnection:

    def load_env(self) -> None:
        self.DATABASE_HOST = os.getenv("DATABASE_HOST")
        self.DATABASE_USERNAME = os.getenv("DATABASE_USERNAME")
        self.DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
        self.DATABASE = os.getenv("DATABASE")

    def connect_db(self) -> None:
        self.connection = mysql.connector.connect(
            host=self.DATABASE_HOST,
            user=self.DATABASE_USERNAME,
            password=self.DATABASE_PASSWORD,
            database=self.DATABASE,
            autocommit=True,
            use_pure=True
        )

    def __init__(self) -> None:
        self.load_env()
        self.connect_db()

    def execute_query(self, query: str) -> None:
        cursor = self.connection.cursor()
        cursor.execute(query)

    def execute_read_query(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query, self.connection)

# Example usage

@app.route('/get_id', methods=['GET'])
def get_id():
    id_param = request.args.get('id')
    
    if not id_param:
        return jsonify({'error': 'Missing parameter: id'}), 400

    try:
        id_value = int(id_param)
    except ValueError:
        return jsonify({'error': 'Invalid parameter: id must be an integer'}), 400

    query = f"SELECT * FROM devices WHERE `id` = {id_param}"
    
    df = DBConnection().execute_read_query(query=query)    
    print (df)
    if len(df) > 0:
        return jsonify({'status': 200})
    else:
        return jsonify({'status': 'not found'}), 404


if __name__ == "__main__":
    app.secret_key = ".."
    app.run(port=8080, threaded=True, host=('127.0.0.1'))