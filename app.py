import os
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, jsonify, request
import mysql.connector
import json
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
    def table_exists(self, table_name: str) -> bool:
        query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s AND table_name = %s"
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, (self.DATABASE, table_name))
            return cursor.fetchone()[0] == 1
        except mysql.connector.error as e:
            print(f"Error checking table existence: {e}")
            return False    

    def __init__(self) -> None:
        self.load_env()
        self.connect_db()

    def execute_query(self, query: str) -> None:
        cursor = self.connection.cursor()
        cursor.execute(query)

    def execute_fetch_query(self,  query:str) :
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    
    def execute_read_query(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query, self.connection)

Dbconn = DBConnection()

def build_reponse(statusCode, body = None):
    response = {
        "status": statusCode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body" : body
    }
    return jsonify(response)

# Example usage
def id_exist(id : str, sensor_type: str) :
    table =  "actuator"  if sensor_type is  None else f"data_{sensor_type}"
    query =  f"SELECT id FROM {table} WHERE `id` = {id}"
    print(table)
    print(query)
    if not Dbconn.table_exists(table) : return False
    df =  Dbconn.execute_read_query(query=query) 
    return len(df)

@app.route('/get_id', methods=['GET'])
def get_id():
    id = request.args.get('id')
    sensor_type = request.args.get('sensor_type')
    if id_exist(id, sensor_type) : return build_reponse(200, "id exist")
    return build_reponse(500, "id does not exist")

@app.route('/create_id', methods=['POST'])
def create_id() :
    sensor_type  = request.args.get('sensor_type')
    table =  "actuator"  if sensor_type is  None else f"data_{sensor_type}"
    query =  f"SELECT max(id) FROM {table} "
    new_id = Dbconn.execute_fetch_query(query= query)[0][0] + 1
    insert_query = f"INSERT INTO {table} (`id`) VALUES ({new_id})"
    Dbconn.execute_query(query= insert_query)    
    return build_reponse(200, "id created")


def dht11_query(id, data):
    temperature = data["temperature"]
    humidity = data["humidity"] 
    query = f"UPDATE data_dht11 SET humidity = {humidity}, temperature = {temperature} WHERE id = {id}"

    return query
query_constructors = {
    'dht11': dht11_query
}
@app.route('/create_rule', methods=['POST'])
def create_rule():
    id_actor = request.args.get('id_actor')
    id_sensor = request.args.get('id_sensor')
    sensor_type = request.args.get('sensor_type')
    condition = request.args.get('condition')
    if not id_exist(id_actor, None): return build_reponse(500, "id does not exist")
    if not id_exist(id_sensor, sensor_type): return build_reponse(500, "id sensor does not exist")

    query = f"INSERT INTO rules (`id_actor`, `id_receptor`, `sensor_type`, `condition_rule`) VALUES ({id_actor}, {id_sensor}, '{sensor_type}', '{condition}')"
    print(query)
    Dbconn.execute_query(query=query)
    return build_reponse(200, "rule created")

@app.route('/delete_rule', methods=['DELETE'])
def delete_rule():
    id = request.args.get('id')
    query = f"DELETE FROM rules WHERE `id` = {id}"
    Dbconn.execute_query(query=query)
    return build_reponse(200, "rule deleted")

def check_actuator_event(id : str, sensor_type : str, data : dict) -> list :

    query = f"SELECT * FROM rules WHERE `id_receptor` = {id} AND `sensor_type` = '{sensor_type}'"
    
    df = Dbconn.execute_read_query(query=query)
    print(df)

    lst_actors = []
    for index, row in df.iterrows():
        evaluation_query = f"SELECT * FROM data_{sensor_type} WHERE `id` = {row['id_actor']} and {row['condition_rule']}"
        result = Dbconn.execute_read_query(query=evaluation_query)
        if not result.empty :  lst_actors.append(row['id_actor'])

    return lst_actors

@app.route('/register_data', methods=['POST'])
def register_data() :
    tp = request.get_data()
    tp= json.loads(tp)

    id = tp['id']
    sensor_type = tp['sensor_type']
    data = tp['data']

    if not Dbconn.table_exists(f"data_{sensor_type}") : return build_reponse(500,  "device type not supported")
    if not id_exist(id, sensor_type): return build_reponse(500, "id does not exist")
    query = query_constructors[sensor_type](id, data)

    try:
        Dbconn.execute_query(query=query)
        actors = check_actuator_event(id, sensor_type, data) 

        body = {
            "argument": "data registered",
            "activate": len(actors) > 0,
            "actors": actors
        }

        return build_reponse(200, body)
    except Exception as e:
        body = {
            "argument": "error un db connection",
            "error": str(e)
        }
        return build_reponse(500, body)

if __name__ == "__main__":
    app.secret_key = ".."
    app.run(port=8080, threaded=True, host=('127.0.0.1'))



