from flask import Flask
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask_jsonpify import jsonify

db = create_engine("sqlite:///mqtt.db")
app = Flask(__name__)
api = Api(app)


class Devices(Resource):
    def get(self):
        connection = db.connect()
        query = connection.execute("""
                                  select * from mqtt, node
                                  where mqtt.mac = node.mac;
                                  """)
        result = [dict(zip(tuple(query.keys()), i)) for i in query.cursor]
        return jsonify(result)


api.add_resource(Devices, "/devices")

if __name__ == "__main__":
    app.run(port=5000)
