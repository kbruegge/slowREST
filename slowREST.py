from flask import Flask
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson import json_util
from flask.ext.restful import reqparse
from flask import render_template
from flask import abort


app = Flask(__name__)
app.debug = True

app.config.update(dict(
    DATABASE='aux',
    HOST='localhost',
    PORT=37017
    ))

db = None
names=[]


def connect_to_db():
    client = MongoClient("mongodb://" + app.config["HOST"] + ":" + str(app.config["PORT"]) + "/" + app.config["DATABASE"])
    return client.get_default_database()


@app.route('/')
def main_page():
    return render_template('index.html', names=names)


@app.route('/services')
def service_page():
    return json_util.dumps(names)



@app.route('/aux/<attribute_name>', methods=['GET'])
def aux_data(attribute_name):
    if attribute_name not in names:
        abort(404)
    parser = reqparse.RequestParser()
    parser.add_argument('from', type=float, help='start query at this time')
    parser.add_argument('to', type=float, help='latest time to query')
    args = parser.parse_args(strict=True)

    start = args.get('from')
    end = args.get('to')

    a = None
    if start is None or end is None:
        # print("No parameters")
        a = []
    else:
        # print(start, end)
        a = db[attribute_name].find({"Time": {"$gte": start, "$lt": end}})
        # print("entries: " + str(a.count()))

    return json_util.dumps(a)


if __name__ == '__main__':
    try:
        db = connect_to_db()
        names = list(db.collection_names())
        names.remove('system.indexes')
        app.run()
    except ConnectionFailure:
        print("Could not connect to database. Tunnel configured?")



