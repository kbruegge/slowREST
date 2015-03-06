from flask import Flask
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson import json_util
from flask.ext.restful import reqparse
from flask import render_template
from flask import abort
from flask import request
from fact import time
from plot import aux
import dateutil


app = Flask(__name__)
app.debug = True

app.config.update(dict(
    DATABASE='aux',
    HOST='localhost',
    PORT=37017
    ))

db = None
names = []


def request_wants_json():
    best = request.accept_mimetypes \
        .best_match(['application/json', 'text/html'])
    return best == 'application/json' and \
        request.accept_mimetypes[best] > \
        request.accept_mimetypes['text/html']


def connect_to_db():
    client = MongoClient("mongodb://" + app.config["HOST"] + ":" + str(app.config["PORT"]) + "/" + app.config["DATABASE"])
    return client.get_default_database()


@app.route('/')
def main_page():
    return render_template('index.html', names=names)


@app.route('/services')
def service_page():
    if request_wants_json():
        return json_util.dumps(names)
    return render_template('aux.html', names=names)


def check_date_string(str):
    return dateutil.parser.parse(str)


@app.route('/aux/<attribute_name>', methods=['GET'])
def aux_data(attribute_name):
    if attribute_name not in names:
        abort(404)
    parser = reqparse.RequestParser()
    parser.add_argument('from', type=check_date_string, help='start query at this time', required=True)
    parser.add_argument('to', type=check_date_string, help='latest time to query', required=True)
    args = parser.parse_args(strict=True)

    start = time.fjd(args.get('from'))
    end = time.fjd(args.get('to'))
    a = db[attribute_name].find({"Time": {"$gte": start, "$lt": end}})
    # print("entries: " + str(a.count()))

    if request_wants_json():
        return json_util.dumps(a)
    chart = aux.test_plot()
    #print(chart)
    return render_template('service.html', names=names, chart=chart)


if __name__ == '__main__':
    try:
        db = connect_to_db()
        names = list(db.collection_names())
        names.remove('system.indexes')
        app.run()
    except ConnectionFailure:
        print("Could not connect to database. Tunnel configured?")



