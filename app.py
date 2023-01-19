from flask import Flask, jsonify, request
from flask import render_template
import ast
from collections import OrderedDict
import logging

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)

dataValues = []
categoryValues = []

tags = {}

def top_acoounts(data, n=20):

    top = sorted(data.items(), key=lambda x: x[1], reverse=True)[:n]
    return OrderedDict(top)

@app.route("/")

def home():

    return render_template('index.html', dataValues=dataValues, categoryValues=categoryValues)

@app.route('/refreshData')

def refresh_data():

    global dataValues, categoryValues
    return jsonify(dataValues=dataValues, categoryValues=categoryValues)

@app.route('/updateData', methods=['POST'])

def update_data():

    global tags, dataValues, categoryValues
    data = ast.literal_eval(request.data.decode("utf-8"))
    tags[data['hashtag']] = data['count']
    sort_tag = top_acoounts(tags)
    categoryValues.clear()
    dataValues.clear()
    categoryValues = [x for x in sort_tag]
    dataValues = [tags[x] for x in sort_tag]
    return "success", 201

if __name__ == "__main__":
    app.run(host='localhost', port=3000, debug=True)
