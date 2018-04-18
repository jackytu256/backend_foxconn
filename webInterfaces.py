#coding:utf-8
from flask import Flask, request
from infoCtrl import *
import json
from flask_cors import CORS
app = Flask(__name__)
cors = CORS(app)
@app.route('/save', methods=['GET', 'POST'])
def save():
    a = Producer('doSave') 
    js = json.loads(request.form['res'])
    js['img_base64'] = request.form['image_base64']
    js['did'] = request.form['did']
    a.append(json.dumps(js))
    return 'OK'
@app.route('/get', methods = ['GET', 'POST'])
def get():
    pass
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7788)
