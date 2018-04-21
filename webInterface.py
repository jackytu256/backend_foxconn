#coding:utf-8
from flask import Flask, request
from infoCtrl import *
from label import *
import json
from flask_cors import CORS
import redis
app = Flask(__name__)
cors = CORS(app)


@app.route('/hello', methods=['GET', 'HOST'])
def hello():
    return 'hello world'

@app.route('/save', methods=['GET', 'POST'])
def save():
    a = Producer('doSave') 
    js = {}
    print request.form
    js['res'] = json.loads(request.form['res'])
    js['img_base64'] = request.form['image_base64']
    js['did'] = str(request.form['did'])
    a.append(json.dumps(js))
    return 'OK'
@app.route('/getDevs', methods = ['GET', 'POST'])
def getDevs():
    con = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    return con.get(DEV_LABEL)
@app.route('/getRep', methods= ['GET', 'POST'])
def getRep():
    j = json.loads(request.form['req'])
    try:
        start = str(j['start'])
        stop = str(j['stop'])
        start = time.strptime(start, "%Y%m%d%H")
        stop = time.strptime(stop, "%Y%m%d%H")
        start = int(time.mktime(start))
        stop = int(time.mktime(stop))
        devs = j['devs']
        con = redis.Redis(host=REDIS_HOST, port=REDIS_PORT) 
        j = js.loads(con.get(DATA_LABEL))
        storedKeys = j.keys();
        male = 0
        female = 0
        age = [0 for i in range(0, 101)]
        group = [0 for i in range(0, 8)]
        trendM = [0 for i in range(0, 24)]
        trendF = [0 for i in range(0, 24)]
        emotion = [0 for i in range(0, 9)]
        for i in devs:
            if i not in storedKeys:
                return "{'code':'unknow keys'}"
            devData = j[i]
            hourSeg = devData.keys();
            for hour in hourSeg:
                hourInt = int(hour)
                if hourInt > start and hourInt <= stop:
                    data = devData[hour]
                    #ÐÔ±ð
                    male = male + int(data['male'])
                    female = female + int(data['female'])
                    #ÄêÁä
                    ages = data['ages']
                    for a in ages:
                        age[int(a)] = age[int(a)] + int(ages[a])
                    #group
                    tag = data['groups']
                    for t in tag:
                        group[int(t)] = group[int(t)] + int(tag[t])
                    #trend
                    tm = time.localtime(float(hour))
                    h = tm.tm_hour
                    trendF[h] = trendF[h] + int(data['female'])
                    trendM[h] = trendM[h] + int(data['male'])
                    #emotion
                    em = data['emotion']
                    for e in range(0, 9):
                        emotion[e] = emotion[e] + int(em[e])
        res = '{"code":"success", "male": %d, "female": %d, "ages": {' % (male, female)
        ss = []
        for i in range(0, 101):
            if 0 != age[i]:
                ss.append([i, age[i]])
        ssLen = len(ss)
        for i in range(0, ssLen - 1):
            res += '"%d": %d, ' % (ss[i][0], ss[i][1])
        if ssLen > 0:
            res += '"%d": %d}, "group": [' % (ss[ssLen-1][0], ss[ssLen-1][1])
        else:
            res += '}, "group":['
        for i in range(0, 7):
            res += '%d, ' %  group[i]
        res += '%d], "trendM": {' % group[7]
        for i in range(0, 23):
            res += '"%d": %d, ' % (i, trendM[i])
        res += '"%d": %d}, "trendF":{' % (23, trendM[23])
        for i in range(0, 23):
            res += '"%d": %d, ' % (i, trendF[i])
        res += '"%d": %d}, "emotion": [' % (23, trendF[23])
        for i in range(0, 8):
            res += '%d, ' % emotion[i]
        res += '%d]}' % emotion[8]
        return res
    except Exception, e:
        print e
    return '{"code":"failure"}'
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7788)
