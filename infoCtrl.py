#coding:utf-8
import redis
import sys
from datetime import datetime as dt
import os
import md5 as dig
import base64 as b64
import time
import json as js
import signal as sig
import psycopg2 as psql
TIMEOUT_LABEL='timeout'
REDIS_HOST = '127.0.0.1'
REDIS_PORT = 6379
PSQL_HOST = '127.0.0.1'
PSQL_PORT =  5432
TIMEOUT_VAL = 1 #in second
SAFE_EXIT = 0
def exitHandler(sigNum, frm): 
    print 'Ctrl-C caught, exiting...'
    global SAFE_EXIT
    SAFE_EXIT = 1
def theCallBack(imgBase64):
    return len(imgBase64)
def doMkdir(path):
    if not os.path.exists(path):
        try:
            os.mkdir(path)
            return True
        except:
            return False
    return True

class Producer:
    def __init__(self, queName):
        self.name = queName
        self.con = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    def append(self, imgBase64):
        last = int(self.con.get(TIMEOUT_LABEL))
        cur = int(time.time())
        if cur - last < TIMEOUT_VAL:
            try:
                self.con.lpush(self.name, imgBase64)
            except:
                print 'cannot push'
    def getLen(self):
	return self.con.llen(self.name)

class Consumer:
    def __init__(self, queName, savingPath):
        self.name = queName
        try:
            self.con = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        except:
            print 'redis connection error'
        self.path = savingPath
        try:
            os.mkdir(self.path)
        except:
            print 'mkdir failed: %s' % str(self.path)
        self.strDict = {}
        try:
            self.dbCon = psql.connect(database='foxconn', user='postgres')
        except:
            print 'failed to open database'

        s = "SELECT name, id from lbl"
        try:
            cursor  = self.dbCon.cursor()
            cursor.execute(s)
            rows = cursor.fetchall()
            for i in rows:
                self.strDict[i[0]] = i[1]
        except:
            print 'failed to execute: %s' % s
        sig.signal(sig.SIGINT, exitHandler)
        sig.signal(sig.SIGTERM, exitHandler)
    def safeExit(self):
        try:
            self.dbCon.close()
        except:
            print 'cannot close psqlite'
    def doCreateTbl(self, s):
        try:
            cursor = self.dbCon.cursor()
            cursor.execute(s)
        except psql.OperationalError, e:
            print e
    def doStr(self, theString):
        if not self.strDict.has_key(theString):
            s = 'INSERT INTO lbl(name) VALUES(\'%s\');' % theString
            try:
                c = self.dbCon.cursor()
                c.execute(s)
                r = psql.curval('lbl_id_seq')
                self.strDict[theString] = r
                return r
            except:
                print 'failed to insert and query'
                return -1
        else:
            return self.strDict[theString]
    def __del__(self):
        self.safeExit()
    def getLen(self):
	return self.con.llen(self.name)
    def runForever(self):
	a = dig.new()
        while True:
            global SAFE_EXIT
            if SAFE_EXIT == 1:
                self.safeExit()
                print 'safely returned'
                sys.exit(sig.SIGINT)
                return
            now = dt.now()
            localTime = time.localtime()
            year = time.strftime('%Y', localTime)
            month = time.strftime('%m', localTime)
            date = time.strftime('%d',localTime)
            hour = time.strftime('%H', localTime)
            mins = time.strftime('%M', localTime)
            sec = time.strftime('%S', localTime)
            mil = '%.6d' % now.microsecond

            yPath = os.path.join(self.path, year)
            if not doMkdir(yPath):
                print 'cannot mkdir %s' % yPath
                return
            mPath = os.path.join(yPath, month)
            if not doMkdir(mPath):
                print 'cannot mkdir %s' % mPath
                return
            dPath = os.path.join(mPath, date)
            if not doMkdir(dPath):
                print 'cannot mkdir %s' % dPath
                return
            hPath = os.path.join(dPath, hour)
            if not doMkdir(hPath):
                print 'cannot mkdir %s' % hPath
                return
            l = self.con.llen(self.name)
            if l == 0:
                cur = int(time.time())
                self.con.set(TIMEOUT_LABEL, cur)

	    job = self.con.rpop(self.name)
	    if job:
		jObj = js.loads(str(job))
                base64Img = jObj['img_base64']
		a.update(base64Img)
                binImg = b64.b64decode(base64Img)
		p = '%s-%s-%s-%s-%s-%s-%s-%s.jpg' % (year, month, date, hour, mins, sec, mil, a.hexdigest().upper())
		k = os.path.join(hPath, p)
                did = int(jObj['did'])
                numFace = int(jObj['num_faces'])
                faces = jObj['faces']
		with open(k, 'wb') as f:
		    f.write(binImg)
                try:
                    cur = int(time.mktime(localTime))
                    cursor = self.dbCon.cursor()
                    #insert main table
                    query = "INSERT INTO img(path, timestamp) VALUES(\'%s\', %d);" % (p, cur)
                    cursor.execute(query)
                    imgId = 0;
                    cursor.execute("SELECT last_value FROM img_id_seq")
                    imgId = int(cursor.fetchone()[0])
                    #insert attributes table
                    for i in faces:
                        #×ø±ê
                        x = float(i['position']['center']['x'])
                        y = float(i['position']['center']['y'])
                        w = float(i['position']['size']['width'])
                        h = float(i['position']['size']['height'])
                        #ÇéÐ÷
                        em = i['emotions']
                        angry = float(em['angry'])
                        confused = float(em['confused'])
                        sad = float(em['sad'])
                        neutral=float(em['neutral'])
                        disgust = float(em['disgust'])
                        surprise = float(em['surprise'])
                        fear = float(em['fear'])
                        contempt = float(em['contempt'])
                        happy = float(em['happy'])
                        #ÊôÐÔ
                        attr = i['attribute']
                        age=int(attr['age'])
                        arched_eyebrows = int(attr['arched_eyebrows'])
                        attractive = float(attr['attractive'])
                        bagsUnderEyes = int(attr['bags_under_eyes'])
                        beardStyleStr = str(attr['beard_style'])
                        doubleChin = int(attr['double_chin'])
                        glass = int(attr['eyeglasses'])
                        gender = int(attr['gender'])
                        hairColorStr = str(attr['hair_color'])
                        hairLengthStr= str(attr['hair_length'])
                        hairStyleStr = str(attr['hair_style'])
                        heavyMakeup = int(attr['heavy_makeup'])
                        interest = int(attr['interest']);
                        intent = int(attr['intent'])
                        race = str(attr['race'])
                        hat = int(attr['wearing_hat'])
                        beardStyleInt = self.doStr(beardStyleStr) 
                        hairColorInt = self.doStr(hairColorStr)
                        hairStyleInt = self.doStr(hairStyleStr)
                        hairLengthInt = self.doStr(hairLengthStr)
                        raceInt = self.doStr(race)
                        s = 'INSERT INTO att(did, iid, age, arched_eyebrows, '
                        s += 'attractive, bags_under_eyes, beard_style, '
                        s += 'bushy_eyebrows, double_chin, eyeglasses, gender, '
                        s += 'hair_color, hair_length, hair_style, heavy_makeup, '
                        s += 'race, wearing_hat, interest, intent, angry, confused, '
                        s += 'contempt, disgust, fear, happy, neutral, sad, surprise, '
                        s += 'x, y, w, h) VALUES '
                        s += '(%d, %d, %d, %d,'
                        s += '%f, %d, %d, '
                        s += '%d, %d, %d, %d, '
                        s += '%d, %d, %d, %d, '
                        s += '%d, %d, %d, %d, %f, %f, '
                        s += '%f, %f, %f, %f, %f, %f, %f, '
                        s += '%d, %d, %d, %d)'
                        print s
                        cursor.execute(s)
                    self.con.set(TIMEOUT_LABEL, cur)
                    self.dbCon.commit();
                except:
                    print 'db operational error'
