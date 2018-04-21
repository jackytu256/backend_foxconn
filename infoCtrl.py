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
from label import *
TIMEOUT_LABEL='timeout'
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
def doSql(cursor, query):
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except:
        print 'failed to sql: %s' % query

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
    def initStatistics(self):
        cursor = self.dbCon.cursor()
	start = 0;
	stop = 0
        try:#数据库中最早的时间, 最晚的时间
            cursor.execute("SELECT min(ts), max(ts) FROM img")
            k = cursor.fetchone();
            start = int(k[0])
            stop = int(k[1])
            self.con.set(MIN_START_TIME, start);
            self.con.set(MAX_STOP_TIME, stop)
        except Exception, e:
            print 'f', e
            start = 0 
            stop = 0

        startSeg = start - start % 3600
        stopSeg = stop - stop % 3600 + 3600
        totalData={}
        for dev in self.devs:
            devData = {}
            for seg in range(startSeg, stopSeg, 3600):
                hourDic = {}
                ## 女性
                s = "SELECT count(attribs.id) FROM attribs, img WHERE attribs.iid = img.id and attribs.gender = 1 and attribs.did = %d and img.ts > %d and img.ts <= %d;" % (self.devDict[dev], seg, seg + 3600)
                cursor.execute(s)
                k = cursor.fetchone()
                hourDic['female'] = int(k[0])
                ## 男性
                s = "SELECT count(attribs.id) FROM attribs, img WHERE attribs.iid = img.id and attribs.gender = 0 and attribs.did = %d and img.ts > %d and img.ts <= %d;" % (self.devDict[dev], seg, seg + 3600)
                cursor.execute(s)
                k = cursor.fetchone()
                hourDic['male'] = int(k[0])

                #年龄
                s = "SELECT attribs.age, count(attribs.age) FROM attribs, img WHERE attribs.iid = img.id and attribs.did = %d and img.ts > %d and img.ts <= %d group by attribs.age order by attribs.age asc;" % (self.devDict[dev], seg, seg + 3600)
                cursor.execute(s)
                k = cursor.fetchall();
                age = {}
                for j in k:
                    age[int(j[0])] = int(j[1])
                hourDic['ages'] = age

                #人群分类
                s = "SELECT attribs.tag, count(attribs.tag) FROM attribs, img WHERE attribs.iid = img.id and attribs.did = %d and img.ts > %d and img.ts <= %d and attribs.tag < 8 group by attribs.tag order by attribs.tag asc;" % (self.devDict[dev], seg, seg + 3600)
                cursor.execute(s)
                k = cursor.fetchall()
                grps = {}
                for j in k:
                    grps[int(j[0])] = int(j[1])
                hourDic['groups'] = grps

                #表情
                s = "SELECT attribs.angry, attribs.confused, attribs.contempt, attribs.disgust, attribs.fear, attribs.happy, attribs.neutral, attribs.sad, attribs.surprise FROM attribs, img "
                s += "WHERE attribs.iid = img.id and attribs.did = %d " % self.devDict[dev]
                s += "and img.ts > %d and img.ts <= %d"  % (seg, seg + 3600)
                cursor.execute(s)
                k = cursor.fetchall()
                stat = [0 for fuck in range(0, 9)]
                for j in k:
                    idx=0;
                    m = 0.0
                    for w in range(0, 9):
                        if j[w] > m:
                            m = j[w]
                            idx = w
                    stat[idx] = stat[idx] + 1
                hourDic['emotion'] = stat
                ####
                devData[seg] = hourDic
            totalData[dev] = devData
        j = js.dumps(totalData)
        self.con.set(DATA_LABEL, j)
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
        self.devDict = {}
        try:
            self.dbCon = psql.connect(host="localhost", database='foxconn', user='postgres', password="emotibot007")
        except Exception, e:
            print 'a', e
        s = "SELECT name, id from lbl"
        try:
            cursor  = self.dbCon.cursor()
            cursor.execute(s)
            rows = cursor.fetchall()
            for i in rows:
                self.strDict[i[0]] = i[1]
        except:
            print 'failed to execute: %s' % s
        try:
            s = "SELECT name, id from devs"
            cursor  = self.dbCon.cursor()
            cursor.execute(s)
            rows = cursor.fetchall()
            self.devs = []
            for i in rows:
                d = i[0].replace(" ", '')
                self.devDict[d] = i[1]
                self.devs.append(d)
            dc = {}
            dc['devs'] = self.devs
            j = js.dumps(dc)
            self.con.set(DEV_LABEL, j)
        except Exception, e:
            print 'b', e
        self.initStatistics();
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
            s = 'INSERT INTO "lbl" VALUES(nextval(\'lbl_id_seq\'), \'%s\');' % theString
            try:
                c = self.dbCon.cursor()
                c.execute(s)
                c.execute("SELECT last_value FROM lbl_id_seq")
                r = int(c.fetchone()[0])
                self.strDict[theString] = r
                return r
            except Exception, e:
                print 'c', e
                return -1
        else:
            return self.strDict[theString]
    def getDevId(self, theString):
        if not self.devDict.has_key(theString):
            s = 'INSERT INTO "devs" VALUES(nextval(\'devs_id_seq\'), \'%s\');' % theString
            try:
                c = self.dbCon.cursor()
                c.execute(s)
                c.execute("SELECT last_value FROM devs_id_seq")
                r = int(c.fetchone()[0])
                self.devDict[theString] = r
                self.devs.append(theString)
                s = self.con.get(DEV_LABEL)
                j = js.loads(s)
                j['devs'].append(theString) ######## 将新dev-id加入
                s = js.dumps(j)
                self.con.set(DEV_LABEL, s)
                return r
            except:
                print 'failed to insert and query'
                return -1
        else:
            return self.devDict[theString]

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
            fltTime = time.time()
            self.con.set(MAX_STOP_TIME, fltTime)
            localTime = time.localtime(int(fltTime))
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
                self.con.set(TIMEOUT_LABEL, int(fltTime))

	    job = self.con.rpop(self.name)
	    if job:
		jObj = js.loads(str(job))
                base64Img = jObj['img_base64']
		a.update(base64Img)
                binImg = b64.b64decode(base64Img)
		p = '%s-%s-%s-%s-%s-%s-%s-%s.jpg' % (year, month, date, hour, mins, sec, mil, a.hexdigest().upper())
		k = os.path.join(hPath, p)
                did = self.getDevId(str(jObj['did']))
                #numFace = len(jObj['res'])
                faces = jObj['res']
                numFace = len(faces)
		with open(k, 'wb') as f:
		    f.write(binImg)
                try:
                    cursor = self.dbCon.cursor()
                    #insert main table
                    query = 'INSERT INTO "img" VALUES(nextval(\'img_id_seq\'), \'%s\', %f);' % (p, fltTime)
                    cursor.execute(query)
                    imgId = 0;
                    cursor.execute("SELECT last_value FROM img_id_seq")
                    imgId = int(cursor.fetchone()[0])
                    #insert attributes table
                    for i in faces:
                        #坐标
                        x = float(i['position']['center']['x'])
                        y = float(i['position']['center']['y'])
                        w = float(i['position']['size']['width'])
                        h = float(i['position']['size']['height'])
                        #情绪
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
                        #属性
                        attr = i['attribute']
                        age=int(attr['age'])
                        arched_eyebrows = int(attr['arched_eyebrows'])
                        attractive = float(attr['attractive'])
                        bagsUnderEyes = int(attr['bags_under_eyes'])
                        beardStyleStr = str(attr['beard_style'])
                        bushyEyeBrows = int(attr['bushy_eyebrows'])
                        doubleChin = int(attr['double_chin'])
                        glass = int(attr['eyeglasses'])
                        gender = int(attr['gender'])
                        hairColorStr = str(attr['hair_color'])
                        hairLengthStr= str(attr['hair_length'])
                        hairStyleStr = str(attr['hair_style'])
                        heavyMakeup = int(attr['heavy_makeup'])
                        interest = 0;
                        try:
                            interest = int(attr['interest']);
                        except:
                            interest = 0;
                        intent = 0;
                        try:
                            intent = int(attr['intent'])
                        except:
                            intent = 0
                        race = str(attr['race'])
                        hat = int(attr['wearing_hat'])
                        beardStyleInt = self.doStr(beardStyleStr) 
                        hairColorInt = self.doStr(hairColorStr)
                        hairStyleInt = self.doStr(hairStyleStr)
                        hairLengthInt = self.doStr(hairLengthStr)
                        raceInt = self.doStr(race)

                        ### compute the tags
                        #0-7分别按照雅雯的规则图
                        val = attractive * 20 + 80
                        tag = 100 ###未落入规则部分
                        if 0 == gender: #男
                            if "black_hair" == hairColorStr:
                                if age >= 15 and age <= 35 and val >=65 and val <= 77:
                                    tag = 6
                                elif age >= 36 and age <= 55 and val >= 60 and val <= 78:
                                    tag = 7
                            else:
                                if age >= 15 and age <= 40 and val >=70: #霸屏欧巴
                                    tag = 4
                                elif age >= 40 and age <= 60 and val >= 75 and val <= 84: #气质大叔
                                    tag = 5
                        else:
                            if "black_hair" == hairColorStr:
                                if age >= 15 and age <= 35 and val >= 70 and val <= 87:
                                    tag = 1
                                elif age >= 30 and age <= 60 and val <= 79 and val >= 66:
                                    tag = 2
                                elif age >= 5 and age <= 25 and val >= 70 and val <= 87:
                                    tag = 3
                            else:
                                if age>= 15 and age <= 35 and val >=75 and val <=100:
                                    tag = 0


                        s = 'INSERT INTO "attribs" VALUES('
                        s += 'nextval(\'attribs_id_seq\'), %d, %d, %d, %d,' % (did, imgId, age, arched_eyebrows)
                        s += '%f, %d, %d, ' % (attractive, bagsUnderEyes, beardStyleInt)
                        s += '%d, %d, %d, %d, ' % (bushyEyeBrows, doubleChin, glass, gender)
                        s += '%d, %d, %d, %d, ' % (hairColorInt, hairLengthInt, hairStyleInt, heavyMakeup)
                        s += '%d, %d, %d, %d, %f, %f, ' % (raceInt, hat, interest, intent, angry, confused)
                        s += '%f, %f, %f, %f, %f, %f, %f, ' % (contempt, disgust, fear, happy, neutral, sad, surprise)
                        s += '%f, %f, %f, %f, %d)' % (x, y, w, h, tag)
                        cursor.execute(s)
                    self.con.set(TIMEOUT_LABEL, int(fltTime))

                    j = js.loads(self.con.get(DATA_LABEL))
                    intTime = int(fltTime) - (int(fltTime)) % 3600
                    devStr = jObj['did']
                    if devStr in j.keys():
                        timeStr = str(intTime)
                        if not timeStr in j[devStr].keys():
                            j[devStr][timeStr] = {}
                            j[devStr][timeStr]["male"] = 0
                            j[devStr][timeStr]["female"]  = 0
                            j[devStr][timeStr]["ages"] = {}
                            j[devStr][timeStr]["groups"] = {}
                            j[devStr][timeStr]["emotion"] = [0 for i in range(0, 9)]
                        if 0 == gender:
                            j[devStr][timeStr]["male"] = j[devStr][timeStr]["male"] + 1
                        else:
                            j[devStr][timeStr]["female"] = j[devStr][timeStr]["female"] + 1
                        if j[devStr][timeStr]['ages'].has_key(str(age)):
                            j[devStr][timeStr]['ages'][str(age)] = j[devStr][timeStr]['ages'][str(age)] + 1
                        else:
                            j[devStr][timeStr]['ages'][str(age)] = 1
                        if tag < 8:
                            strTag = str(tag)
                            if j[devStr][timeStr]['groups'].has_key(strTag):
                                j[devStr][timeStr]['groups'][strTag] = j[devStr][timeStr]['groups'][strTag] + 1
                            else:
                                j[devStr][timeStr]['groups'][strTag] = 1
                        emt = [angry, confused, contempt, disgust, fear, happy, neutral, sad, surprise]
                        emtIdx = 0;
                        maxVal = 0.0
                        for z in range(0, 9):
                            if maxVal < emt[z]:
                                maxVal = emt[z]
                                emtIdx = z
                        j[devStr][timeStr]['emotion'][emtIdx] = int(j[devStr][timeStr]['emotion'][emtIdx]) + 1

                    self.con.set(DATA_LABEL, js.dumps(j))
                    self.dbCon.commit();
                except Exception, e:
                    print 'e', e
