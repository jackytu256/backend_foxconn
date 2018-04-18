# coding:utf-8
# file att.py
# author thegreatchaos@thegreatchaos.com
# date 2018/04/17 21:21:05
TYPE_STR = 0
TYPE_INT = 1
TYPE_FLT = 2
class RecResTbl:
    def __init__(self):
        self.att = [["age", TYPE_STR], 
                ["arched_eyebrows", TYPE_INT],
                ["attractive", TYPE_FLT],
                ["bags_under_eyes", TYPE_INT],
                ["beard_style", TYPE_STR],
                ["bushy_eyebrows", TYPE_INT],
                ["double_chin",  TYPE_INT],
                ["eyeglasses",  TYPE_INT],
                ["gender",  TYPE_INT],
                ["hair_color", TYPE_STR],
                ["hair_length", TYPE_STR],
                ["hair_style",  TYPE_STR],
                ["heavy_makeup", TYPE_INT],
                ["race", TYPE_STR],
                ["wearing_hat", TYPE_INT],
                ["angry",  TYPE_FLT],
                ["confused", TYPE_FLT], 
                ["contempt",  TYPE_FLT],
                ["disgust",  TYPE_FLT],
                ["fear", TYPE_FLT],
                ["happy", TYPE_FLT],
                ["neutral", TYPE_FLT],
                ["sad", TYPE_FLT],
                ["surprise", TYPE_FLT],
                ["x", TYPE_FLT], 
                ["y", TYPE_FLT],
                ["w", TYPE_FLT], 
                ["h", TYPE_FLT]];
        self.fwdMap = {}
        self.invMap = {}
        for i in range(0, len(self.att)):
            self.fwdMap[i] = str(self.att[i][0]),
            self.invMap[self.att[i][0]] = i
    def getAttr(self):
        return self.att
    def getFwdMap(self):
        return self.fwdMap;
    def getInvMap(self):
        return self.invMap;


if __name__ == '__main__':
    a = RecResTbl()
