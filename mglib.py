#!/usr/bin/python3

import datetime
import pprint
import re
import pymongo
import json

class MongoOp():
    def __init__(self):
        self.client = pymongo.MongoClient('mongodb://user:passwd@127.0.0.1:27017/')
        self.db = self.client.db
        self.tab = self.db.tab

    def get_key(self, key_val):
        key = {}
        key['key'] = key_val

        return key

    def set(self, key_val, **kvs):
        data = {}
        for k in kvs:
           data[str(k)]=kvs[k]

        key = self.get_key(key_val)
        self.tab.update(key, {"$set": data}, multi=True, upsert=True)
    def unset(self, key_val, **kvs):
        data = {}
        for k in kvs:
           data[str(k)]=kvs[k]

        key = self.get_key(key_val)
        self.tab.update(key, {"$unset": data}, multi=True, upsert=True)

    def query(self, key_val, **kvs):
        key = self.get_key(key_val)
        for k in kvs:
           key[str(k)]=kvs[k]

        return self.tab.find(key)

    def query_one(self, key_val, **kvs):
        key = self.get_key(key_val)
        for k in kvs:
           key[str(k)]=kvs[k]

        return self.tab.find_one(key)


    def delete(self, key_val):
        key = self.get_key(key_val)
        return self.tab.remove(key)
def set(key_val, **kvs):
    mc = MongoOp()
    return mc.set(key_val, **kvs)

def query_one(key_val):
    mc = MongoOp()
    res = json.dumps(mc.query_one(key_val))
    print(res)
    return res

if __name__ == "__main__":
    mc = MongoOp()
    #mc.set("1", a=1)
    set("1", a=1)
    for rec in mc.query("1"):
        #print(type(rec))
        pprint.pprint(rec)
    mc.set("1", a=2)
    mc.unset("1", a=2)
    mc.set("1", b=3)
    rec = mc.query_one("1")
    pprint.pprint(rec)

    mc.delete("1")
