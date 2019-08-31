'''
COMP9321 2019 Term 1 Assignment Two Code Template
Student Name:XIAO ZHENXI
Student ID:z5202238
'''
from flask import Flask,request
from flask_restplus import Api,Resource,fields,reqparse
import requests
import pandas as pd
import json
import time
import sqlite3
from sqlite3 import Error
import random
location = ''
def create_db(db_file):
    try:
        open_db =sqlite3.connect(db_file)
        print("connected succeed")
    except Error as e:
        print(e)
    sql_create_table = """CREATE TABLE IF NOT EXISTS projects(
                            indicator text,
                            collections text,
                            collections_id text,
                            country text,
                            time text,
                            value text,
                            creation_time text,
                            indicator_value text)"""
    try:
        c = open_db.cursor()
        c.execute(sql_create_table)
        print('table succeed')
    except Error as e:
        print(e)
    finally:
        open_db.close()
app = Flask(__name__)
api = Api(app)
input_model = api.model('input_model',{'indicator_id' : fields.String('NY.GDP.MKTP.CD')})
col = []
@api.route('/<collections>')
class Language(Resource):
    @api.expect(input_model)
    @api.response(201,'Created')
    def post(self,collections):
        data_id = 0
        indicatorId = api.payload
        string_1 = indicatorId['indicator_id']
        urlstring_1 = "http://api.worldbank.org/v2/countries/all/indicators/"
        urlstring_2 = "?date=2013:2018&format=json&per_page=2000"
        urlstring_1 = urlstring_1+string_1
        urlstring_1 = urlstring_1+urlstring_2
        resp = requests.get(url=urlstring_1)
        data = resp.json()
        data_1 = data[1]
        create_db("data.db")
        conn = sqlite3.connect("data.db")
        collection_id= random.randint(0,10000)
        ca = collection_id
        creation_time = time.strftime('%Y-%m-%dT%H:%M:%SZ',time.localtime(time.time()))
        ct = creation_time
        
        for item in data_1:
            data_id = data_id + 1
            date_1 = str(item['date'])
            value = item['value']
            country = item['country']
            country_id = country['value']
            i_id = item['indicator']
            i_id_v = i_id['value']
            c = conn.cursor()
            sql = '''insert into projects
                     (indicator,collections,collections_id,country,time,value,creation_time,indicator_value)
                     values
                     (:st_indicator,:st_collections,:st_collections_id,:st_country,:st_time,:st_value,:st_creation_time,:st_indicator_value)'''
            c.execute(sql,{'st_indicator':string_1,'st_collections':collections,'st_collections_id':str(ca),'st_country':country_id,'st_time':date_1,'st_value':value,'st_creation_time':ct,'st_indicator_value':i_id_v})
            conn.commit()
        collection_id= random.randint(0,100)
        location = '/'
        location = location + collections + '/'+str(ca)
        creation_time = time.strftime('%Y-%m-%d %H/%M/%S',time.localtime(time.time()))
        indicator = string_1
        return_dic = {}
        return_dic.update({'location':location})
        return_dic.update({'collection_id':ca})
        return_dic.update({'creation_time':creation_time})
        return_dic.update({'indicator':indicator})
        col.append(return_dic)
        conn.close()
        return return_dic,201
    @api.response(200,'OK')
    def get(self,collections):
        conn = sqlite3.connect("data.db")
        c = conn.cursor()
        sql = "select collections,collections_id,indicator,creation_time from projects where collections = '"
        sql = sql + collections
        sql = sql + "'"
        c.execute(sql)
        cre = c.fetchall()
        roll = []
        result = []
        for item in cre:
            if item not in roll:
                roll.append(item)
        for item in roll:
            dic_1 = {}
            string_1 = '/'
            string_1 = string_1 + item[0] +'/'+item[1]
            dic_1.update({'location':string_1})
            dic_1.update({'collection_id':str(item[1])})
            dic_1.update({'creation_time':item[3]})
            dic_1.update({'indicator':item[2]})
            result.append(dic_1)
        conn.close()
        return result
@api.route('/<collections>/<collection_id>')
class delete(Resource):
    @api.response(200,'OK')
    def delete(self,collections,collection_id):
        conn = sqlite3.connect("data.db")
        c = conn.cursor()
        sql = "delete from projects where collections_id = '"
        sql = sql + collection_id
        sql = sql + "'"
        c.execute(sql)
        conn.commit()
        conn.close()
        string_1 = 'Collection = '
        string_1 = string_1 + collection_id + ' is removed from database'
        return {'message' : string_1}
    @api.response(200,'OK')
    def get(self,collections,collection_id):
        conn = sqlite3.connect("data.db")
        c = conn.cursor()
        sql = "select creation_time,country,time,value,indicator,indicator_value from projects where collections_id = '"
        sql = sql + collection_id + "'"
        c.execute(sql)
        cre = c.fetchall()
        roll = []
        creation_time = ''
        indicator = ''
        for item in cre:
            dic_1 = {}
            dic_1.update({'country':item[1]})
            dic_1.update({'date':int(item[2])})
            dic_1.update({'value':item[3]})
            creation_time = item[0]
            indicator = item[4]
            indicator_value = item[5]
            roll.append(dic_1)
        result = {}
        result.update({'collection_id':collection_id})
        result.update({'indicator':indicator})
        result.update({'indicator_value':indicator_value})
        result.update({'creation_time':creation_time})
        result.update({'entries':roll})
        conn.close()
        return result
@api.route('/<collections>/<collection_id>/<year>/<country>')
class get1(Resource):
    @api.response(200,'OK')
    def get(self,collections,collection_id,year,country):
        conn=sqlite3.connect("data.db")
        c= conn.cursor()
        sql = "select indicator,value from projects where collections_id = '"
        sql = sql + collection_id +"'"+" and time = '" + year +"'" + "and country = '"
        sql = sql + country + "'"
        c.execute(sql)
        cre = c.fetchall()
        d = cre[0]
        indicator = d[0]
        value_in = d[1]
        dic_1 = {}
        dic_1.update({"collection_id":collection_id})
        dic_1.update({"indicator":indicator})
        dic_1.update({"country":country})
        dic_1.update({"year":year})
        dic_1.update({'value':value_in})
        conn.close()
        return dic_1
parser=reqparse.RequestParser()
parser.add_argument('query',required=True)
@api.route('/<collections>/<string:collection_id>/<string:year>')
class get2(Resource):
    @api.expect(parser,validate=True)
    @api.response(200,'OK')
    def get(self,collections,collection_id,year):
        args = parser.parse_args()
        query = args.get('query')
        conn = sqlite3.connect("data.db")
        c = conn.cursor()
        sql = "select indicator,indicator_value,country,time,value from projects where collections = '"
        sql = sql + collections + "'" + " and collections_id = '" + collection_id + "'"
        sql = sql + " and time = '" + year + "'"
        c.execute(sql)
        cre = c.fetchall()
        sort_list = []
        sort_dic = {}
        result = []
        ax = cre[0]
        indicator1 = ax[0]
        indicator1_value = ax[1]
        for item in cre:
            if not item[4] == None:
                sort_dic.update({item[2]:item[4]})
                indicator1 = item[0]
                indicator1_value = item[1]
        if 'top' in query:
            a = int(query.strip().strip('top'))
            if sort_dic:
                check_a = sorted(sort_dic.items(),key = lambda x:x[1],reverse = True)
            else:
                check_a = []
        elif 'bottom' in query:
            a = int(query.strip().strip('bottom'))
            if sort_dic:
                check_a = sorted(sort_dic.items(),key = lambda x:x[1],reverse = False)
            else:
                check_a = []
        if a>len(check_a):
            a = len(check_a)
        if check_a:
            for i in range(0,a):
                t = check_a[i]
                con = t[0]
                va= t[1]
                dic_1 ={}
                dic_1.update({'country':con})
                dic_1.update({'date':year})
                dic_1.update({'value':va})
                result.append(dic_1)
        dic_r = {}
        dic_r.update({'indicator':indicator1})
        dic_r.update({'indicator_value':indicator1_value})
        dic_r.update({'entries':result})
        conn.close()
        return dic_r

if __name__ == '__main__':
    app.run(debug = True)
