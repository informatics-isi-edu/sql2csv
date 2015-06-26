#!/usr/bin/python

import os
import pyodbc
import csv
import sys
import datetime
import json

py_dic = { str: 'str', buffer: 'buffer', int: 'int', float: 'float', datetime.datetime: 'datetime.datetime',
           datetime.date: 'datetime.date',datetime.time: 'datetime.time', bool: 'bool', 
           unicode: 'unicode', bytearray: 'bytearray' , long: 'long' }


PATH_SEP=os.path.sep

def read_query(query_file):
    with open(query_file) as sqlfile:
        sql_query=sqlfile.read()
        return sql_query

def read_json(input_file):
    json_input=open(input_file).read()
    return json.loads(json_input)


def val2text(v):
    if v is None:
        return ''
    elif type(v) == datetime.datetime:
        return v.isoformat(' ')
    elif type(v) in [int, float, str]:
        return '%s' % str(v)
    elif type(v) == buffer:
        try:
            return '{%s}' % uuid.UUID(bytes_le=v)
        except:
            return '\\x%s' % str(v).encode('hex')
    elif type(v) in [bool]:
        return '%s' % str(v)
    else:
        raise TypeError('value of type %s not supported yet' % type(v))

def csvescape(v):
    if v == None:
        return ''
    elif type(v) in [int, float]:
        return "%s" % v
    else:
        try:
            v = v.replace( '"', '""' )
            v = v.replace( '\n', '' )
            v = v.replace( '\r', '' )
            v = v.replace( '\00', '' )
        except:
            print 'type: %s v: %s' % (type(v), v)
            raise
        return ''.join([ '"', v.encode("utf-8"), '"' ])       


def write_csv(cursor,csv_file):
    header = []
    for colinfo in cursor.description:
        colname = colinfo[0]
        coltype = py_dic[colinfo[1]]
        header.append(colname)
        coldesc = ' '.join( [ csvescape(colname) , csvescape(coltype)] )
        sys.stderr.write( coldesc + ',\n')

    if not os.path.exists(os.path.dirname(csv_file)):
        os.makedirs(os.path.dirname(csv_file))

    with open(csv_file,'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)

    for row in cursor:
        values = []
        for i in range(0, len(row)):
            values.append(val2text(row[i]).encode("utf-8"))
        with open(csv_file,'a') as f:
            writer = csv.writer(f)
            writer.writerow(values)


def write_manifest(inputs_js):

    manifest={}
    extracts =[]
    for ee in inputs_js['EXTRACTS']:
        extract={}
        extract['table_data']=inputs_js['CSV_TARGET_DIRECTORY']+PATH_SEP+ee['schema_name']+PATH_SEP+ee['table_name']+'.csv'
        extract['schema_name']=ee['schema_name']
        extract['table_name']=ee['table_name']
        extract['column_definitions']=[{'name':'col1','type':'text'},{'name':'col2','type':'int'}]
        extract['object_columns']=[]        
        extracts.append(extract)

    manifest['extracts']=extracts
    manifest['source']=inputs_js['SERVER_NAME']
    manifest['destination']='ERMREST in vm-deb-028.misd.isi.edu'

    return manifest


def csv_filename(csv_dir,extract):
    return csv_dir+PATH_SEP+extract['schema_name']+PATH_SEP+extract['table_name']+'.csv'

        
def main(argv):
    
    if len(argv) != 2:
        sys.stderr.write(""" 
usage: python sql2csv.py <input_file.js>

input_file.js example:

{
    "SERVER_NAME":"mssqlserver.isi.edu",
    "DNS": "gpcr",
    "DATABASE_NAME":"DB_NAME",
    "USER_NAME":"db_username",
    "CSV_TARGET_DIRECTORY":"extracts",
    "EXTRACTS": [
         { "query_file" : "sql_construct.sql",
           "schema_name": "gpcr",
           "table_name" : "construct",
           "bulk_data_columns" : [],
           "unique_key_columns": ["id"]
         },
         { "query_file" : "sql_cont_target.sql",
           "schema_name": "gpcr",
           "table_name" : "cont_target",
           "bulk_data_columns" : [],
           "unique_key_columns": ["id"]
         }
    ]
}

1) Must set environment variable DBPASSWORD with corresponding authentication password for the user "db_username" in database "DB_NAME". 
   E.g., export DBPASSWORD='**********'
2) DNS is the Data Source Name used in the ODBC connector. 
3) Each query_file must contain a properly writen query in SQL.

""")
        sys.exit(1)

    inputs_js=read_json(argv[1])

    host = inputs_js['DNS']
    database = inputs_js['DATABASE_NAME']
    user = inputs_js['USER_NAME']
    #password = inputs_js['PASSWORD']
    password = os.environ['DBPASSWORD']

    py_dic = { str: 'str', buffer: 'buffer', int: 'int', float: 'float', datetime.datetime: 'datetime.datetime',bool: 'bool' }

    for extract in inputs_js['EXTRACTS']:
        sql_file=extract['query_file']
        sys.stdout.write(' QUERY FILE=%s \n' % sql_file) 

        conn = pyodbc.connect(dsn=host, database=database, user=user, password=password,charset='UTF8')    
        cursor = conn.cursor()
        sql = read_query(sql_file)
        cursor.execute(sql)
    
        col_types={}
        for colinfo in cursor.description:
            col_types[csvescape(colinfo[0])]=csvescape(py_dic[colinfo[1]])

        for col in col_types:
            sys.stdout.write('%s[%s]\n' % (col,col_types[col]))

        csv_file = csv_filename(inputs_js['CSV_TARGET_DIRECTORY'],extract)
        sys.stdout.write(' --------- CSV_FILE='+csv_file+' ----------------------- \n')

        write_csv(cursor,csv_file)

        
    manifest=write_manifest(inputs_js)
    sys.stdout.write('MANIFEST='+json.dumps(manifest,encoding='utf8')+'\n')
 
    with open('manifest.js', 'w') as f:
        json.dump(manifest, f,indent=3,encoding="utf-8")


if __name__ == '__main__':
    sys.exit(main(sys.argv))
