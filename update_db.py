# da sostituire col connector per postrgresql:
#import psycopg2
import pymysql.cursors

import os

###################################################
# functions

# check if table exists
def table_exists(_conn, _table, _schema):
    #sql = "SELECT * FROM information_schema.tables WHERE table_name = '"+_table+"'"
    sql = "SELECT * FROM information_schema.tables WHERE table_name = '"+_table+"' and table_schema='"+_schema+"'"
    rc=False
    try:
        cursor=_conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchone()
        if   (data):
              rc=True
    finally:
        cursor.close()

    return rc

# for create table (DDL must be passed as _sql)
def create_table(_conn, _sql):
    rc=False
    try:
        cursor=_conn.cursor()
        cursor.execute(_sql)
        rc=True
    except: 
        rc=False
    finally:
        cursor.close()

    return rc

# insert records for tables to be updated
def init_tb_update(_conn, _table):
    # del_col = 'Y' se la tabella ha anche la colonna "deleted_at"
    # (non dovrebbe servire piu', piu' sotto viene testato se esiste la colonna, ma la lascio, non si sa mai)
    sql = "INSERT INTO "+_table+"(table_name, db_to, db_from, del_col, last_update) VALUES (%s, %s, %s, %s, %s)"
    rc=False
    try:
        cursor=_conn.cursor()
        cursor.execute(sql,          ('users'   , 'to'   , 'from_1' , 'N', ''))
        cursor.execute(sql,          ('tab_2'   , 'to'   , 'from_1' , 'N', ''))
        cursor.execute(sql,          ('tab_3'   , 'to'   , 'from_2' , 'Y', ''))
        _conn.commit()
        rc=True
#   except: 
#       rc=False
    finally:
        cursor.close()

    return rc

###################################################
# main

#esempio di connessione
#conn_db_to = pymysql.connect(host='localhost',
#                       user='test',
#                       password='test',
#                       port=3306,
#                       db='test')

# da configurare con database/user/password/port del caso
# to     =database destinazione
# from_1 =database origine 1
# from_2 =database origine 2
parm = { 'conn_db_to'     : {'host':'localhost' , 'user':'test'  , 'password':'test'  , 'port':3306 , 'db':'test'}
        ,'conn_db_from_1' : {'host':'localhost' , 'user':'test2' , 'password':'test2' , 'port':3306 , 'db':'test2'}
        ,'conn_db_from_2' : {'host':'localhost' , 'user':'test3' , 'password':'test3' , 'port':3306 , 'db':'test3'}
       }

# crea le connessioni
conn_db_to     = pymysql.connect( **parm['conn_db_to'] )     #mysql
conn_db_from_1 = pymysql.connect( **parm['conn_db_from_1'] ) #mysql
conn_db_from_2 = pymysql.connect( **parm['conn_db_from_2'] ) #mysql
#conn_db_to     = psycopg2.connect( **parm['conn_db_to'] )     # postgresql
#conn_db_from_1 = psycopg2.connect( **parm['conn_db_from_1'] ) # postgresql
#conn_db_from_2 = psycopg2.connect( **parm['conn_db_from_2'] ) # postgresql

# table containing the name of the tables to copy (and db from/to+max/last timestamp)
tb_update="TB_UPDATE"
schema="test" # nome database destinazione

# se tabella 'pilota' non esiste la crea
if   (table_exists(conn_db_to, tb_update, schema)):
      #print("table_exists() - table %s exists " % tb_update)
      print("");
else:
      print("table_exists() - table %s does not exists, creating: " % tb_update)
      sql="""create table TB_UPDATE (table_name  VARCHAR(30) NOT NULL PRIMARY KEY,
                                     db_to       VARCHAR(30) NOT NULL,
                                     db_from     VARCHAR(30) NOT NULL,
                                     del_col     CHAR(1)     NOT NULL,
                                     last_update VARCHAR(30) NOT NULL)"""

      if   (create_table(conn_db_to, sql)):
            print("table %s created " % tb_update)
            # popola tabella 'pilota'
            if   (init_tb_update(conn_db_to, tb_update)):
                  print("table %s initialized " % tb_update)
            else:
                  print("table %s NOT initialized " % tb_update)
                  os._exit(1)
      else:
            print("table %s NOT created " % tb_update)
            os._exit(1)

try:
    #cursor=conn_db_to.cursor()
    #cursor=conn_db_to.cursor(cursor_factory=psycopg2.extras.DictCursor) # postgresql
    cursor=conn_db_to.cursor(pymysql.cursors.DictCursor) #mysql

    sql = "select version()"
    cursor.execute(sql)
    data = cursor.fetchone()
    print("Database version : %s\n" % data)

    sql = "select * from "+tb_update
    cursor.execute(sql)
    while True:
          row = cursor.fetchone()
          if not row:
             break

          #print ("row: ", row)

          #tstamp=row[4].strip()
          tstamp=row['last_update'].strip()
          #tstamp=row[4].strip()
          tstamp=row['last_update'].strip()
          print ("Examining table %s.%s (where timestamp > '%s' )" % (row['db_from'], row['table_name'], tstamp) )

          #sql_read="select * from "+row[0]+" where created_at > '"+tstamp+"' or updated_at > '"+tstamp+"'"
          sql_read="select * from "+row['table_name']+" where created_at > '"+tstamp+"' or updated_at > '"+tstamp+"'"
          #if   row[3] in ['y', 'Y']:
          if   row['del_col'] in ['y', 'Y']:
               sql_read=sql_read+" or deleted_at > '"+tstamp+"'"
          #print("select :: %s", sql_read)
          if   ( row['db_from'] == 'from_1' ):
                 conn_db_from=conn_db_from_1
          elif ( row['db_from'] == 'from_2' ):
                 conn_db_from=conn_db_from_2

          #cursor_to  =conn_db_to.cursor(cursor_factory=psycopg2.extras.DictCursor)   #postgresql
          #cursor_from=conn_db_from.cursor(cursor_factory=psycopg2.extras.DictCursor) #postgresql
          cursor_to  =conn_db_to.cursor(pymysql.cursors.DictCursor)   #mysql
          cursor_from=conn_db_from.cursor(pymysql.cursors.DictCursor) #mysql
          cursor_from.execute(sql_read)
          max_timestamp=''
          ctrRead=0;
          ctrWrite=0;
          while True:
                  row_from = cursor_from.fetchone()
                  if not row_from:
                     break

                  ctrRead+=1
                  #print ("row_from: ", row_from)
                  cols= '('+', '.join(row_from.keys())+')'    
                  #cols= "('"+"', '".join(row_from.keys())+"')"        # se non funziona prova questa
                  #vals= '('+','.join(map(str,row_from.values()))+')'  # ed eventualmente questa  
                  vals= "('"+"', '".join(map(str, row_from.values()))+"')"
                  sql_insert = """INSERT INTO %s %s VALUES %s"""%(row['table_name'], cols, vals)
                  #print ("insert: ", sql_insert)
                  if   ( row_from['created_at'] ):
                         if   ( max_timestamp < str(row_from['created_at']) ):
                                max_timestamp = str(row_from['created_at']) 
                  if   ( row_from['updated_at'] ):
                         if   ( max_timestamp < str(row_from['updated_at']) ):
                                max_timestamp = str(row_from['updated_at']) 
                  if   ( row_from['deleted_at'] ):
                         if   ( max_timestamp < str(row_from['deleted_at']) ):
                                max_timestamp = str(row_from['deleted_at']) 

                  #print ("   max_timestamp: ", max_timestamp)
                  cursor_to.execute( sql_insert )
                  ctrWrite+=1

          print ("   read: %d - write: %d" % (ctrRead,ctrWrite));

          if   ( max_timestamp > '' ):
                 sql_update="UPDATE "+tb_update+" SET last_update='"+max_timestamp+"' where table_name='"+row['table_name']+"'"
                 #print (">>> update: ", sql_update)
                 cursor_to.execute( sql_update )
                 print ("   table %s.%s updated (%s)" % (row['db_from'], row['table_name'], max_timestamp) )

          conn_db_to.commit()
                  
    # connection is not autocommit by default. So you must commit to save your changes.

finally:
    conn_db_to.commit()
    conn_db_to.close()

