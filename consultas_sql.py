#----------------------------------------CONSULTAS SQL------------------------------------------------------
import sqlite3
from key_generator.key_generator import generate
def comprobar_company_api_key(company_api_key):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res = cur.execute("select ID,company_api_key from Company")
    res=res.fetchall()
    for i in res:
        if i[1]==company_api_key:
            return i[0]
    return 'error'
def comprobar_admin(user,clave):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res = cur.execute("select Username,Password from Admin")
    res=res.fetchone()
    if res[0]==user and res[1]==clave:
        return True
    else:
        return False
    
def comprobar_sensor(sensor_api_key):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res = cur.execute("select sensor_id from Sensor where sensor_api_key='%s'"%sensor_api_key)
    res=res.fetchone()
    if res== None:
        return False
    return res[0]
#----------------------------------------CONSULTAS SQL:LOCATIONS------------------------------------------------------
def obtener_locations(company_id):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res = cur.execute("select * from Location where company_id=%s"%company_id)
    res=res.fetchall()
    return res

def obtener_location(company_id,id):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res = cur.execute("select * from Location where company_id=%s and id=%s"%(company_id,id))
    res=res.fetchone()
    return res

def update_location(location_name,location_country,location_city,location_meta,company_id,id):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    cur.execute("UPDATE Location SET location_name='%s' , location_country='%s',location_city='%s' ,location_meta='%s' WHERE company_id=%s and id=%s"%(location_name,location_country,location_city ,location_meta,company_id,id))
    con.commit()

def delete_location(company_id,id):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    cur.execute("DELETE FROM Location WHERE company_id=%s and id=%s"%(company_id,id))
    con.commit()
#------------------------------------------CONSULTAS SQL:SENSORES---------------------------------------------------
def obtener_sensores(company_id):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res = cur.execute("select sensor.location_id,sensor.sensor_id,sensor.sensor_name,sensor.sensor_category,sensor.sensor_meta,sensor.sensor_api_key from sensor inner join location on sensor.location_id=location.id where location.company_id=%s;"%company_id)
    res=res.fetchall()
    return res

def obtener_sensor(company_id,id):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res = cur.execute("select sensor.location_id,sensor.sensor_id,sensor.sensor_name,sensor.sensor_category,sensor.sensor_meta,sensor.sensor_api_key from sensor inner join location on sensor.location_id=location.id where location.company_id=%s and sensor.sensor_id=%s;"%(company_id,id))
    res=res.fetchall()
    return res

def update_sensor(sensor_name,sensor_category,sensor_meta,company_id,id):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res = cur.execute("UPDATE sensor SET sensor_name='%s',sensor_category='%s',sensor_meta='%s' WHERE (select sensor.sensor_id from sensor inner join location on sensor.location_id=location.id where location.company_id=%s and sensor.sensor_id=%s)=%s;"%(sensor_name,sensor_category,sensor_meta,company_id,id,id))
    con.commit()

def delete_sensor(company_id,id):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res = cur.execute("DELETE FROM sensor WHERE (select sensor.sensor_id from sensor inner join location on sensor.location_id=location.id where location.company_id=%s and sensor.sensor_id=%s)=%s;"%(company_id,id,id))
    con.commit()
#--------------------------------------CONSULTAS SQL:ADMIN-------------------------------------------------------
def create_company(id,company_name,company_api_key):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res=cur.execute("INSERT INTO Company VALUES (%s,'%s','%s')"%(id,company_name,company_api_key))
    con.commit()
def create_location(id,company_id,location_name,location_country,location_city,location_meta):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res=cur.execute("INSERT INTO Location VALUES (%s,%s,'%s','%s','%s','%s')"%(id,company_id,location_name,location_country,location_city,location_meta))
    con.commit()
def create_sensor(location_id,sensor_id,sensor_name,sensor_category,sensor_meta,clave):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res=cur.execute("INSERT INTO Sensor VALUES (%s,%s,'%s','%s','%s','%s')"%(location_id,sensor_id,sensor_name,sensor_category,sensor_meta,clave))
    con.commit()

#--------------------------------------CONSULTAS SQL:datos-------------------------------------------------------
def agregar_data_temp(id,time,temp):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res=cur.execute("INSERT INTO data_temperatura VALUES (%s,%s,%s)"%(id,time,temp))
    con.commit()

def obtener_datos(From,to,id_sensor):
    con = sqlite3.connect("emergentes.db")
    cur = con.cursor()
    res=cur.execute("select time,temperatura from data_temperatura where time>=%s and time<=%s and id_sensor=%s"%(From,to,id_sensor))
    res=res.fetchall()
    return res