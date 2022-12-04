from flask import Flask,request,jsonify
from key_generator.key_generator import generate
from consultas_sql import *
import ast
key = generate()
key.get_key()

app=Flask(__name__)


#----------------------------------------------------------------------------------------------

@app.route('/ping')
def ping():
    return 'Pong'
#--------------------------------------------LOCATIONS------------------------------------
@app.route('/api/v1/locations',methods=['GET'])
def obtenerlocations():
    #comprobar api key entregada
    compania = request.headers.get('company_api_key')
    company_id=comprobar_company_api_key(compania)
    if company_id=='error':
        return 'company_api_key error',400
    #obtener datos
    res=obtener_locations(company_id)
    return jsonify(res)

@app.route('/api/v1/locations/<id>',methods=['GET'])
def obtenerlocation(id):
    #comprobar api key entregada
    compania = request.headers.get('company_api_key')
    company_id=comprobar_company_api_key(compania)
    if company_id=='error':
        return 'company_api_key error',400
    #obtener lacation
    res=obtener_location(company_id,id)
    print(res)
    return jsonify(res)

@app.route('/api/v1/locations/<id>',methods=['PUT'])
def editarlocation(id):
    compania = request.headers.get('company_api_key')
    company_id=comprobar_company_api_key(compania)
    if company_id=='error':
        return 'company_api_key error',400
    
    location_name = request.args.get('location_name')
    location_country = request.args.get('location_country')
    location_meta = request.args.get('location_meta')
    location_city = request.args.get('location_city')
    update_location(location_name,location_country,location_city ,location_meta,company_id,id)
    return 'location editada'

@app.route('/api/v1/locations/<id>',methods=['DELETE'])
def eliminarlocation(id):
    compania = request.headers.get('company_api_key')
    company_id=comprobar_company_api_key(compania)
    if company_id=='error':
        return 'company_api_key error',400
    delete_location(company_id,id)
    return 'location eliminada'

#-----------------------------------------SENSORES----------------------------------------

@app.route('/api/v1/sensores',methods=['GET'])
def obtenersensores():
    #comprobar api key entregada
    compania = request.headers.get('company_api_key')
    company_id=comprobar_company_api_key(compania)
    if company_id=='error':
        return 'company_api_key error',400
    #obtener datos
    res=obtener_sensores(company_id)
    return jsonify(res)

@app.route('/api/v1/sensores/<id>',methods=['GET'])
def obtenersensor(id):
    #comprobar api key entregada
    compania = request.headers.get('company_api_key')
    company_id=comprobar_company_api_key(compania)
    if company_id=='error':
        return 'company_api_key error',400
    #obtener datos
    res=obtener_sensor(company_id,id)
    return jsonify(res)


@app.route('/api/v1/sensores/<id>',methods=['PUT'])
def editarsensores(id):
    compania = request.headers.get('company_api_key')
    company_id=comprobar_company_api_key(compania)
    if company_id=='error':
        return 'company_api_key error',400
    sensor_name = request.args.get('sensor_name')
    sensor_category = request.args.get('sensor_category')
    sensor_meta = request.args.get('sensor_meta')
    res=obtener_sensor(company_id,id)
    print(res)
    if res == []:
        return 'error al editar'
    update_sensor(sensor_name,sensor_category,sensor_meta,company_id,id)
    return 'sensor editado'

@app.route('/api/v1/sensores/<id>',methods=['DELETE'])
def eliminarsensores(id):
    compania = request.headers.get('company_api_key')
    company_id=comprobar_company_api_key(compania)
    if company_id=='error':
        return 'company_api_key error',400
    res=obtener_sensor(company_id,id)

    if res == []:
        return 'error al eliminar'
    delete_sensor(company_id,id)
    return 'sensor eliminado'

#--------------------------------------------------------------------------------------

@app.route('/api/v1/sensor_data',methods=['POST'])
def addsensordata():
    datas=request.get_json()
    sensor_api_key=datas["api_key"]
    id=comprobar_sensor(sensor_api_key)
    if id==False:
        return '',400
    data=datas["json_data"]
    for i in data:
        time=i["Tiempo"]
        temp=i["Temperatura"]
        agregar_data_temp(id,time,temp)
    return 'agregado',201


@app.route('/api/v1/sensor_data',methods=['GET'])
def obtenerdatos():
    conpania = request.headers.get('company_api_key')
    From = request.args.get('from')
    if From == '':
        From=0
    to =request.args.get('to')
    if to == '':
        to=9223372036854775807#valor 
    sensores =request.args.get('sensor_id')
    sensores=ast.literal_eval(sensores)
    print(sensores)
    
    id_company=comprobar_company_api_key(conpania)
    if id_company=='error':
        return 'error company_api_key',400
    
    respuesta=[]
    for i in sensores:
        respuesta.append(obtener_datos(From,to,i))

        return jsonify(respuesta)

#-----------------------------------ADMIN--------------------------------------------------

@app.route('/api/v1/createcompany',methods=['POST'])
def crearcompany():
    user=request.args.get('Username')
    password=request.args.get('Password')
    comp=comprobar_admin(user,password)
    if comp==False:
        return 'este usuario no es admin',400
    
    id=request.args.get('ID')
    company_name=request.args.get('company_name')
    key = generate()
    clave=key.get_key()
    create_company(id,company_name,clave)
    return 'company create',201

@app.route('/api/v1/createlocation',methods=['POST'])
def crearlocation():
    user=request.args.get('Username')
    password=request.args.get('Password')
    comp=comprobar_admin(user,password)
    if comp==False:
        return 'este usuario no es admin',400
    
    id=request.args.get('id')
    if id == '':
        return '',400
    company_id=request.args.get('company_id')
    if company_id == '':
        return '',400
    location_name=request.args.get('location_name')
    if location_name == '':
        return '',400
    location_country=request.args.get('location_country')
    if location_country == '':
        return '',400
    location_city=request.args.get('location_city')
    print('aquiiiii'+location_city) 
    if location_city == '':
        return '',400
    location_meta=request.args.get('location_meta')
    create_location(id,company_id,location_name,location_country,location_city,location_meta)
    return 'location create',201

@app.route('/api/v1/createsensor',methods=['POST'])
def crearsensor():
    user=request.args.get('Username')
    password=request.args.get('Password')
    comp=comprobar_admin(user,password)
    if comp==False:
        return 'este usuario no es admin',400
    
    sensor_id=request.args.get('sensor_id')
    if sensor_id == '':
        return '',400
    location_id=request.args.get('location_id')
    if location_id == '':
        return '',400
    sensor_name=request.args.get('sensor_name')
    if sensor_name == '':
        return '',400
    sensor_category=request.args.get('sensor_category')
    if sensor_category == '':
        return '',400
    sensor_meta=request.args.get('sensor_meta')
    
    key = generate()
    clave=key.get_key()
    
    create_sensor(location_id,sensor_id,sensor_name,sensor_category,sensor_meta,clave)
    return 'sensor create',201


if __name__== '__main__':
    app.run(debug=True)
