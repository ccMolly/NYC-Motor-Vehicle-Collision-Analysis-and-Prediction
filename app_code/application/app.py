from flask import Flask, request, send_from_directory, render_template
import urllib
import json
import requests
from engine import predict


cityid = {"Staten_Island": 5128581, "Brooklyn": 5110302,
          "Queens": 5133268, "Manhattan": 5125771, "Bronx": 5110253}
risk = ["LOW", "MEDIUM", "HIGH"]

# Get weather data from api, id means city id, different id relate to different api url
def getweather(id):
    url = "http://api.openweathermap.org/data/2.5/forecast?id=" + \
        str(id)+"&appid=39313eab1e90ef791a0adcccde61efe6"
    # response = urllib.urlopen(url)
    r = requests.get(url)
    data = r.json()
    temp = 20
    wind = 0
    try:
        temp = round(float(data['list'][0]['main']['temp']) - 273, 2)
    except:
        temp = 20
    try:
        wind = round(float(data['list'][0]['wind']['speed']), 2)
    except:
        wind = 0
    try:
        rain = round(float(data['list'][0]['rain']['3h']), 2)
    except:
        rain = 0
    try:
        rain = round(float(data['list'][0]['rain']['3h']), 2)
    except:
        rain = 0
    try:
        snow = round(float(data['list'][0]['snow']['1h']), 2)
    except:
        snow = 0
    try:
        snwd = round(float(data['list'][0]['snow']['3h']), 2)
    except:
        snwd = 0
    return {'temp': temp, 'wind': wind, 'rain': rain, 'snow': snow, 'snwd': snwd}

# initialize weather information part of website
def initweather():
    weather = {}
    weather["Staten_Island"] = getweather(cityid["Staten_Island"])
    weather["Brooklyn"]=getweather(cityid["Brooklyn"])
    weather["Queens"]=getweather(cityid["Queens"])
    weather["Manhattan"]=getweather(cityid["Manhattan"])
    weather["Bronx"]=getweather(cityid["Bronx"])
    print(weather)
    return weather



def create_app(sc):
    app=Flask(__name__, static_url_path='/static')
    @app.route("/")
    def render_static():
        weather=initweather()
        return render_template("index.html", data=[{'borough': 'Staten_Island'}, {'borough': 'Queens'}, {'borough': 'Brooklyn'}, {'borough': 'Manhattan'}, {'borough': 'Bronx'}],
                               speeddata=[{'speed': 20}, {'speed': 30}, {'speed': 40}, {'speed': 50}, {'speed': 60}, {'speed': 70}, {'speed': 80}, {'speed': 90}, {
                                   'speed': 100}, {'speed': 110}, {'speed': 120}, {'speed': 130}, {'speed': 140}, {'speed': 150}, {'speed': 160}, {'speed': 170}],
                               weatherdata=weather, predictdata='UNKNOWN', boroughtdata='UNKNOWN', speednow=0)
    
    @app.route('/', methods=['POST'])
    def my_form_post():
        borough=request.form.get('borough')
        speed=request.form.get('speed')
        cityId=cityid[borough]
        weather=getweather(cityId)
        # Call predict function in engine.py to get predicted risk level
        riskLevel=predict(sc, borough, speed, weather)
        weather=initweather()
        predictresult=risk[int(riskLevel)]
        return render_template("index.html", data=[{'borough': 'Staten_Island'}, {'borough': 'Queens'}, {'borough': 'Brooklyn'}, {'borough': 'Manhattan'}, {'borough': 'Bronx'}],
                               speeddata=[{'speed': 20}, {'speed': 30}, {'speed': 40}, {'speed': 50}, {'speed': 60}, {
                                   'speed': 70}, {'speed': 80}, {'speed': 90}, {'speed': 100}, {'speed': 110}, {'speed': 120}, ],
                               weatherdata=weather, predictdata=predictresult, boroughdata=borough, speednow=speed)
    app.run()

