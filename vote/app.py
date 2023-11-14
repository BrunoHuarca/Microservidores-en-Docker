from flask import Flask, render_template, request, make_response, g
from redis import Redis
import os
import socket
import random
import json
import logging
from math import sqrt
import csv

option_a = os.getenv('OPTION_A', "Manhattan")
option_b = os.getenv('OPTION_B', "Pearson")
hostname = socket.gethostname()

def manhattan(rating1, rating2):
    distance = 0
    total = 0
    for key in rating1:
        if key in rating2:
            distance += abs(rating1[key] - rating2[key])
            total += 1
    if total > 0:
        return distance / total
    else:
        return -1  # Indica que no hay calificaciones en común

def pearson(rating1, rating2):
    sum_xy = 0
    sum_x = 0
    sum_y = 0
    sum_x2 = 0
    sum_y2 = 0
    n = 0
    for key in rating1:
        if key in rating2:
            n += 1
            x = rating1[key]
            y = rating2[key]
            sum_xy += x * y
            sum_x += x
            sum_y += y
            sum_x2 += pow(x, 2)
            sum_y2 += pow(y, 2)
    # ahora calcula el denominador
    denominator = sqrt(sum_x2 - pow(sum_x, 2) / n) * sqrt(sum_y2 - pow(sum_y, 2) / n)
    if denominator == 0:
        return 0
    else:
        return (sum_xy - (sum_x * sum_y) / n) / denominator

app = Flask(__name__)

gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.INFO)

def get_redis():
    if not hasattr(g, 'redis'):
        g.redis = Redis(host="redis", db=0, socket_timeout=5)
    return g.redis

def cargar_datos_desde_csv(ruta_csv):
    datos = {}
    with open(ruta_csv, newline='', encoding='utf-8') as archivo_csv:
        lector_csv = csv.DictReader(archivo_csv)
        for fila in lector_csv:
            userId = fila['userId']
            movieId = fila['movieId']
            rating = float(fila['rating'])
            if userId not in datos:
                datos[userId] = {}
            datos[userId][movieId] = rating
    return datos

ruta_csv = 'ratings.csv'  # Cambia esto a la ubicación real de tu archivo CSV
usuarios = cargar_datos_desde_csv(ruta_csv)

@app.route("/", methods=['POST', 'GET'])
def distancias():
    voter_id = request.cookies.get('voter_id')
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]
    vote = None
    if request.method == 'POST':
        redis = get_redis()
        user_1 = request.form['option_a']
        user_2 = request.form['option_b']

        if user_1 in usuarios and user_2 in usuarios:
            distancia_pearson = str(pearson(usuarios[user_1], usuarios[user_2]))
            distancia_manhattan = str(manhattan(usuarios[user_1], usuarios[user_2]))
            data = json.dumps({'voter_id': voter_id, 'distancia_manhattan': distancia_manhattan, 'distancia_pearson': distancia_pearson})
            redis.rpush('distancias', data)
        else:
            return "Usuarios no encontrados en los datos cargados desde el CSV"

    resp = make_response(render_template(
        'index.html',
        option_a=option_a,
        option_b=option_b,
        hostname=hostname,
    ))
    resp.set_cookie('voter_id', voter_id)
    return resp

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True, threaded=True)
