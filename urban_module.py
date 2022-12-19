from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

import psycopg2
import pandas as pd
import numpy as np
import geopandas as gpd
import json
from shapely.geometry import Polygon, LineString, Point
from sqlalchemy.pool import NullPool
from sqlalchemy import exc

# функция для чтения того, что получили в запросе
def select_pg(sql):
    return pd.read_sql(sql, con_base)
# функция для изменения значений в бд
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        # print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

#имя слоя с объектами (проверить, чтобы были lon, lat)
name_sloy_obj = 'public.auto_market'
lon_obj = 'lon'
lat_obj = 'lat'

#имя слоя с кварталами (проверить, чтобы были lon, lat, id)
name_sloy_kvartal = 'urban.urban_kvartals_2018_copy_alg'
name_sloy_kvartal_ = 'urban_kvartals_2018_copy_alg'
id = 'id'
lon = 'lon'
lat = 'lat'

#название колонки для вставки значений в бд
object_column = 'tk_new'

# создали подключение к базе данных 1
con_base = create_engine('postgresql+psycopg2://postgres:gfhjkm_1@192.168.2.104/spatial001')
# координаты объекта (schools_official_2 - название слоя)

objects = f"SELECT {lon_obj},{lat_obj} FROM {name_sloy_obj}"
coord_obj = select_pg(objects)
coord_obj.rename(columns={lon_obj: 'lon_obj', lat_obj: 'lat_obj'}, inplace=True)
coord_obj = coord_obj[['lon_obj','lat_obj']]
coord_obj['geom_obj'] = gpd.points_from_xy(coord_obj.lat_obj, coord_obj.lon_obj)
coord_obj = gpd.GeoDataFrame(coord_obj,geometry=coord_obj['geom_obj'],crs='EPSG:4326')

# # координаты кварталов
urban_kvartal = f"SELECT {id},{lon},{lat} FROM {name_sloy_kvartal}"
coord_kv = select_pg(urban_kvartal)
coord_kv.rename(columns={lon: 'lon', lat: 'lat', id : 'id'}, inplace=True)
coord_kv['geom_point'] = gpd.points_from_xy(coord_kv.lat, coord_kv.lon)
coord_kv = coord_kv[['id','geom_point','lon', 'lat']]
coord_kv = gpd.GeoDataFrame(coord_kv,geometry=coord_kv['geom_point'],crs='EPSG:4326')

#отключили соединения с 1 бд
con_base = create_engine('postgresql+psycopg2://postgres:gfhjkm_1@192.168.2.104/spatial001', poolclass=NullPool)

#буффер от каждого квартала 10 км
coord_kv['buffer'] = coord_kv['geom_point'].buffer(10000).set_crs(4326)
frame = {'id': coord_kv['id'], 'geom': coord_kv['buffer']}
result = pd.DataFrame(frame)
kvartal_id_buffer = gpd.GeoDataFrame(result, geometry=result.geom, crs="EPSG:4326")
kvartal_id_buffer = kvartal_id_buffer[['id','geometry']]

#школы, которые попадают в буфер
object_in_buffer = gpd.sjoin(coord_obj,kvartal_id_buffer , how='left', op='intersects')

# объединение координатов школ и кварталов
# (id квартала, линия от центроида квартала до каждой школы, центроид квартала, координаты квартала, координаты школы)
lon_lat_kv_full = coord_kv.merge(object_in_buffer, left_on='id', right_on='id' )
lon_lat_kv_full = gpd.GeoDataFrame(lon_lat_kv_full, geometry=lon_lat_kv_full['geom_point'], crs='EPSG:4326')
lon_lat_kv_full = lon_lat_kv_full[['id','lon','lat', 'geom_point','lon_obj','lat_obj']]

list_new_meters = []
list_new_id = []


#проходим циклом по всем координатам школ и кварталов
for i, row in lon_lat_kv_full.iterrows():
    #id каждого квартала
    id = row['id']

    #координаты квартала и школы для функции
    x_kv = row['lat']
    y_kv = row['lon']
    x_sch = row['lat_obj']
    y_sch = row['lon_obj']

    #аргументы для функции
    arg = (f'{x_kv}::float, {y_kv}::float, {x_sch}::float, {y_sch}::float')

    # подключение к бд с функцией
    con_base = create_engine('postgresql+psycopg2://postgres:gfhjkm_1@192.168.2.10/gi_routing')
    # sql = f'''SELECT * FROM "public"."__asp_get_route_cars_tosh"({arg}, 'avg', 'avg')'''
    # res = (select_pg(sql))
    # res.meters = res.meters.astype(float)
    # print(res.meters)

    #запрос на выполнение функции
    try:
        sql = f'''SELECT * FROM "public"."__asp_get_route_cars_tosh"({arg}, 'avg', 'avg')'''
        #результат выполнения функции
        res = (select_pg(sql))
        res.meters = res.meters.astype(float)
        # print(res.meters)

    # except exc.SQLAlchemyError:
    except:
        df_kv = pd.DataFrame({'x_kv': [x_kv], 'y_kv': [y_kv],'x_sch': [x_kv], 'y_sch': [y_sch]})
        coords_kv_fals = gpd.points_from_xy(df_kv.x_kv, df_kv.y_kv)
        coords_sch_fals = gpd.points_from_xy(df_kv.x_sch, df_kv.y_sch)
        res.meters = coords_kv_fals.distance(coords_sch_fals)*2.7
        # print(res.meters)
        # print('здесь были странные координаты')

    meters = res['meters'].tolist()
    list_new_meters.append(meters)

    list_new_meters_new = [x for sublist in list_new_meters for x in sublist]
    list_new_id.append(id)



intermediate_dictionary = {'id': list_new_id, 'dist': list_new_meters_new}
id_metres = pd.DataFrame.from_dict(intermediate_dictionary)

# #наименьшее расстояние
min_dist = id_metres['dist'].min()
# #наибольшее расстояние
max_dist = id_metres['dist'].max()
# #
# #t['min'] - минимальное расстояние от центроида этого квартала до всех школ
t = id_metres.groupby('id',dropna=False)['dist'].agg(['count','min']).reset_index()
# #посчитанное нужное значение
min_kv_metres = pd.to_numeric(t['min'])
t['itog'] = (min_kv_metres-min_dist)/(max_dist - min_dist)

#отключили соединения с 2 бд
con_base = create_engine('postgresql+psycopg2://postgres:gfhjkm_1@192.168.2.10/gi_routing', poolclass=NullPool)

connection = create_connection("spatial001", "postgres", "gfhjkm_1", "192.168.2.104", "5432")
cursor = connection.cursor()

cursor.execute(f"CREATE TABLE urban.{object_column} (id integer PRIMARY KEY, itog float)")

for r, itogs in t.iterrows():

    id_itog = itogs.id
    itog = itogs.itog
    cursor.execute(f"INSERT INTO urban.{object_column} (itog, id) VALUES ({itog},{id_itog})")
    connection.commit()
#
#
# # print(cursor.execute(f'''
# # SELECT object.itog FROM urban.{object_column} as object
# # LEFT JOIN {name_sloy_kvartal} as kvartal
# # ON kvartal.id = object.id'''))
#
# cursor.execute(f'''
# update {name_sloy_kvartal}
# set {object_column} = itog_table.itog
# FROM
# (SELECT object.id, object.itog FROM urban.{object_column} as object
# LEFT JOIN {name_sloy_kvartal} as kvartal
# ON kvartal.id = object.id) AS itog_table
#
# where {name_sloy_kvartal_}.id= itog_table.id''')
#
# cursor.execute(f"DROP TABLE urban.{object_column}")
#
# connection.commit()

cursor.close()
connection.close()






