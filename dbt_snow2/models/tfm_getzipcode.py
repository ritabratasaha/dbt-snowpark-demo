import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import IntegerType, FloatType, StringType
import pandas as pd
import yaml
import json
import geopy
from geopy.geocoders import Nominatim
        

## Reading connection parameters from dbt profile.yml
with open(r'/Users/rsaha/.dbt/profiles.yml') as file :
        profile_documents = yaml.full_load(file)

account = (profile_documents['dbt_snow']['outputs']['dev']['account'])
user = (profile_documents['dbt_snow']['outputs']['dev']['user'])
password = (profile_documents['dbt_snow']['outputs']['dev']['password'])
role = (profile_documents['dbt_snow']['outputs']['dev']['role'])
warehouse = (profile_documents['dbt_snow']['outputs']['dev']['warehouse'])
database = (profile_documents['dbt_snow']['outputs']['dev']['database'])
schema = (profile_documents['dbt_snow']['outputs']['dev']['schema'])

connection_parameters_str = "{" + "\"account\"" + ":\"" + account + "\"," \
                                "\"user\"" + ":\"" + user + "\"," \
                                "\"password\"" + ":\"" + password + "\"," \
                                "\"role\"" + ":\"" + role + "\"," \
                                "\"warehouse\"" + ":\"" + warehouse + "\"," \
                                "\"database\"" + ":\"" + database + "\"," \
                                "\"schema\"" + ":\"" + schema + "\"}"


def get_startloc_zipcode(row):
    
    try :
        location = geolocator.reverse((row["START_STATION_LATITUDE"],row["START_STATION_LONGITUDE"])) 
        zipcode = location.raw['address']['postcode']
    except KeyError:
        zipcode = '99999'
        pass
    
    return zipcode

def get_endloc_zipcode(row):

    try :
        location = geolocator.reverse((row["END_STATION_LATITUDE"],row["END_STATION_LONGITUDE"]))
        zipcode = location.raw['address']['postcode']
    except KeyError:
        zipcode = '99999'
        pass

    return location.raw['address']['postcode']


try:

    df_table = ref('tfm_trip_distance')
    geolocator = Nominatim(user_agent="geoapiExercises")
    df_table['START_LOC_ZIPCODE'] = df_table.apply(get_startloc_zipcode,axis=1)
    df_table['END_LOC_ZIPCODE'] = df_table.apply(get_endloc_zipcode,axis=1)
    write_to_model(df_table)

except Exception as e:
    print("Oops!", e.__class__, "occurred.")
    print()

## You can write your snowpark script here

connection_parameters = json.loads(connection_parameters_str)
session = Session.builder.configs(connection_parameters).create()
session.sql("select current_warehouse(), current_database(), current_schema()").collect()
session.close()