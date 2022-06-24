SELECT
    TRIP_DURATION,
    START_TIME,
    STOP_TIME,
    START_STATION_ID,
    START_STATION_NAME,
    START_STATION_LATITUDE,
    START_STATION_LONGITUDE,
    END_STATION_LATITUDE,
    END_STATION_LONGITUDE,
    END_STATION_ID,
    END_STATION_NAME,
    BIKE_ID,
    HAVERSINE( START_STATION_LATITUDE, START_STATION_LONGITUDE, END_STATION_LATITUDE, END_STATION_LONGITUDE ) DISTANCE
FROM {{ref('base_view_trips')}} src