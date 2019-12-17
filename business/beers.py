import json
from enum import Enum
from datetime import timedelta

from websocket import create_connection

import settings as st
from business.exceptions import BeerMLException
from connectors import postgres


class BeerTypes(Enum):
    Cana = 'Caña'
    Doble = 'Doble'
    Jarra = 'Jarra'


def store_beer(beer_type, total_ml, date):
    if beer_type == BeerTypes.Cana.name:
        if total_ml < st.MIN_ML_CANA or total_ml > st.MAX_ML_CANA:
            raise BeerMLException(
                "Milliliters of beer {} is not between {} and {}.".format(beer_type, st.MIN_ML_CANA, st.MAX_ML_CANA))
    elif beer_type == BeerTypes.Doble.name:
        if total_ml < st.MIN_ML_DOBLE or total_ml > st.MAX_ML_DOBLE:
            raise BeerMLException(
                "Milliliters of beer {} is not between {} and {}.".format(beer_type, st.MIN_ML_DOBLE, st.MAX_ML_DOBLE))
    elif beer_type == BeerTypes.Jarra.name:
        if total_ml < st.MIN_ML_JARRA or total_ml > st.MAX_ML_JARRA:
            raise BeerMLException(
                "Milliliters of beer {} is not between {} and {}.".format(beer_type, st.MIN_ML_JARRA, st.MAX_ML_JARRA))
    postgres.insert_beer_record(date.isoformat(sep=' '), beer_type, total_ml)


def send_ws_message(data):
    data['Date'] = data['Date'].isoformat()
    ws = create_connection(st.BEER_CLIENT_HOST + st.WS_URL)
    ws.send(json.dumps(data))
    ws.close()


def get_beer_records(begin_date, end_date, beer_type):
    new_end_date_str = (end_date + timedelta(days=1)).isoformat()
    results = postgres.query_beer_records(begin_date.isoformat(), new_end_date_str, beer_type)
    records = []
    for r in results:
        records.append({
            'Date': r[0].isoformat(),
            'BeerType': r[1],
            'TotalML': r[2]
        })
    return records


def get_daily_report(date):
    end_date_str = (date + timedelta(days=1)).isoformat()
    begin_date_str = date.isoformat()
    total_ml = postgres.query_total_ml(begin_date_str, end_date_str)
    total_ml_by_beer = postgres.query_total_ml_by_beer(begin_date_str, end_date_str)
    total_beers_by_type = postgres.query_total_beers_by_type(begin_date_str, end_date_str)
    report = {
        "DailyTotalML": total_ml,
        "DailyTotalMLByBeer": {beer[0]: beer[1] for beer in total_ml_by_beer},
        "DailyTotalBeersByType": {beer[0]: beer[1] for beer in total_beers_by_type}
    }
    return report
