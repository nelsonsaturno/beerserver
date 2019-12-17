import json

from tornado.web import RequestHandler, HTTPError
import tornado.escape
from cerberus import Validator
from dateutil.parser import parse

from business.beers import store_beer, get_beer_records, get_daily_report, send_ws_message
from business.exceptions import BeerMLException


class BeerHandler(RequestHandler):

    async def get(self):
        schema = {
            'beginDate': {'type': 'date', 'required': True, 'coerce': lambda s: parse(s).date()},
            'endDate': {'type': 'date', 'required': True, 'coerce': lambda s: parse(s).date()},
            'beerType': {'type': 'string', 'nullable': True, 'allowed': ['Caña', 'Jarra', 'Doble']}
        }
        v = Validator(schema)
        params = {
            'beginDate': self.get_query_argument("beginDate"),
            'endDate': self.get_query_argument("endDate"),
            'beerType': self.get_query_argument("beerType", None)
        }
        if not v.validate(params):
            raise HTTPError(400, log_message=json.dumps(v.errors))
        normalized_params = v.normalized(params)
        begin_date = normalized_params['beginDate']
        end_date = normalized_params['endDate']
        beer_type = normalized_params.get('beerType', None)
        response = get_beer_records(begin_date, end_date, beer_type)
        self.write(json.dumps(response))

    async def post(self):
        schema = {
            'BeerType': {'type': 'string', 'required': True, 'allowed': ['Caña', 'Jarra', 'Doble']},
            'TotalML': {'type': 'float', 'required': True},
            'Date': {'type': 'datetime', 'required': True, 'coerce': lambda s: parse(s)}
        }
        v = Validator(schema)
        data = tornado.escape.json_decode(self.request.body)
        if not v.validate(data):
            raise HTTPError(status_code=400, log_message=json.dumps(v.errors), reason=json.dumps(v.errors))
        normalized_data = v.normalized(data)
        beer_type = normalized_data['BeerType']
        total_ml = normalized_data['TotalML']
        date = normalized_data['Date']
        try:
            store_beer(beer_type, total_ml, date)
        except BeerMLException as e:
            raise HTTPError(status_code=400, log_message=str(e), reason=str(e))
        send_ws_message(normalized_data)


class ReportHandler(RequestHandler):

    async def get(self):
        schema = {
            'date': {'type': 'date', 'required': True, 'coerce': lambda s: parse(s).date()}
        }
        v = Validator(schema)
        params = {
            'date': self.get_query_argument("date")
        }
        if not v.validate(params):
            raise HTTPError(400, log_message=json.dumps(v.errors))
        normalized_params = v.normalized(params)
        date = normalized_params['date']
        response = get_daily_report(date)
        self.write(json.dumps(response))
