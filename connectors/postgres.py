from sqlalchemy import create_engine
from sqlalchemy.sql import text

import settings as st

engine = create_engine(st.DATABASE_URL)


def insert_beer_record(date, beer_type, total_ml):
    with engine.connect() as con:
        params = {"date": date, "beer_type": beer_type, "total_ml": total_ml}
        statement = text(
            """INSERT INTO beer_record ("date", beer_type, total_ml) VALUES (:date, :beer_type, :total_ml)""")
        con.execute(statement, **params)


def query_beer_records(begin_date, end_date, beer_type):
    with engine.connect() as con:
        params = {"begin_date": begin_date, "end_date": end_date}
        if beer_type:
            params['beer_type'] = beer_type
            statement = text(
                """
                SELECT "date", beer_type, total_ml 
                FROM beer_record 
                WHERE "date" >= :begin_date AND "date" < :end_date and beer_type = :beer_type
                """
            )
        else:
            statement = text(
                """
                SELECT "date", beer_type, total_ml 
                FROM beer_record 
                WHERE "date" >= :begin_date AND "date" < :end_date
                """
            )
        results = con.execute(statement, **params).fetchall()
    return results


def query_total_ml(begin_date, end_date):
    with engine.connect() as con:
        params = {"begin_date": begin_date, "end_date": end_date}
        statement = text(
            """SELECT SUM(total_ml) FROM beer_record WHERE "date" >= :begin_date AND "date" < :end_date""")
        results = con.execute(statement, **params).fetchone()
    return results[0] if results else None


def query_total_ml_by_beer(begin_date, end_date):
    with engine.connect() as con:
        params = {"begin_date": begin_date, "end_date": end_date}
        statement = text(
            """
            SELECT beer_type, SUM(total_ml) 
            FROM beer_record 
            WHERE "date" >= :begin_date AND "date" < :end_date 
            GROUP BY beer_type
            """
        )
        results = con.execute(statement, **params).fetchall()
    return results


def query_total_beers_by_type(begin_date, end_date):
    with engine.connect() as con:
        params = {"begin_date": begin_date, "end_date": end_date}
        statement = text(
            """
            SELECT beer_type, COUNT(beer_type) 
            FROM beer_record 
            WHERE "date" >= :begin_date AND "date" < :end_date 
            GROUP BY beer_type
            """
        )
        results = con.execute(statement, **params).fetchall()
    return results
