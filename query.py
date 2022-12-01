from datetime import date, timedelta
from database import SessionLocal
from models import Bhavcopy
from sqlalchemy import desc
from sqlalchemy.sql import text

def get_db():
    db = SessionLocal()
    try:
        return db
    finally:
        db.close()

db = get_db()

def get_25_gainers_by_timestamp(obj):
    last_day = db.query(Bhavcopy).order_by(Bhavcopy.timestamp.desc()).first()
    top_25_gainers = db.query(Bhavcopy).filter(Bhavcopy.timestamp == obj.timestamp).order_by(desc((Bhavcopy.close - Bhavcopy.open) / Bhavcopy.open)).limit(25).all()
    print(obj.timestamp.strftime("%d-%m-%Y"))
    print("\n")
    print("Symbol\t Open\t Close\t Percentage Gain")
    for i in top_25_gainers:
        print(f"{i.symbol}\t {i.open}\t {i.close}\t {((i.close-i.open)/i.open)*100}%")
    print("\n\n")

def get_last_day_gainers():
    last_day = db.query(Bhavcopy).order_by(Bhavcopy.timestamp.desc()).first()
    get_25_gainers_by_timestamp(obj=last_day)

def get_last_30_days_gainers_datewise():
    # get unique timestamp
    timestamps = db.query(Bhavcopy.timestamp).distinct().order_by(Bhavcopy.timestamp.desc()).limit(30).all()
    for timestamp in timestamps:
        get_25_gainers_by_timestamp(obj=timestamp)


def get_last_30_days_gainers():
    # get the last 30 days timestamp
    timestamps = db.query(Bhavcopy.timestamp).distinct().order_by(Bhavcopy.timestamp.desc()).limit(30).all()
    first, last = timestamps[0], timestamps[-1]
    query_string = f"SELECT b1.symbol, b2.open, b1.close,\
        ((b1.close-b2.open)/b2.open)*100 as profit from bhavcopy b1,\
        bhavcopy b2 where (b1.timestamp =\
        '{first.timestamp.strftime('%Y-%m-%d')}' and b2.timestamp = \
        '{last.timestamp.strftime('%Y-%m-%d')}' and b1.symbol = b2.symbol)\
        order by ((b1.close-b2.open)/b2.open) desc limit 25;"

    query = text(query_string)
    data = db.execute(query).fetchall()
    print("\n")
    print(f"Top 25 Gainers of last 30 days from {first.timestamp.strftime('%d-%m-%Y')} to {last.timestamp.strftime('%d-%m-%Y')}\n")
    print("Symbol\t Open\t Close\t Percentage Gain")
    for i in data:
        print(f"{i.symbol}\t {i.open}\t {i.close}\t {i.profit}%")

def fetch_queries(query):
    """
    Query 
    :param queries: the queries to be executed
    :return:
    """
    match query:
        case "1":
            get_last_day_gainers()
        case "2":
            get_last_30_days_gainers_datewise()
        case "3":
            get_last_30_days_gainers()
        case _ :
            pass


def main():
    while(True):
        print("Choose the query to be executed")
        print("1. Query for current day top 25 gainers:")
        print("2. Query for last 30 days top 25 gainers datewise:")
        print("3. Top 25 Gainers of last 30 days:")
        print("4. Exit")
        query = input()
        if query == "4":
            break
        fetch_queries(query=query)