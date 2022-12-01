from bs4 import BeautifulSoup
import pandas as pd
import requests, io
from datetime import date, timedelta
from tqdm import tqdm
from models import Company
from sqlalchemy.orm import Session
from models import Base, Company, Bhavcopy
from database import SessionLocal, engine
from datetime import datetime as dt
from sqlalchemy.dialects.postgresql import insert

Base.metadata.create_all(bind=engine)


date_generator = lambda x: (date.today() - timedelta(days=x))

URL_FOR_COMPANIES = 'https://www.nseindia.com/market-data/securities-available-for-trading'

URL_FOR_BHAVCOPY = lambda x: f'https://archives.nseindia.com/content/historical/EQUITIES/{date_generator(x).strftime("%Y")}/{date_generator(x).strftime("%^b")}/cm{date_generator(x).strftime("%d%^b%Y")}bhav.csv.zip'

def get_db():
    db = SessionLocal()
    try:
        return db
    finally:
        db.close()

db = get_db()

headers = {
    'authority': 'www.nseindia.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'cross-site',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
}

# data fetching for securities available for equity segment
def fetch_data():
    response = requests.get(URL_FOR_COMPANIES, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    element = soup.find('a', text='Securities available for Equity segment (.csv)')
    file_url = element['href']
    df = pd.read_csv(file_url)
    companies = [{
        'name': df['NAME OF COMPANY'][i],
        'series' : df[' SERIES'][i], 
        'symbol' : df['SYMBOL'][i],
        'listing_date' : dt.strptime(df[' DATE OF LISTING'][i], "%d-%b-%Y"), 
        'paid_up_value' : int(df[' PAID UP VALUE'][i]),
        'market_lot' : int(df[' MARKET LOT'][i]),
        'isin' : df[' ISIN NUMBER'][i],
        'face_value' : int(df[' FACE VALUE'][i])
        } for i in df.index]
    stmt = insert(Company).values(companies)
    stmt = stmt.on_conflict_do_update(
        constraint="symbol",
        set_={
            "face_value": stmt.excluded.face_value,
            "paid_up_value": stmt.excluded.paid_up_value,
            "market_lot": stmt.excluded.market_lot,
            "isin": stmt.excluded.isin,
            "listing_date": stmt.excluded.listing_date,
            "series": stmt.excluded.series,
            "name": stmt.excluded.name,
        }
    )

    db.execute(stmt)
    db.commit()

# data fetching for bhavcopy
def fetch_bhavcopy_data(days):
    days = min(days, 100)
    for i in tqdm(range(days)):
        response = requests.get(URL_FOR_BHAVCOPY(i), headers=headers)
        if response.status_code != 200:
            continue
        df = pd.read_csv(io.BytesIO(response.content), compression='zip')
        bhavcopies = [{
            'symbol': df['SYMBOL'][i],
            'series' : df['SERIES'][i],
            'open' : df['OPEN'][i],
            'high' : df['HIGH'][i],
            'low' : df['LOW'][i],
            'close' : df['CLOSE'][i],
            'last' : df['LAST'][i],
            'prev_close' : df['PREVCLOSE'][i],
            'total_traded_quantity' : int(df['TOTTRDQTY'][i]),
            'total_traded_value' : int(df['TOTTRDVAL'][i]),
            'total_trades' : int(df['TOTALTRADES'][i]),
            'timestamp' : dt.strptime(df['TIMESTAMP'][i], "%d-%b-%Y"),
            'isin' : df['ISIN'][i],
            } for i in df.index]
        stmt = insert(Bhavcopy).values(bhavcopies)
        stmt = stmt.on_conflict_do_update(
            index_elements=['symbol', 'timestamp'],
            set_={
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "last": stmt.excluded.last,
                "prev_close": stmt.excluded.prev_close,
                "total_traded_quantity": stmt.excluded.total_traded_quantity,
                "total_traded_value": stmt.excluded.total_traded_value,
                "total_trades": stmt.excluded.total_trades,
                "timestamp": stmt.excluded.timestamp,
                "isin": stmt.excluded.isin,
            }
        )
        db.execute(stmt)
    db.commit()


def main():
    print("1. Fetching data for securities available for equity segment")
    fetch_data()
    print("Enter the number of days for which bhavcopy data is to be fetched")
    days = int(input())
    print("2. Fetching data for bhavcopy")
    fetch_bhavcopy_data(days)
    print("Data fetch Complete")