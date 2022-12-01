import sqlalchemy
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Float, Date
from sqlalchemy.orm import relationship
from database import Base


class Company(Base):

    __tablename__ = "company"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    symbol = Column(String, unique=True)
    series = Column(String)
    listing_date = Column(Date)
    paid_up_value = Column(Integer)
    market_lot = Column(Integer)
    isin = Column(String, index=True)
    face_value = Column(Integer)


class Bhavcopy(Base):

    __tablename__ = "bhavcopy"
    __table_args__ = (
        sqlalchemy.UniqueConstraint('symbol', 'timestamp', name='unique_for_day'),
    )

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String)
    series = Column(String)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    last = Column(Float)
    prev_close = Column(Float)
    total_traded_quantity = Column(Integer)
    total_traded_value = Column(Integer)
    total_trades = Column(Integer)
    timestamp = Column(Date, index=True)
    isin = Column(String, index=True)