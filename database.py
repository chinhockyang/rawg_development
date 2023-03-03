
# Tentative File to store Tables (SQLAlchemy Style) necessary for Database Creation (Through Python)
# [KIV]: Usage of Raw SQL Commands through Airflow BashOperators
# ======================================================

from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

# Create a base for the models to build upon.
Base = declarative_base()

################################################# ENTITIES

class Game(Base):
    __tablename__ = "game"

    id  = Column(Integer, primary_key=True)
    slug  = Column(String(50))
    name  = Column(String(100))
    playtime  = Column(Integer)
    released  = Column(DateTime)
    rating  = Column(Float)
    reviews_text_count  = Column(Integer)
    added  = Column(Integer)
    metacritic  = Column(Float)
    suggestions_count  = Column(Integer)
    updated  = Column(DateTime) # DATETIME
    score  = Column(Float)
    reviews_count  = Column(Integer)
    website  = Column(String(500))
    screenshots_count  = Column(Integer)
    movies_count  = Column(Integer)
    creators_count  = Column(Integer)
    achievements_count  = Column(Integer)
    parent_achievements_count  = Column(Integer)
    reddit_url  = Column(String(500))
    reddit_name  = Column(String(100))
    reddit_count  = Column(Integer)
    twitch_count  = Column(Integer)
    youtube_count  = Column(Integer)
    parents_count  = Column(Integer)
    additions_count  = Column(Integer)
    game_series_count  = Column(Integer)
    description_raw  = Column(String(10000))
    yet  = Column(Integer)
    owned  = Column(Integer)
    beaten  = Column(Integer)
    toplay  = Column(Integer)
    dropped  = Column(Integer)
    playing  = Column(Integer)
    esrb  = Column(String(25))