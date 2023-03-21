
# Tentative File to store Tables (SQLAlchemy Style) necessary for Database Creation (Through Python)
# [KIV]: Usage of Raw SQL Commands through Airflow BashOperators
# ======================================================

from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base

# Create a base for the models to build upon.
Base = declarative_base()

################################################# ENTITIES

class Game(Base):
    __tablename__ = "game"

    id  = Column(Integer, primary_key=True)
    slug  = Column(String(50))
    name  = Column(String(200))
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
    reddit_name  = Column(String(300))
    reddit_count  = Column(Integer)
    twitch_count  = Column(Integer)
    youtube_count  = Column(Integer)
    parents_count  = Column(Integer)
    additions_count  = Column(Integer)
    game_series_count  = Column(Integer)
    description_raw  = Column(Text(50000))
    added_yet  = Column(Integer)
    added_owned  = Column(Integer)
    added_beaten  = Column(Integer)
    added_toplay  = Column(Integer)
    added_dropped  = Column(Integer)
    added_playing  = Column(Integer)
    esrb  = Column(String(25))
    name_original = Column(String(200))
    tba = Column(Boolean)
    rating_top = Column(Integer)
    description = Column(Text(50000))
    background_image = Column(String(500))
    background_image_additional = Column(String(500))
    reddit_description = Column(Text)
    reddit_logo = Column(String(500))

class Publisher(Base):
    __tablename__ = "publisher"
    id = Column(Integer, primary_key=True)
    slug = Column(String(50))
    name = Column(String(100))
    description = Column(String(10000))

class Genre(Base):
    __tablename__ = "genre"
    id = Column(Integer, primary_key=True)
    slug = Column(String(50))
    name = Column(String(100))

class Tag(Base):
    __tablename__ = "tag"
    id = Column(Integer, primary_key=True)
    slug = Column(String(50))
    name = Column(String(100))

class Store(Base):
    __tablename__ = "store"
    id = Column(Integer, primary_key=True)
    slug = Column(String(50))
    name = Column(String(100))
    domain = Column(String(100))

class Platform(Base):
    __tablename__ = "platform"
    id = Column(Integer, primary_key=True)
    slug = Column(String(50))
    name = Column(String(100))
    parent_platform_id=Column(Integer, ForeignKey("parent_platform.id"))

class ParentPlatform(Base):
    __tablename__ = "parent_platform"
    id = Column(Integer, primary_key=True)
    slug = Column(String(50))
    name = Column(String(100))

class Rating(Base):
    __tablename__ = "rating"
    id = Column(Integer, primary_key=True)
    title = Column(String(50))

class GamePublisher(Base):
    __tablename__ = "game_publisher"
    game_id = Column(Integer, ForeignKey("game.id"), primary_key=True)
    publisher_id = Column(Integer, ForeignKey("publisher.id"), primary_key=True)

class GameGenre(Base):
    __tablename__ = "game_genre"
    game_id  = Column(Integer, ForeignKey("game.id"), primary_key=True)
    genre_id = Column(Integer, ForeignKey("genre.id"), primary_key=True)

class GameTag(Base):
    __tablename__ = "game_tag"
    game_id = Column(Integer, ForeignKey("game.id"), primary_key=True)
    tag_id = Column(Integer, ForeignKey("tag.id"), primary_key=True)

class GameStore(Base):
    __tablename__ = "game_store"
    game_id = Column(Integer, ForeignKey("game.id"), primary_key=True)
    store_id = Column(Integer, ForeignKey("store.id"), primary_key=True)

class GamePlatform(Base):
    __tablename__ = "game_platform"
    game_id = Column(Integer, ForeignKey("game.id"), primary_key=True)
    platform_id = Column(Integer, ForeignKey("platform.id"), primary_key=True)
    metascore = Column(Float)
    metacritic_url = Column(String(500))

class Platform_ParentPlatform(Base):
    __tablename__ = "platform_parentplatform"
    platform_id = Column(Integer, ForeignKey("platform.id"), primary_key=True)
    parent_platform_id = Column(Integer, ForeignKey("parent_platform.id"), primary_key=True)

class GameRating(Base):
    __tablename__ = "game_rating"
    rating_id = Column(Integer, ForeignKey("rating.id"), primary_key=True)
    game_id = Column(Integer, ForeignKey("game.id"), primary_key=True)
    count = Column(Integer)