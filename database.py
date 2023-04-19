
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base

# Create a base for the models to build upon.
Base = declarative_base()


# Data Warehouse
# =====================================================================

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

################################################# RELATIONSHIP

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

class GameRating(Base):
    __tablename__ = "game_rating"
    rating_id = Column(Integer, ForeignKey("rating.id"), primary_key=True)
    game_id = Column(Integer, ForeignKey("game.id"), primary_key=True)
    count = Column(Integer)
    
    
# Data Store
# =====================================================================

class ClassificationData(Base):
    __tablename__ = "classification_data"
    
    id = Column(Integer, primary_key=True)
    
    # statistics and basicinfo of game
    playtime = Column(Integer)
    rating = Column(Integer)
    reviews_text_count = Column(Integer)
    added_count = Column(Integer)
    suggestions_count = Column(Integer)
    website = Column(Integer)
    screenshots_count = Column(Integer)
    movies = Column(Integer)
    achievements_count = Column(Integer)
    reddit_url = Column(Integer)
    reddit_count = Column(Integer)
    twitch_count = Column(Integer)
    youtube_count = Column(Integer)
    parents_count = Column(Integer)
    additions_count = Column(Integer)
    game_series_count = Column(Integer)
    tba = Column(Integer)
    description = Column(Integer)
    
    # release date features
    day_of_week = Column(Integer)
    day_of_month = Column(Integer)
    month = Column(Integer)
    year = Column(Integer)
    
    # platform features
    platform_android = Column(Integer)
    platform_ios = Column(Integer)
    platform_linux = Column(Integer)
    platform_macos = Column(Integer)
    platform_nintendo_3ds = Column(Integer)
    platform_nintendo_64 = Column(Integer)
    platform_nintendo_switch = Column(Integer)
    platform_pc = Column(Integer)
    platform_playstation4 = Column(Integer)
    platform_playstation5 = Column(Integer)
    platform_ps_vita = Column(Integer)
    platform_xbox_one = Column(Integer)
    platform_xbox_series_x = Column(Integer)
    platforms_count = Column(Integer)
    
    # store features
    store_apple_appstore = Column(Integer)
    store_epic_games = Column(Integer)
    store_gog = Column(Integer)
    store_google_play = Column(Integer)
    store_nintendo = Column(Integer)
    store_playstation_store = Column(Integer)
    store_steam = Column(Integer)
    store_xbox_store = Column(Integer)
    stores_count = Column(Integer)
    
    # genre features
    genre_action = Column(Integer)
    genre_adventure = Column(Integer)
    genre_arcade = Column(Integer)
    genre_board_games = Column(Integer)
    genre_card = Column(Integer)
    genre_casual = Column(Integer)
    genre_educational = Column(Integer)
    genre_family = Column(Integer)
    genre_fighting = Column(Integer)
    genre_indie = Column(Integer)
    genre_massively_multiplayer = Column(Integer)
    genre_platformer = Column(Integer)
    genre_puzzle = Column(Integer)
    genre_racing = Column(Integer)
    genre_role_playing_games_rpg = Column(Integer)
    genre_shooter = Column(Integer)
    genre_simulation = Column(Integer)
    genre_sports = Column(Integer)
    genre_strategy = Column(Integer)
    genres_count = Column(Integer)
