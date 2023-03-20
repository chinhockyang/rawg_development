import os 
import pandas as pd


data_directory = os.path.join(os.getcwd(), "raw_data")
data_upload_directory = os.path.join(os.getcwd(), "transformed_data")


def transform_entity_game():
	# Read CSV files containing raw data from the directory "raw_data"
	df_game = pd.read_csv(os.path.join(data_directory, "game_data.csv"))
	df_game_details_data = pd.read_csv(os.path.join(data_directory, "game_details_data.csv"), low_memory=False)

	# Remove columns in df_game from df_game_details_data
	df_game_details_data_subset = df_game_details_data[[col for col in df_game_details_data.columns if col not in df_game.columns.tolist()] + ["id"]].copy()
	df_game_output = df_game.merge(df_game_details_data_subset, on=["id"], how="left")
	df_game_output.drop_duplicates(subset=["id"], inplace=True)

	# Add GAME STATUS data into game table
	df_status = pd.read_csv(os.path.join(data_directory, "game_status.csv"))
	df_game_output = df_game_output.merge(df_status.rename(columns={"game_id": "id"}), on=["id"], how="left")
	
	# Add ESRB data into game table
	df_game_esrb = pd.read_csv(os.path.join(data_directory, "game_esrb.csv"))
	df_game_output = df_game_output.merge(df_game_esrb[["name", "game_id"]].rename(columns={"name": "esrb", "game_id": "id"}), how="left", on="id")

	# Remove these columns
	col_to_delete = [
	    "tba",
	    "background_image",
	    "rating_top",
	    "ratings_count",
	    "clip",
	    "user_game",
	    "saturated_color",
	    "dominant_color",
	    "community_rating",
	    "name_original",
	    "description",
	    "background_image_additional",
	    "reddit_logo",
	    "reddit_description",
	    "metacritic_url"
	]
	df_game_output.drop(columns=col_to_delete, inplace=True)

	# Export transformed data to CSV file, saved in the directory "transformed_data"
	df_game_output.to_csv(os.path.join(data_upload_directory, "entity_game.csv"), index=False)


def transform_entity_parent_platform():
	df_parent_platform = pd.read_csv(os.path.join(data_directory, "parent_platform_data.csv"))
	df_parent_platform.to_csv(os.path.join(data_upload_directory, "entity_parent_platform.csv"), index=False)


def transform_entity_platform():
	df_platform = pd.read_csv(os.path.join(data_directory, "platform_data.csv"))
	df_platform = df_platform[["id", "name", "slug"]].copy()

	# Add Parent Platform FK in
	df_parent_platform_platform = pd.read_csv(os.path.join(data_directory, "parent_platform_platform.csv"))
	df_platform_output = df_platform.merge(df_parent_platform_platform[["platform_id", "parent_platform_id"]].rename(columns={"platform_id": "id"}), how="left", on="id")

	df_platform_output.to_csv(os.path.join(data_upload_directory, "entity_platform.csv"), index=False)


def transform_entity_publisher():
	df_publisher = pd.read_csv(os.path.join(data_directory, "publisher_data.csv"))
	
	# Exclude these columns first
	df_publisher.drop(columns=["games_count", "image_background"], inplace=True)
	
	df_publisher.to_csv(os.path.join(data_upload_directory, "entity_publisher.csv"), index=False)


def transform_entity_tag():
	df_tag = pd.read_csv(os.path.join(data_directory, "tag_data.csv"))
	
	# tag/list API return duplicates
	df_tag.drop_duplicates(subset=["id"], inplace=True)

	df_tag.to_csv(os.path.join(data_upload_directory, "entity_tag.csv"), index=False)


def transform_entity_genre():
	df_genre = pd.read_csv(os.path.join(data_directory, "genre_data.csv"))
	df_genre.to_csv(os.path.join(data_upload_directory, "entity_genre.csv"), index=False)


def transform_entity_store():
	df_store = pd.read_csv(os.path.join(data_directory, "store_data.csv"))
	df_store.to_csv(os.path.join(data_upload_directory, "entity_store.csv"), index=False)


def transform_entity_rating():
	df_game_ratings = pd.read_csv(os.path.join(data_directory, "game_rating.csv"))
	df_ratings = df_game_ratings[["id", "title"]].drop_duplicates()
	df_ratings.to_csv(os.path.join(data_upload_directory, "entity_rating.csv"), index=False)


def transform_rs_game_platform():
	df_game_platforms = pd.read_csv(os.path.join(data_directory, "game_platform.csv"))
	df_game_metacritic = pd.read_csv(os.path.join(data_directory, "game_details_metacritic.csv"))
	
	# Add metacritic score info into this relationship table
	df_game_platform = pd.merge(df_game_platforms, df_game_metacritic[["metascore", "url", "platform_id", "game_id"]], how="left", on=["platform_id", "game_id"])
	df_game_platform.rename(columns={"url": "metacritic_url"}, inplace=True)
	df_game_platform.drop_duplicates(inplace=True)

	df_game_platform.to_csv(os.path.join(data_upload_directory, "rs_game_platform.csv"), index=False)


def transform_rs_game_genre():
	df_game_genre = pd.read_csv(os.path.join(data_directory, "game_genre.csv"))
	df_game_genre["genre_id"] = df_game_genre["genre_id"].astype(int)
	df_game_genre_output = df_game_genre[["genre_id", "game_id"]]
	df_game_genre_output.to_csv(os.path.join(data_upload_directory, "rs_game_genre.csv"), index=False)


def transform_rs_game_store():
	df_game_store = pd.read_csv(os.path.join(data_directory, "game_store.csv"))
	df_game_store.to_csv(os.path.join(data_upload_directory, "rs_game_store.csv"), index=False)


def transform_rs_game_rating():
	df_game_ratings = pd.read_csv(os.path.join(data_directory, "game_rating.csv"))
	df_game_ratings = df_game_ratings[["id", "count", "game_id"]]
	df_game_rating_output = df_game_ratings.rename(columns={"id": "rating_id"})
	df_game_rating_output["rating_id"] = df_game_rating_output["rating_id"].astype(int)
	df_game_rating_output.to_csv(os.path.join(data_upload_directory, "rs_game_rating.csv"), index=False)


def transform_rs_game_tag():
	df_game_tag = pd.read_csv(os.path.join(data_directory, "game_tag.csv"))
	df_game_tag.to_csv(os.path.join(data_upload_directory, "rs_game_tag.csv"), index=False)


def transform_rs_game_publisher():
	df_game_publisher = pd.read_csv(os.path.join(data_directory, "game_details_publisher.csv"))
	df_game_publisher["publisher_id"] = df_game_publisher["publisher_id"].astype(int)
	df_game_publisher.drop_duplicates(inplace=True)
	df_game_publisher.to_csv(os.path.join(data_upload_directory, "rs_game_publisher.csv"), index=False)