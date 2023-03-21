
# ------------------------- Transform Functions for ETL -------------------------

# Import Libraries and Utility Functions
import os 
import pandas as pd


data_directory = os.path.join(os.getcwd(), "raw_data")
data_upload_directory = os.path.join(os.getcwd(), "transformed_data")


# Transformation Functions Used to Normalize JSON Output of Game API
################################################################################################

# Utility Function
def create_dictionary_from_df(df: pd.DataFrame(), key: str, value: str) -> dict():
	"""
	Helper Function to convert 2 columns in a DataFrame into a Dictionary
	"""
	df = df[[key, value]].drop_duplicates()
	return df.set_index([key])[value].to_dict()


def transform_game_list_api(resp_json):
	"""
	Function to transform the json response of RAWG's game (list) API
	Returns a dictionary containing transformed DataFrames:

	Output Keys
	----------
	game_data:
		Data that are associated to each game

	platforms:
		Platform that supports each game
	
	stores:
		Stores that sell each game

	detailed_ratings:
		Breakdown of ratings given to a game

	status:
		Breakdown of "played" status given to a game by users

	tags:
		Tags associated to a game

	esrb_rating:
		ESRB rating assigned to a game

	parent_platform:
		Parent Platform of game

	genres:
		Genre associated to a game


	Parameters
	----------
	resp_json: json
		JSON object

	"""
	df = pd.DataFrame(resp_json)

	output = {}
	col = ['platforms',
		'stores',
		'ratings',
		'added_by_status',
		'tags',
		'esrb_rating',
		'parent_platforms',
		'genres',
		'short_screenshots'
	]
	
	# Game Details
	# ======================================================
	df_data = df.drop(col, axis=1)
	output["game_data"] = df_data

	df_platforms = pd.DataFrame()
	df_stores = pd.DataFrame()
	df_ratings = pd.DataFrame()
	df_added_by_status = pd.DataFrame()
	df_tags = pd.DataFrame()
	df_esrb_ratings  = pd.DataFrame()
	df_parent_platforms  = pd.DataFrame()
	df_genres = pd.DataFrame()

	for idx, row in df.iterrows():
		# platform
		# ======================================================
		if row["platforms"] != None:
			for platform in row["platforms"]:
				df_platforms = pd.concat([df_platforms, pd.DataFrame({"game_id": [row["id"]], "platform_id": [platform["platform"]["id"]]})])

		# stores
		# ======================================================
		if row["stores"] != None:
			for store in row["stores"]:
				df_stores = pd.concat([df_stores, pd.DataFrame({"game_id": [row["id"]], "store_id": [store["store"]["id"]]})])

		# ratings
		# ======================================================
		if row["ratings"] != None:
			df_curr_rating = pd.DataFrame(row["ratings"])
			df_curr_rating.dropna(inplace=True)
			df_curr_rating["game_id"] = row["id"]
			df_ratings = pd.concat([df_ratings, df_curr_rating])

		# addedd by status
		# ======================================================
		if row["added_by_status"] != None:
			df_curr_added_by_status = pd.DataFrame(row["added_by_status"], index=[0])
			df_curr_added_by_status.dropna(inplace=True)
			df_curr_added_by_status["game_id"] = row["id"]
			df_added_by_status = pd.concat([df_added_by_status, df_curr_added_by_status])
		
		# tags
		# ======================================================
		if row["tags"] != None:
			for tag in row["tags"]:
				df_tags = pd.concat([df_tags, pd.DataFrame({"game_id": [row["id"]], "tag_id": [tag["id"]]})])

		# esrb ratings
		# ======================================================
		if row["esrb_rating"] != None:
			df_curr_esrb = pd.DataFrame(row["esrb_rating"], index=[0])
			df_curr_esrb.rename(columns={"id": "esrb_id"}, inplace=True)
			df_curr_esrb.dropna(inplace=True)
			df_curr_esrb["game_id"] = row["id"]
			df_esrb_ratings = pd.concat([df_esrb_ratings, df_curr_esrb])
		
		# parent platforms
		# ======================================================
		if row["parent_platforms"] != None and type(row["parent_platforms"]) != float:
			for platform in row["parent_platforms"]:
				df_parent_platforms = pd.concat([df_parent_platforms, pd.DataFrame({"game_id": [row["id"]], "platform": \
																					[platform["platform"]["id"]]})])

		# genres
		# ======================================================
		if row["genres"] != None:
			df_curr_genre = pd.DataFrame(row["genres"])
			df_curr_genre.rename(columns={"id": "genre_id"}, inplace=True)
			df_curr_genre.dropna(inplace=True)
			df_curr_genre["game_id"] = row["id"]
			df_genres = pd.concat([df_genres, df_curr_genre])

		output["platforms"] = df_platforms
		output["stores"] = df_stores
		output["detailed_ratings"] = df_ratings
		output["status"] = df_added_by_status
		output["tags"] = df_tags
		output["esrb_rating"] = df_esrb_ratings
		output["parent_platform"] = df_parent_platforms
		output["genres"] = df_genres

		return output


def transform_game_detail_api(resp_json):
		"""
		Function to transform the json response of RAWG's game (detail) API
		Returns a dictionary containing transformed DataFrames (those data returned in transform_game_list are excluded)

		Output Keys
		----------
		game_details_data:
			Detailed Data that are associated to each game

		metacritic_platforms:
			Metacritic Scores across platforms that supports each game
		
		reactions:
			User reactions to a game

		developers:
			Developers of a game

		publishers:
			Publishers of a game

		Parameters
		----------
		resp_json: json
			JSON object

		"""
		output = {}
		lst_or_dct_fields = [ 
			"metacritic_platforms",
			"ratings",
			"reactions",
			"added_by_status",
			"parent_platforms",
			"platforms",
			"stores",
			"developers",
			"genres",
			"tags",
			"publishers",
			"esrb_rating",
			"alternative_names" # ignore
		]

		# Game Details Data
		# ======================================================
		df = pd.DataFrame({k:v for k,v in resp_json.items() if k not in lst_or_dct_fields}, index=[0])
		output["game_details_data"] = df

		# clean metacritic platforms
		# ======================================================
		if resp_json["metacritic_platforms"] != None:
			df_metacritic = pd.DataFrame(resp_json["metacritic_platforms"])
			if len(df_metacritic) > 0:
				df_metacritic["platform_id"] = df_metacritic["platform"].apply(lambda x: x["platform"])
				df_metacritic["game_id"] = resp_json["id"]
			output["metacritic_platforms"] = df_metacritic

		# reactions
		# ======================================================
		if resp_json[ "reactions"] != None:
			df_reactions = pd.DataFrame(resp_json["reactions"], index=["count"]).T.reset_index(drop=False).rename(columns={"index": "reaction_id"})
			df_reactions["game_id"] = resp_json["id"]
			output["reactions"] = df_reactions


		# developers
		# ======================================================
		if resp_json[ "developers"] != None:
			df_developers = pd.DataFrame({"developer_id": [record["id"] \
											 for record in resp_json["developers"]]})
			df_developers["game_id"] = resp_json["id"]
			output["developers"] = df_developers
		
		# publishers
		# ======================================================
		if resp_json[ "publishers"] != None:
			df_publishers = pd.DataFrame({"publisher_id": [record["id"] \
											 for record in resp_json["publishers"]]})
			df_publishers["game_id"] = resp_json["id"]
			output["publishers"] = df_publishers

		return output



# Python Functions of Python Operators
################################################################################################

def transform_entity_game(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")
	
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
	# ---------------------------------------------------------------------------------------------[TO DOCUMENT]
	try:
		df_game_esrb = pd.read_csv(os.path.join(data_directory, "game_esrb.csv"))
	except pd.errors.EmptyDataError:
		df_game_esrb = pd.DataFrame(columns=["esrb_id", "name", "slug", "name_en", "name_ru", "game_id"])

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


def transform_entity_parent_platform(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")

	df_parent_platform = pd.read_csv(os.path.join(data_directory, "parent_platform_data.csv"))
	df_parent_platform.to_csv(os.path.join(data_upload_directory, "entity_parent_platform.csv"), index=False)


def transform_entity_platform(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")
	
	df_platform = pd.read_csv(os.path.join(data_directory, "platform_data.csv"))
	df_platform = df_platform[["id", "name", "slug"]].copy()

	# Add Parent Platform FK in
	df_parent_platform_platform = pd.read_csv(os.path.join(data_directory, "parent_platform_platform.csv"))
	df_platform_output = df_platform.merge(df_parent_platform_platform[["platform_id", "parent_platform_id"]].rename(columns={"platform_id": "id"}), how="left", on="id")

	df_platform_output.to_csv(os.path.join(data_upload_directory, "entity_platform.csv"), index=False)


def transform_entity_publisher(**kwargs):
	ti = kwargs["ti"]
	# not from extract_game_publisher because publisher is a dependent of game_list
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")
	
	df_publisher = pd.read_csv(os.path.join(data_directory, "publisher_data.csv"))
	
	# Exclude these columns first
	df_publisher.drop(columns=["games_count", "image_background"], inplace=True)
	
	df_publisher.to_csv(os.path.join(data_upload_directory, "entity_publisher.csv"), index=False)


def transform_entity_tag(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")

	df_tag = pd.read_csv(os.path.join(data_directory, "tag_data.csv"))

	# ---------------------------------------------------------------------------------------------[TO DOCUMENT]
	# tag/list API return duplicates
	df_tag.drop_duplicates(subset=["id"], inplace=True)

	df_tag.to_csv(os.path.join(data_upload_directory, "entity_tag.csv"), index=False)


def transform_entity_genre(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")

	df_genre = pd.read_csv(os.path.join(data_directory, "genre_data.csv"))
	df_genre.to_csv(os.path.join(data_upload_directory, "entity_genre.csv"), index=False)


def transform_entity_store(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")

	df_store = pd.read_csv(os.path.join(data_directory, "store_data.csv"))
	df_store.to_csv(os.path.join(data_upload_directory, "entity_store.csv"), index=False)


def transform_entity_rating(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")

	df_game_ratings = pd.read_csv(os.path.join(data_directory, "game_rating.csv"))

	# ---------------------------------------------------------------------------------------------[TO DOCUMENT]
	if len(df_game_ratings) == 0:
		df_game_ratings = pd.DataFrame(columns=["id", "title", "count", "percent", "game_id"])
	
	df_ratings = df_game_ratings[["id", "title"]].drop_duplicates()
	df_ratings.to_csv(os.path.join(data_upload_directory, "entity_rating.csv"), index=False)


def transform_rs_game_platform(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")

	df_game_platforms = pd.read_csv(os.path.join(data_directory, "game_platform.csv"))
	# ---------------------------------------------------------------------------------------------[TO DOCUMENT]
	try:
		df_game_metacritic = pd.read_csv(os.path.join(data_directory, "game_details_metacritic.csv"))
	except pd.errors.EmptyDataError:
		df_game_metacritic = pd.DataFrame(columns=["metascore", "url", "platform", "platform_id", "game_id"])
	
	# Add metacritic score info into this relationship table
	df_game_platform = pd.merge(df_game_platforms, df_game_metacritic[["metascore", "url", "platform_id", "game_id"]], how="left", on=["platform_id", "game_id"])
	df_game_platform.rename(columns={"url": "metacritic_url"}, inplace=True)
	df_game_platform.drop_duplicates(inplace=True)

	df_game_platform.to_csv(os.path.join(data_upload_directory, "rs_game_platform.csv"), index=False)


def transform_rs_game_genre(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")

	df_game_genre = pd.read_csv(os.path.join(data_directory, "game_genre.csv"))
	df_game_genre["genre_id"] = df_game_genre["genre_id"].astype(int)
	df_game_genre_output = df_game_genre[["genre_id", "game_id"]]
	df_game_genre_output.to_csv(os.path.join(data_upload_directory, "rs_game_genre.csv"), index=False)


def transform_rs_game_store(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")

	df_game_store = pd.read_csv(os.path.join(data_directory, "game_store.csv"))
	df_game_store.to_csv(os.path.join(data_upload_directory, "rs_game_store.csv"), index=False)


def transform_rs_game_rating(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")

	df_game_ratings = pd.read_csv(os.path.join(data_directory, "game_rating.csv"))
	# ---------------------------------------------------------------------------------------------[TO DOCUMENT]
	if len(df_game_ratings) == 0:
		df_game_ratings = pd.DataFrame(columns=["id", "title", "count", "percent", "game_id"])

	df_game_ratings = df_game_ratings[["id", "count", "game_id"]]
	df_game_rating_output = df_game_ratings.rename(columns={"id": "rating_id"})
	df_game_rating_output["rating_id"] = df_game_rating_output["rating_id"].astype(int)
	df_game_rating_output.to_csv(os.path.join(data_upload_directory, "rs_game_rating.csv"), index=False)


def transform_rs_game_tag(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")

	df_game_tag = pd.read_csv(os.path.join(data_directory, "game_tag.csv"))
	df_game_tag.to_csv(os.path.join(data_upload_directory, "rs_game_tag.csv"), index=False)


def transform_rs_game_publisher(**kwargs):
	ti = kwargs["ti"]
	root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
	data_directory = os.path.join(root_data_directory, "raw_data")
	data_upload_directory = os.path.join(root_data_directory, "transformed_data")

	df_game_publisher = pd.read_csv(os.path.join(data_directory, "game_details_publisher.csv"))
	df_game_publisher["publisher_id"] = df_game_publisher["publisher_id"].astype(int)
	df_game_publisher.drop_duplicates(inplace=True)
	df_game_publisher.to_csv(os.path.join(data_upload_directory, "rs_game_publisher.csv"), index=False)