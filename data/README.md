# Data Directory of RAWG Data Analysis Project

This is the data directory that serve the main project (see the [README](https://github.com/chinhockyang/rawg_development/blob/finalise/README.md) page in the root project directory).

During initial setup, all nested folders in this directory will contain no data files, but only a ```.gitkeep``` file. Remove the ```.gitkeep``` files after the data directory is pulled into your local machine.

The full data used in development can be found in this [link](https://drive.google.com/drive/folders/1jl6hQb7kpFjnvImgaRJcCfR5Pvzd1Fby?usp=sharing). It contain data of games released between Jan 2018 to Jan 2023 (```initial_upload```), as well as data retrieved for the month of Feb 2023 (```monthly_new_games``` and ```monthly_updates```).


| Folder | Data Stored |
| :------------- |:-------------|
| ```initial_upload``` | Data that are buik uploaded into the Database on initial setup |
| ```monthly_new_games``` | Data of new games that are extracted in the past month |
| ```monthly_updates``` | Data of existing games in the Database, but have information updates in the past month |
| ```ml_data``` | Transformed data ready for use for Classification model developmen
