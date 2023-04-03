import os

import dash
import plotly.express as px
import plotly.figure_factory as ff
import pandas as pd
import dash_bootstrap_components as dbc
import numpy as np

from dash import Dash, html, dcc, Output, Input
from dash_bootstrap_templates import load_figure_template
from sklearn.preprocessing import StandardScaler

# Database Connection Setup
################################################################################################
from dotenv import load_dotenv
dotenv = load_dotenv()
CONNECTION_STRING = os.getenv("MYSQL_CONNECTION_STRING")
from database import Base
from load import session_engine_from_connection_string

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP]
)

load_figure_template("LUX")

def extract_table(table_name):

    # open database session
    session, engine = session_engine_from_connection_string(CONNECTION_STRING)
    conn = engine.connect()

    # SQL command to extract table
    sql_query = f"SELECT * FROM {table_name}"
    table = pd.read_sql(sql_query, session.bind)

    # close database session
    session.close()
    conn.close()

    return table

# Read all transformed data files
entity_game = extract_table("game")
entity_genre = extract_table("genre")
entity_parent_platform = extract_table("parent_platform")
entity_platform = extract_table("platform")
entity_publisher = extract_table("publisher")
entity_rating = extract_table("rating")
entity_store = extract_table("store")
entity_tag = extract_table("tag")
rs_game_genre = extract_table("game_genre")
rs_game_platform = extract_table("game_platform")
rs_game_publisher = extract_table("game_publisher")
rs_game_rating = extract_table("game_rating")
rs_game_store = extract_table("game_store")
rs_game_tag = extract_table("game_tag")

df = entity_game.copy()
df = df[df['rating']>0]

# released
df['released'] = pd.to_datetime(df['released'], format='%Y-%m-%d')
df['released_year'] = df['released'].dt.year
df['released_month'] = df['released'].dt.month
df['released_day_of_month'] = df['released'].dt.day
df['released_day_of_week'] = df['released'].dt.dayofweek

# website
df.loc[~df['website'].isna(), 'website'] = 1
df.loc[df['website'].isna(), 'website'] = 0

# reddit_url
df.loc[~df['reddit_url'].isna(), 'reddit_url'] = 1
df.loc[df['reddit_url'].isna(), 'reddit_url'] = 0

# esrb 
df.loc[df['esrb'].isna(), 'esrb'] = 'Rating Pending'

# status
status = ['added_yet', 'added_owned', 'added_beaten', 'added_toplay', 'added_dropped', 'added_playing']
df[status] = df[status].fillna(0)

df.drop(columns=['id', 'slug', 'released', 'updated', 'score', 'achievements_count', 'reddit_name', 'description_raw'], inplace=True)

cont_columns = [
    'playtime', 'rating', 'reviews_text_count', 'added',
    'metacritic', 'suggestions_count', 'reviews_count',
    'screenshots_count', 'movies_count', 'creators_count',
    'parent_achievements_count', 'reddit_count',
    'twitch_count', 'youtube_count', 'parents_count', 'additions_count',
    'game_series_count', 'added_yet', 'added_owned', 'added_beaten', 
    'added_toplay', 'added_dropped', 'added_playing'
]

cat_columns = [
    'website', 'reddit_url', 'esrb', 'released_year', 
    'released_month', 'released_day_of_month', 
    'released_day_of_week'
]

# genre and review
game_genre_table = rs_game_genre.merge(
    entity_genre, how='left', left_on='genre_id', right_on='id'
)[['game_id', 'name']]
game_genre_table = pd.pivot_table(
    game_genre_table, 
    index='game_id', 
    columns='name', 
    values='game_id', 
    aggfunc=len
).reset_index().rename_axis(None, axis=1)

genre_review = rs_game_rating.merge(game_genre_table, how='left', on='game_id')
genre_review['None'] = genre_review.apply(lambda row: 1.0 if row.iloc[3:].isnull().all() else 0.0, axis=1)
genre_review = genre_review.fillna(0)

# platform and review
game_platform_table = rs_game_platform.merge(
    entity_platform, how='left', left_on='platform_id', right_on='id'
)[['game_id', 'parent_platform_id']]
game_platform_table = game_platform_table.merge(
    entity_parent_platform, how='left', left_on='parent_platform_id', right_on='id'
)[['game_id', 'name']]
game_platform_table = pd.pivot_table(
    game_platform_table, 
    index='game_id', 
    columns='name', 
    values='game_id', 
    aggfunc=len
).reset_index().rename_axis(None, axis=1)

platform_review = rs_game_rating.merge(game_platform_table, how='left', on='game_id')
platform_review['None'] = platform_review.apply(lambda row: 1.0 if row.iloc[3:].isnull().all() else 0.0, axis=1)
platform_review = platform_review.fillna(0)

# store and review
game_genre_table = rs_game_store.merge(
    entity_store, how='left', left_on='store_id', right_on='id'
)[['game_id', 'name']]
game_store_table = pd.pivot_table(
    game_genre_table, 
    index='game_id', 
    columns='name', 
    values='game_id', 
    aggfunc=len
).reset_index().rename_axis(None, axis=1)

store_review = rs_game_rating.merge(game_store_table, how='left', on='game_id')
store_review['None'] = store_review.apply(lambda row: 1.0 if row.iloc[3:].isnull().all() else 0.0, axis=1)
store_review = store_review.fillna(0)

# transform data
df_transformed = df.copy()
# scaler = StandardScaler()
# df_transformed[cont_columns] = scaler.fit_transform(df_transformed[cont_columns])
df_transformed[cont_columns] = np.log(df_transformed[cont_columns]+1)

def trend_line():

    temp = df.groupby(['released_year', 'released_month'])['rating'].mean().reset_index()
    temp = temp.sort_values(by=['released_month', 'released_year'])
    fig = px.line(temp, x='released_month', y='rating', color='released_year')
    fig.layout.title.text = f'Average Monthly Rating'

    return fig


app.layout = dbc.Container([
    html.H1('RAWG - Video Game Database'),

    dbc.Container([
        html.H2('Exploratory Data Analysis'),

        dbc.Row([
            dcc.Dropdown(
                df.drop(columns=['name']).columns,
                'rating',
                id='feature',
            )
        ], className='feature_dropdown'),

        dbc.Tabs([
            dbc.Tab(label='Raw Data', tab_id='tab-raw-data'),
            dbc.Tab(label='Transformed Data', tab_id='tab-transformed-data')
        ],
            id='data-tabs',
            active_tab='tab-raw-data'
        ),

        dbc.Row([
            dbc.Col([
                dcc.Graph(id='eda-histogram')
            ], className='left_grid'),
            dbc.Col([
                dcc.Graph(id='eda-boxplot'),
                dcc.Graph(id='eda-scatterplot')
            ], className='right_grid'),
        ]),

        html.H3('Correlation Matrix of Features'),

        dbc.Row([
            dcc.Graph(
                id='eda-corrmatrix'
            )
        ], className='heatmap')
    ], className='section_container'),

    dbc.Container([
        html.H2('Rating Analysis'),

        dbc.Row([
    
            dbc.Col([
                dbc.Row([
                    dcc.Dropdown(
                        genre_review.columns[3:],
                        id='genre',
                    )
                ], className='feature_dropdown_small'),
                dbc.Row([
                    dcc.Graph(id='ta-genre-review')
                ])
            ]),

            dbc.Col([
                dbc.Row([
                    dcc.Dropdown(
                        platform_review.columns[3:],
                        id='platform',
                    )
                ], className='feature_dropdown_small'),
                dbc.Row([
                    dcc.Graph(id='ta-platform-review')
                ])
            ]),

            dbc.Col([
                dbc.Row([
                    dcc.Dropdown(
                        store_review.columns[3:],
                        id='store',
                    )                    
                ], className='feature_dropdown_small'),
                dbc.Row([
                    dcc.Graph(id='ta-store-review')
                ])
            ])
        ]),

        dbc.Row([
            dcc.Graph(
                id='ta-time-trend', 
                figure=trend_line()
            )
        ])
    ], className='section_container')

], className='main_container')

@app.callback(
    Output('eda-histogram', 'figure'),
    Input('data-tabs', 'active_tab'),
    Input('feature', 'value')
)
def update_histogram(tab, feature):

    if tab == 'tab-raw-data':
        fig = px.histogram(df, x=feature)
        fig.layout.title.text = f'Histogram of {feature}'


    elif tab == 'tab-transformed-data':
        fig = px.histogram(df_transformed, x=feature)
        fig.layout.title.text = f'Histogram of {feature}'
    
    return fig

@app.callback(
    Output('eda-boxplot', 'figure'),
    Input('data-tabs', 'active_tab'),
    Input('feature', 'value')
)
def update_boxplot(tab, feature):

    if tab == 'tab-raw-data':
        fig = px.box(df, x=feature)
        fig.layout.title.text = f'Boxplot of {feature}'

    elif tab == 'tab-transformed-data':
        fig = px.box(df_transformed, x=feature)
        fig.layout.title.text = f'Boxplot of {feature}'

    return fig

@app.callback(
    Output('eda-scatterplot', 'figure'),
    Input('data-tabs', 'active_tab'),
    Input('feature', 'value')
)
def update_scatterplot(tab, feature):

    if tab == 'tab-raw-data':
        fig = px.scatter(df, x=feature, y='rating')
        fig.layout.title.text = f'Scatterplot of rating against {feature}'

    elif tab == 'tab-transformed-data':
        fig = px.scatter(df_transformed, x=feature, y='rating')
        fig.layout.title.text = f'Scatterplot of rating against {feature}'

    return fig


@app.callback(
    Output('eda-corrmatrix', 'figure'),
    Input('data-tabs', 'active_tab')
)
def update_corrmatrix(tab):

    if tab == 'tab-raw-data':
        corr_matrix = df.corr(numeric_only=True)
    elif tab == 'tab-transformed-data':
        corr_matrix = df_transformed.corr(numeric_only=True)

    mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
    corr_matrix = corr_matrix.mask(mask)

    x = list(corr_matrix.columns)
    y = list(corr_matrix.index)
    z = np.array(corr_matrix)

    fig = ff.create_annotated_heatmap(
        z,
        x=x,
        y=y,
        annotation_text = np.around(z, decimals=2),
        hoverinfo='z', 
        colorscale='RdBu'
    )

    return fig

@app.callback(
    Output('ta-genre-review', 'figure'),
    Input('genre', 'value')
)
def update_genre_review(genre):

    if genre is None:
        fig = px.histogram(genre_review, x='rating_id', color='rating_id')
        fig.layout.title.text = f'Spread of Ratings across All Genres'
    else:
        fig = px.histogram(genre_review.loc[genre_review[genre]==1], x='rating_id', color='rating_id')
        fig.layout.title.text = f'Spread of Ratings across Genre: {genre}'
    fig.update_xaxes(type='category', categoryorder='category ascending')

    return fig

@app.callback(
    Output('ta-platform-review', 'figure'),
    Input('platform', 'value')
)
def update_platform_review(platform):

    if platform is None:
        fig = px.histogram(platform_review, x='rating_id', color='rating_id')
        fig.layout.title.text = f'Spread of Ratings across All Platforms'
    else:
        fig = px.histogram(platform_review.loc[platform_review[platform]==1], x='rating_id', color='rating_id')
        fig.layout.title.text = f'Spread of Ratings across Platform: {platform}'
    fig.update_xaxes(type='category', categoryorder='category ascending')

    return fig

@app.callback(
    Output('ta-store-review', 'figure'),
    Input('store', 'value')
)
def update_store_review(store):

    if store is None:
        fig = px.histogram(store_review, x='rating_id', color='rating_id')
        fig.layout.title.text = f'Spread of Ratings across All Stores'
    else:
        fig = px.histogram(store_review.loc[store_review[store]==1], x='rating_id', color='rating_id')
        fig.layout.title.text = f'Spread of Ratings across Store: {store}'
    fig.update_xaxes(type='category', categoryorder='category ascending')

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
