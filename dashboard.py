import dash
import dash_bootstrap_components as dbc
import math
import numpy as np
import os
import pandas as pd
import pickle
import plotly.express as px
import plotly.figure_factory as ff
import plotly.graph_objects as go

from dash import Dash, html, dcc, Output, Input
from dash_bootstrap_templates import load_figure_template
from dotenv import load_dotenv
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix

# Database Connection Setup
################################################################################################
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
################################################################################################
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

# Read machine learning model
################################################################################################
standard_scaler_file = open('./model/standard_scaler.pkl', 'rb')
standard_scaler_model = pickle.load(standard_scaler_file)
standard_scaler_file.close()

classification_model_file = open('./model/classification_model.pkl', 'rb')
classification_model = pickle.load(classification_model_file)
classification_model_file.close()

# Read train test data
################################################################################################
x_train = pd.read_csv('./model/x_train.csv')
x_test = pd.read_csv('./model/x_test.csv')
y_train = pd.read_csv('./model/y_train.csv')
y_test = pd.read_csv('./model/y_test.csv')

# Data transformation for exploratory data analysis
################################################################################################
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

genre_review = genre_review.merge(entity_rating, how='left', left_on='rating_id', right_on='id')
genre_review.drop(columns=['rating_id', 'id'], inplace=True)

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

platform_review = platform_review.merge(entity_rating, how='left', left_on='rating_id', right_on='id')
platform_review.drop(columns=['rating_id', 'id'], inplace=True)

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

store_review = store_review.merge(entity_rating, how='left', left_on='rating_id', right_on='id')
store_review.drop(columns=['rating_id', 'id'], inplace=True)

# transform data
df_transformed = df.copy()
scaler = StandardScaler()
df_transformed[cont_columns] = scaler.fit_transform(df_transformed[cont_columns])

# Prediction from machine learning model
################################################################################################
y_pred = classification_model.predict(standard_scaler_model.transform(x_test))

scores = {}
scores["Accuracy"] = accuracy_score(y_test,y_pred).round(3)
scores["Precision"] = precision_score(y_test,y_pred,average="weighted").round(3)
scores["Recall"] = recall_score(y_test,y_pred,average="weighted").round(3)
scores["F1"] = f1_score(y_test,y_pred,average="weighted").round(3)
scores["ROC AUC"] = roc_auc_score(y_test,y_pred,average="weighted").round(3)

feature_importance = pd.DataFrame(data=x_test.columns, columns=["feature"])
feature_importance["importance"] = pow(math.e, classification_model.coef_[0]).round(3)
feature_importance = feature_importance.sort_values(by=["importance"], ascending=False)

# Building visualisations
################################################################################################
def trend_line():
    temp = df.groupby(['released_year', 'released_month'])['rating'].mean().reset_index()
    temp = temp.sort_values(by=['released_month', 'released_year'])
    fig = px.line(temp, x='released_month', y='rating', color='released_year')
    
    fig.layout.title.text = '<b>Average Monthly Rating</b>'
    fig.update_layout(
        autosize = True,
        paper_bgcolor = 'rgba(0,0,0,0)',
        plot_bgcolor = 'rgba(0,0,0,0)',
        font_color = 'black',
        font_size = 10,
        margin = {'l': 70, 'r': 25, 't': 50, 'b': 25}
    )
    return fig

def metric_card(metric):
    fig = go.Figure(go.Indicator(
        mode = "number",
        value = scores[metric],
        number = {'font': {'size': 40}},
    ))
    
    fig.layout.title.text = f'<b>{metric}</b>'
    fig.update_layout(
        autosize = True,
        paper_bgcolor = 'rgba(0,0,0,0)',
        plot_bgcolor = 'rgba(0,0,0,0)',
        font_color = 'black',
        font_size = 10,
        margin = {'l': 25, 'r': 25, 't': 50, 'b': 25}
    )
    return fig

sidebar = html.Div(
    [
        html.H2('RAWG', className='display-4'),
        html.Hr(),
        html.P(
            'Video Game Database', className='lead'
        ),
        dbc.Nav(
            [
                dbc.NavLink('Exploratory Data Analysis', href='/', active='exact'),
                dbc.NavLink('Rating Analysis', href='/rating-analysis', active='exact'),
                dbc.NavLink('Machine Learning Analysis', href='/machine-learning-analysis', active='exact'),
            ],
            vertical=True,
            pills=True,
        ),
    ], className='side_bar'
)

filter_group = dbc.Row([
    dbc.Col([
        dbc.RadioItems(
            id='data-tabs',
            className='btn-group',
            inputClassName='btn-check',
            labelClassName='btn btn-outline-primary',
            labelCheckedClassName="active",
            options=[
                {'label': 'Raw Data', 'value': 'tab-raw-data'},
                {'label': 'Transformed Data', 'value': 'tab-transformed-data'}
            ],
            value='tab-raw-data',
        )
    ], className='radio-group'),
    dbc.Col(dcc.Dropdown(df.drop(columns=['name']).columns,'rating', id='feature', clearable=False))
], className='filter_group')

eda_content = html.Div([
    filter_group,

    dbc.Row([
        dbc.Col(dcc.Graph(id='eda-histogram', className='eda_graph')),
        dbc.Col(dcc.Graph(id='eda-boxplot', className='eda_graph')),
        dbc.Col(dcc.Graph(id='eda-scatterplot', className='eda_graph'))
    ]),

    dbc.Row([
        dbc.Col(dcc.Graph(id='eda-corrmatrix', className='heatmap'))
    ])
])

ra_content = html.Div([
    dbc.Row([
        dbc.Col([
            dbc.Row(dcc.Dropdown(genre_review.columns[3:], id='genre', placeholder='Select a genre...'), className='feature_dropdown_small'),
            dbc.Row(dcc.Graph(id='ta-genre-review', className='ra_graph'))
        ], className='ra_graph_container'),
        dbc.Col([
            dbc.Row(dcc.Dropdown(platform_review.columns[3:], id='platform', placeholder='Select a platform...'), className='feature_dropdown_small'),
            dbc.Row(dcc.Graph(id='ta-platform-review', className='ra_graph'))
        ], className='ra_graph_container'),
        dbc.Col([
            dbc.Row(dcc.Dropdown(store_review.columns[3:], id='store', placeholder='Select a store...'), className='feature_dropdown_small'),
            dbc.Row(dcc.Graph(id='ta-store-review', className='ra_graph'))
        ], className='ra_graph_container')
    ]),

    dbc.Row(
        dbc.Col(dcc.Graph(id='ta-time-trend', figure=trend_line(), className='amr_graph'))
    )
])

mla_dropdown = dcc.Dropdown(
    ['platform', 'store', 'genre', 'others'],
    id='feature-importance-view',
    placeholder='Select a feature view...'
)

mla_content = html.Div([
    dbc.Row([
        dbc.Col(dcc.Graph(figure=metric_card('Accuracy'), className='metric_card')),
        dbc.Col(dcc.Graph(figure=metric_card('F1'), className='metric_card')),
        dbc.Col(dcc.Graph(figure=metric_card('Precision'), className='metric_card')),
        dbc.Col(dcc.Graph(figure=metric_card('ROC AUC'), className='metric_card')),
        dbc.Col(dcc.Graph(figure=metric_card('Recall'), className='metric_card'))
    ]),
    dbc.Row(
        dbc.Col([
            dbc.Row(mla_dropdown, className='feature_dropdown_small'),
            dbc.Row(dcc.Graph(id='mla-feature-importance'))
        ], className='fi_graph')
    )
])

content = html.Div(id='page-content', className='content_style')

app.layout = html.Div([dcc.Location(id='url'), sidebar, content])

# Callbacks
################################################################################################
@app.callback(
    Output('page-content', 'children'), 
    Input('url', 'pathname')
)
def render_page_content(pathname):
    if pathname == '/':
        return eda_content
    elif pathname == '/rating-analysis':
        return ra_content
    elif pathname == '/machine-learning-analysis':
        return mla_content
    # If the user tries to reach a different page, return a 404 message
    return html.Div(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ],
        className="p-3 bg-light rounded-3",
    )

@app.callback(
    Output('eda-histogram', 'figure'),
    Input('data-tabs', 'value'),
    Input('feature', 'value')
)
def update_histogram(tab, feature):

    if tab == 'tab-raw-data':
        fig = px.histogram(df, x=feature)
    elif tab == 'tab-transformed-data':
        fig = px.histogram(df_transformed, x=feature)

    fig.layout.title.text = f'<b>Histogram of {feature}</b>'
    fig.update_layout(
        autosize = True,
        paper_bgcolor = 'rgba(0,0,0,0)',
        plot_bgcolor = 'rgba(0,0,0,0)',
        font_color = 'black',
        font_size = 10,
        margin = {'l': 70, 'r': 25, 't': 50, 'b': 25}
    )
    
    return fig

@app.callback(
    Output('eda-boxplot', 'figure'),
    Input('data-tabs', 'value'),
    Input('feature', 'value')
)
def update_boxplot(tab, feature):

    if tab == 'tab-raw-data':
        fig = px.box(df, x=feature)

    elif tab == 'tab-transformed-data':
        fig = px.box(df_transformed, x=feature)
        
    fig.layout.title.text = f'<b>Boxplot of {feature}</b>'
    fig.update_layout(
        autosize = True,
        paper_bgcolor = 'rgba(0,0,0,0)',
        plot_bgcolor = 'rgba(0,0,0,0)',
        font_color = 'black',
        font_size = 10,
        margin = {'l': 25, 'r': 25, 't': 50, 'b': 25}
    )

    return fig

@app.callback(
    Output('eda-scatterplot', 'figure'),
    Input('data-tabs', 'value'),
    Input('feature', 'value')
)
def update_scatterplot(tab, feature):

    if tab == 'tab-raw-data':
        fig = px.scatter(df, x=feature, y='rating')

    elif tab == 'tab-transformed-data':
        fig = px.scatter(df_transformed, x=feature, y='rating')
        
    fig.layout.title.text = f'<b>Scatterplot of rating against {feature}</b>'
    fig.update_layout(
        autosize = True,
        paper_bgcolor = 'rgba(0,0,0,0)',
        plot_bgcolor = 'rgba(0,0,0,0)',
        font_color = 'black',
        font_size = 10,
        margin = {'l': 70, 'r': 25, 't': 50, 'b': 25}
    )

    return fig

@app.callback(
    Output('eda-corrmatrix', 'figure'),
    Input('data-tabs', 'value')
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
        hoverinfo='none',
        colorscale='RdBu',
        showscale=True,
        ygap=1, xgap=1
    )

    fig.update_xaxes(side="bottom")
    fig.layout.title.text = '<b>Correlation Matrix of Features</b>'
    fig.update_layout(
        xaxis_showgrid=False,
        yaxis_showgrid=False,
        xaxis_zeroline=False,
        yaxis_zeroline=False,
        yaxis_autorange='reversed',
        autosize = True,
        paper_bgcolor = 'rgba(0,0,0,0)',
        plot_bgcolor = 'rgba(0,0,0,0)',
        font_color = 'black',
        font_size = 10,
        margin = {'l': 0, 'r': 0, 't': 50, 'b': 0}
    )

    for i in range(len(fig.layout.annotations)):
        if fig.layout.annotations[i].text == 'nan':
            fig.layout.annotations[i].text = ''

    return fig

@app.callback(
    Output('ta-genre-review', 'figure'),
    Input('genre', 'value')
)
def update_genre_review(genre):

    if genre is None:
        fig = px.histogram(genre_review, x='title', color='title')
        fig.layout.title.text = '<b>Spread of Ratings across All Genres</b>'
    else:
        fig = px.histogram(genre_review.loc[genre_review[genre]==1], x='title', color='title')
        fig.layout.title.text = f'<b>Spread of Ratings across Genre: {genre}</b>'

    fig.update_layout(
        xaxis_title = 'Rating',
        showlegend=False,
        autosize = True,
        paper_bgcolor = 'rgba(0,0,0,0)',
        plot_bgcolor = 'rgba(0,0,0,0)',
        font_color = 'black',
        font_size = 10,
        margin = {'l': 70, 'r': 25, 't': 50, 'b': 25}
    )

    return fig

@app.callback(
    Output('ta-platform-review', 'figure'),
    Input('platform', 'value')
)
def update_platform_review(platform):

    if platform is None:
        fig = px.histogram(platform_review, x='title', color='title')
        fig.layout.title.text = '<b>Spread of Ratings across All Platforms</b>'
    else:
        fig = px.histogram(platform_review.loc[platform_review[platform]==1], x='title', color='title')
        fig.layout.title.text = f'<b>Spread of Ratings across Platform: {platform}</b>'
    
    fig.update_layout(
        xaxis_title = 'Rating',
        showlegend=False,
        autosize = True,
        paper_bgcolor = 'rgba(0,0,0,0)',
        plot_bgcolor = 'rgba(0,0,0,0)',
        font_color = 'black',
        font_size = 10,
        margin = {'l': 70, 'r': 25, 't': 50, 'b': 25}
    )

    return fig

@app.callback(
    Output('ta-store-review', 'figure'),
    Input('store', 'value')
)
def update_store_review(store):

    if store is None:
        fig = px.histogram(store_review, x='title', color='title')
        fig.layout.title.text = '<b>Spread of Ratings across All Stores</b>'
    else:
        fig = px.histogram(store_review.loc[store_review[store]==1], x='title', color='title')
        fig.layout.title.text = f'<b>Spread of Ratings across Store: {store}</b>'
    
    fig.update_layout(
        xaxis_title = 'Rating',
        showlegend=False,
        autosize = True,
        paper_bgcolor = 'rgba(0,0,0,0)',
        plot_bgcolor = 'rgba(0,0,0,0)',
        font_color = 'black',
        font_size = 10,
        margin = {'l': 70, 'r': 25, 't': 50, 'b': 25}
    )

    return fig

@app.callback(
    Output('mla-feature-importance', 'figure'),
    Input('feature-importance-view', 'value')
)
def update_feature_importance(feature_importance_view):

    if feature_importance_view is None:
        fig = px.bar(feature_importance.head(10), x='importance', y='feature', orientation='h')
        fig.layout.title.text = '<b>Top 10 Most Important Features</b>'
    elif feature_importance_view == 'others':
        others = feature_importance[~feature_importance["feature"].str.startswith(('platform','store','genre'))].head(10)
        fig = px.bar(others, x='importance', y='feature', orientation='h')
        fig.layout.title.text = '<b>Top 10 Most Important Features (Excluding Platform, Store and Genre)</b>'
    else:
        view = feature_importance[feature_importance["feature"].str.startswith(feature_importance_view)].head(10)
        fig = px.bar(view, x='importance', y='feature', orientation='h')
        fig.layout.title.text = f'<b>Top 10 Most Important Features by {feature_importance_view.capitalize()}</b>'
    
    fig.update_yaxes(autorange='reversed')
    fig.update_layout(
        autosize = True,
        paper_bgcolor = 'rgba(0,0,0,0)',
        plot_bgcolor = 'rgba(0,0,0,0)',
        font_color = 'black',
        font_size = 10,
        margin = {'l': 50, 'r': 25, 't': 50, 'b': 25}
    )

    return fig

if __name__ == '__main__':
    app.run_server(debug=False)
