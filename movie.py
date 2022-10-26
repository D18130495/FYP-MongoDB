import pandas as pd
import json

from bson import json_util

from mongodbConnection import connect_mongo


def create_document(df, col):
    movie = df[['movieId']].drop_duplicates().sort_values(['movieId'], ascending=[True])

    for i in movie[['movieId']].itertuples(index=False):
        this_movie = (df[(df['movieId'] == i)])

        agg_info = this_movie[['movieId', 'title', 'genres']]

        entries = json.dumps({
            "MovieID": str(i[0]),
            "Title": agg_info['title'].values[0],
            "Genres": agg_info['genres'].values[0]
            # "Athletes": tc.to_dict('records')
        })
        # print(json.loads(entries))
        col.insert_one(json.loads(entries))


def main():
    col = connect_mongo("fyp", "movie")
    df = pd.read_csv('Datasets/movie/movies.csv', sep=',', delimiter=None, encoding='latin-1')
    create_document(df, col)
    # mydict = {"name": "RUNOOB", "alexa": "10000", "url": "https://www.runoob.com"}
    # col.insert_one(mydict)


if __name__ == '__main__':
    main()

    # agginfo = movie[['ID', 'ImdbID', 'TmdbID', 'Title', 'Year', 'URL', 'Director', 'Actor', 'Genres', 'Rate']]
