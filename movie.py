import numpy as np
import pandas as pd
import json

from mongodbConnection import connect_mongo


def merge_data(movies, ratings, links, merged_tags):
    datas = pd.merge(movies, ratings, on='movieId')
    datas = pd.merge(datas, links, on='movieId')
    datas = pd.merge(datas, merged_tags, on=['movieId', 'userId'], how='left')
    datas = datas[['movieId', 'imdbId', 'tmdbId', 'title', 'genres', 'userId', 'rating', 'tags']].\
        sort_values(by=['movieId', 'userId'], ascending=True)

    for line in range(len(datas)):
        if datas["title"][line][-12:-7] == ", The":
            datas.loc[line, "title"] = "The " + datas["title"][line].replace(", The", "")

    datas.to_csv('Datasets/movie/datas.csv', index=False)


def merge_tags(tags):
    user_id = tags.iloc[0]["userId"]
    movie_id = tags.iloc[0]["movieId"]

    tags_array = []
    merged_tags = pd.DataFrame(columns=['userId', 'movieId', 'tags'])

    for line in range(len(tags)):
        if tags["userId"][line] == user_id and tags["movieId"][line] == movie_id:
            tags_array.append(tags.loc[line, "tag"])
        else:
            arr = np.array(tags_array)
            tags_str = ', '.join(arr)
            merged_tags = merged_tags.append({'userId': user_id, 'movieId': movie_id, 'tags': tags_str}, ignore_index=True)

            tags_array.clear()
            user_id = tags.loc[line, "userId"]
            movie_id = tags.loc[line, "movieId"]

            tags_array.append(tags["tag"][line])

    merged_tags.to_csv('Datasets/movie/mergedTags.csv', index=False)


def create_document(df, col):
    movie = df[['movieId']].drop_duplicates().sort_values(['movieId'], ascending=[True])

    for i in movie[['movieId']].itertuples(index=False):
        this_movie = (df[(df['movieId'] == i)])

        agg_info = this_movie[['movieId', 'imdbId', 'tmdbId', 'title', 'genres']]

        user = this_movie[['userId', 'rating', 'tags']]

        entries = json.dumps({
            "MovieID": str(i[0]),
            "ImdbID": agg_info['imdbId'].values[0],
            "TmdbID": agg_info['tmdbId'].values[0],
            "Title": agg_info['title'].values[0],
            "Year": "123",
            "URL": "123",
            "Genres": agg_info['genres'].values[0],
            "Director": "123",
            "Actor": "123",
            "Rate": user.to_dict('records')
        })

        col.insert_one(json.loads(entries))


def main():
    col = connect_mongo("fyp", "movie")
    movies = pd.read_csv('Datasets/movie/movies.csv', sep=',', delimiter=None, encoding='latin-1')
    ratings = pd.read_csv("Datasets/movie/ratings.csv", sep=',', delimiter=None, encoding='latin-1')
    links = pd.read_csv("Datasets/movie/links.csv", sep=',', delimiter=None, encoding='latin-1')

    # create merged tags csv if it not exits
    try:
        merged_tags = pd.read_csv("Datasets/movie/mergedTags.csv", sep=',', delimiter=None, encoding='latin-1')
    except FileNotFoundError:
        tags = pd.read_csv("Datasets/movie/tags.csv", sep=',', delimiter=None, encoding='latin-1')
        merge_tags(tags)
        merged_tags = pd.read_csv("Datasets/movie/mergedTags.csv", sep=',', delimiter=None, encoding='latin-1')

    # merged dataset if datas.csv is not exits
    try:
        datas = pd.read_csv("Datasets/movie/datas.csv", sep=',', delimiter=None, encoding='latin-1')
    except FileNotFoundError:
        merge_data(movies, ratings, links, merged_tags)
        datas = pd.read_csv("Datasets/movie/datas.csv", sep=',', delimiter=None, encoding='latin-1')

    create_document(datas, col)


if __name__ == '__main__':
    main()
