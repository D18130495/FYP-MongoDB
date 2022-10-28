import numpy as np
import pandas as pd
import json

import re
import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

from mongodb_connection import connect_mongo


def merge_data(movies, ratings, links, merged_tags):
    url_array = []
    url_prefix = "https://www.imdb.com/title/tt"
    url_suffix = ""

    # merge datasets
    datas = pd.merge(movies, ratings, on='movieId')
    datas = pd.merge(datas, links, on='movieId')
    datas = pd.merge(datas, merged_tags, on=['movieId', 'userId'], how='left')
    datas = datas[['movieId', 'imdbId', 'tmdbId', 'title', 'genres', 'userId', 'rating', 'tags']]. \
        sort_values(by=['movieId', 'userId'], ascending=True)

    for line in range(len(datas)):
        # reset the movie title
        if datas["title"][line][-12:-7] == ", The":
            datas.loc[line, "title"] = "The " + datas["title"][line].replace(", The", "")

        # structure movie URL
        url_suffix_list = list(url_suffix)
        for i in range(7 - len(str(datas["imdbId"][line]))):
            url_suffix_list.append("0")

        url_suffix_list.append(str(datas["imdbId"][line]))
        suffix_str = ''.join(url_suffix_list)
        movie_url = url_prefix + suffix_str + "/"

        url_array.append(movie_url)

    # add new column to the dataframe
    datas['URL'] = url_array

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
            merged_tags = merged_tags.append({'userId': user_id, 'movieId': movie_id, 'tags': tags_str},
                                             ignore_index=True)

            tags_array.clear()
            user_id = tags.loc[line, "userId"]
            movie_id = tags.loc[line, "movieId"]

            tags_array.append(tags["tag"][line])

    merged_tags.to_csv('Datasets/movie/mergedTags.csv', index=False)


def movie_detail(movie_url):
    try:
        response = requests.get(movie_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'lxml')

            movie_year = soup.select_one('a[class="ipc-link ipc-link--baseAlt ipc-link--inherit-color sc-8c396aa2-1 '
                                         'WIUyh"]').string

            movie_image = soup.select_one('img[class="ipc-image"]')['src']

            movie_description = soup.select_one('span[class="sc-16ede01-0 fMPjMP"]').string

            director = soup.select_one('div[class="ipc-metadata-list-item__content-container"]')

            director_link = "https://www.imdb.com/" + director.select_one('a')['href']
            director_name = director.select_one('a').string
            director_id_rule = re.compile(r'(?<=nm)\d+(?=/?)')
            director_id = int(director_id_rule.search(director_link).group())

            try:
                actors = soup.select('ul[class="ipc-inline-list ipc-inline-list--show-dividers ipc-inline-list--inline '
                                     'ipc-metadata-list-item__list-content baseAlt"]')[2]

                actors_dict = get_cast_data(actors)
            except IndexError:
                director_id = 0
                director_name = None
                director_link = None
                actors_dict = None

            return movie_year, movie_image, movie_description, director_id, director_name, director_link, actors_dict
        else:
            print("GET url of movie Do Not 200 OK!")
            movie_year = None
            movie_image = None
            movie_description = None
            director_id = 0
            director_name = None
            director_link = None
            actors_dict = None
            return movie_year, movie_image, movie_description, director_id, director_name, director_link, actors_dict
    except RequestException:
        print("Get Movie URL failed!")
        return None


def get_cast_data(actors):
    actors_array = []

    for actor in actors:
        actor_data = actor.select_one('a[class="ipc-metadata-list-item__list-content-item '
                                      'ipc-metadata-list-item__list-content-item--link"]')

        actor_link = "https://www.imdb.com" + actor_data['href']

        actor_id_rule = re.compile(r'(?<=nm)\d+(?=/)')
        actor_id = int(actor_id_rule.search(actor_link).group())

        actor_name = actor_data.get_text().strip()

        actors_array.append({"actorId": actor_id, "actorName": actor_name, "actorLink": actor_link})

    return actors_array


def create_document(df, col):
    movie = df[['movieId']].drop_duplicates().sort_values(['movieId'], ascending=[True])

    for i in movie[['movieId']].itertuples(index=False):
        # if int(i[0]) < 26528:
        #     continue

        this_movie = (df[(df['movieId'] == i)])

        agg_info = this_movie[['movieId', 'imdbId', 'tmdbId', 'title', 'genres', 'URL']]

        user = this_movie[['userId', 'rating', 'tags']]

        movie_year, movie_image, movie_description, director_id, director_name, director_link, actors_dict\
            = movie_detail(agg_info['URL'].values[0])

        entries = json.dumps({
            "movieId": int(i[0]),
            "imdbId": int(agg_info['imdbId'].values[0]),
            "tmdbId": int(agg_info['tmdbId'].values[0]),
            "title": agg_info['title'].values[0],
            "year": str(movie_year),
            "url": agg_info['URL'].values[0],
            "genres": agg_info['genres'].values[0],
            "movieImage": movie_image,
            "description": movie_description,
            "director": {
                "directorId": int(director_id),
                "directorName": director_name,
                "directorLink": director_link
            },
            "actor": actors_dict,
            "rate": user.to_dict('records')
        })
        print(json.loads(entries))
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
