import numpy as np
import pandas as pd
import json

import re
import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

from mongodb_connection import connect_mongo


def separate_data(books):
    isbn_array = []
    book_title_array = []
    book_author_array = []
    book_year_array = []
    book_publisher_array = []
    book_images_s_array = []
    book_images_m_array = []
    book_images_l_array = []

    for book in range(0, len(books)):
        book_data = ""

        for column in range(0, 20):
            if books.iloc[book, column] is np.nan:
                break

            book_data = book_data + str(books.iloc[book, column])

        isbn_index = book_data.find(";\"")
        isbn = "y" + book_data[0:int(isbn_index)]

        book_title_index = book_data.find("\";\"", int(isbn_index) + 2)
        book_title = book_data[int(isbn_index) + 2:int(book_title_index)]

        book_author_index = book_data.find("\";\"", int(book_title_index) + 3)
        book_author = book_data[int(book_title_index) + 3:int(book_author_index)]

        book_year_index = book_data.find("\";\"", int(book_author_index) + 3)
        book_year = book_data[int(book_author_index) + 3:int(book_year_index)]

        book_publisher_index = book_data.find("\";\"", int(book_year_index) + 3)
        book_publisher = book_data[int(book_year_index) + 3:int(book_publisher_index)]

        book_image_s_index = book_data.find("\";\"", int(book_publisher_index) + 3)
        book_image_s = book_data[int(book_publisher_index) + 3:int(book_image_s_index)]

        book_image_m_index = book_data.find("\";\"", int(book_image_s_index) + 3)
        book_image_m = book_data[int(book_image_s_index) + 3:int(book_image_m_index)]

        book_image_l_index = book_data.find("\";\"", int(book_image_m_index) + 3)
        book_image_l = book_data[int(book_image_m_index) + 3:int(book_image_l_index)]

        isbn_array.append(isbn)
        book_title_array.append(book_title)
        book_author_array.append(book_author)
        book_year_array.append(book_year)
        book_publisher_array.append(book_publisher)
        book_images_s_array.append(book_image_s)
        book_images_m_array.append(book_image_m)
        book_images_l_array.append(book_image_l)

    separated_data = pd.DataFrame()
    separated_data['ISBN'] = isbn_array
    separated_data['title'] = book_title_array
    separated_data['author'] = book_author_array
    separated_data['publication'] = book_year_array
    separated_data['publisher'] = book_publisher_array
    separated_data['URLS'] = book_images_s_array
    separated_data['URLM'] = book_images_m_array
    separated_data['URLL'] = book_images_l_array

    separated_data.to_csv('Datasets/book/books.csv', index=False)


def separate_rating(ratings):
    user_id_array = []
    isbn_array = []
    rating_array = []

    for rate in range(0, len(ratings)):
        rate_data = ratings.iloc[rate, 0]

        user_id_index = rate_data.find(";\"")
        user_id = rate_data[0:int(user_id_index)]

        isbn_index = rate_data.find("\";\"", int(user_id_index) + 2)
        isbn = "y" + rate_data[int(user_id_index) + 2:int(isbn_index)]

        rate_index = rate_data.find("\";\"", int(isbn_index) + 3)
        rating = rate_data[int(isbn_index) + 3:int(rate_index)]

        user_id_array.append(user_id)
        isbn_array .append(isbn)
        rating_array.append(rating)

    separated_data = pd.DataFrame()
    separated_data['userId'] = user_id_array
    separated_data['ISBN'] = isbn_array
    separated_data['rating'] = rating_array

    separated_data.to_csv('Datasets/book/ratings.csv', index=False)


def merge_data(books, ratings):
    datas = pd.merge(books, ratings, on='ISBN', how='left')
    datas = datas[['ISBN', 'title', 'author', 'publication', 'publisher', 'URLS', 'URLM', 'URLL', 'userId', 'rating']].\
        sort_values(by=['ISBN', 'UserId'], ascending=True)

    datas.to_csv('Datasets/book/datas.csv', index=False)


def create_document(df, col):
    books = df[['ISBN']].drop_duplicates().sort_values(['ISBN'], ascending=[True])

    for i in books[['ISBN']].itertuples(index=False):
        this_book = df[df['ISBN'].values == i]

        agg_info = this_book[['ISBN', 'title', 'author', 'publication', 'publisher', 'URLS', 'URLM', 'URLL']]

        user = this_book[['userId', 'rating']]

        entries = json.dumps({
            "ISBN": i[0][1:],
            "title": agg_info['title'].values[0],
            "author": agg_info['author'].values[0],
            "year": agg_info['publication'].values[0],
            "publisher": agg_info['publisher'].values[0],
            "bookImageS": agg_info['URLS'].values[0],
            "bookImageM": agg_info['URLM'].values[0],
            "bookImageL": agg_info['URLL'].values[0],
            "rate": user.to_dict('records')
        })

        # print(json.loads(entries))
        col.insert_one(json.loads(entries))


def main():
    col = connect_mongo("fyp", "book")

    books = pd.read_csv('Datasets/book/BX-Books.csv', sep=',', delimiter=None, encoding='latin-1')
    ratings = pd.read_csv("Datasets/book/BX-Book-Ratings.csv", sep=',', delimiter=None, encoding='latin-1')
    # links = pd.read_csv("Datasets/movie/links.csv", sep=',', delimiter=None, encoding='latin-1')

    try:
        new_books = pd.read_csv('Datasets/book/books.csv', sep=',', delimiter=None, encoding='latin-1')
    except FileNotFoundError:
        separate_data(books)
        new_books = pd.read_csv('Datasets/book/books.csv', sep=',', delimiter=None, encoding='latin-1')

    try:
        new_rating = pd.read_csv('Datasets/book/ratings.csv', sep=',', delimiter=None, encoding='latin-1')
    except FileNotFoundError:
        separate_rating(ratings)
        new_rating = pd.read_csv('Datasets/book/ratings.csv', sep=',', delimiter=None, encoding='latin-1')

    try:
        new_data = pd.read_csv('Datasets/book/datas.csv', sep=',', delimiter=None, encoding='latin-1')
    except FileNotFoundError:
        merge_data(new_books, new_rating)
        new_data = pd.read_csv('Datasets/book/datas.csv', sep=',', delimiter=None, encoding='latin-1')

    create_document(new_data, col)


if __name__ == '__main__':
    main()
