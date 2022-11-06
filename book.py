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

    for book in range(1, len(books)):
        book_data = ""

        for column in range(0, 20):
            if books.iloc[book, column] is np.nan:
                break

            book_data = book_data + str(books.iloc[book, column])

        isbn_index = book_data.find(";\"")
        isbn = book_data[0:int(isbn_index)]

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
    separated_data['Title'] = book_title_array
    separated_data['Author'] = book_author_array
    separated_data['Publication'] = book_year_array
    separated_data['Publisher'] = book_publisher_array
    separated_data['URLS'] = book_images_s_array
    separated_data['URLM'] = book_images_m_array
    separated_data['URLL'] = book_images_l_array

    separated_data.to_csv('Datasets/book/books.csv', index=False)


def main():
    col = connect_mongo("fyp", "book")

    books = pd.read_csv('Datasets/book/BX-Books.csv', sep=',', delimiter=None, encoding='latin-1')
    # ratings = pd.read_csv("Datasets/movie/ratings.csv", sep=',', delimiter=None, encoding='latin-1')
    # links = pd.read_csv("Datasets/movie/links.csv", sep=',', delimiter=None, encoding='latin-1')

    separate_data(books)


if __name__ == '__main__':
    main()
