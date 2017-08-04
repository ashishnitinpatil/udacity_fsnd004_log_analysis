#!/usr/bin/python3
# -*- coding: utf-8 -*-
import psycopg2
import pprint


def fetch_results(query, limit):
    """Wrapper for fetching query from DB and returning results"""
    with psycopg2.connect("dbname=news") as db:
        c = db.cursor()
        c.execute(query)
        results = c.fetchmany(limit)
    return results


def get_most_popular_articles(limit=3):
    """Fetches and returns the top 3 <limit> most popular articles"""
    query = """SELECT title, count(log.id) AS views
               FROM articles
               JOIN log
                  ON path LIKE '%%' || slug || '%%'
                  AND status = '200 OK'
               GROUP BY articles.id
               ORDER BY views DESC"""
    return fetch_results(query, limit)


def get_most_popular_article_authors(limit=5):
    """Fetches and returns the most popular authors"""
    query = """SELECT name, count(log.id) AS views
               FROM log
               JOIN (
                   SELECT name, slug, title
                   FROM articles
                   LEFT JOIN authors
                       ON authors.id = articles.author
               ) AS article_authors
                  ON path LIKE '%%' || slug || '%%'
                  AND status = '200 OK'
               GROUP BY name
               ORDER BY views DESC"""
    return fetch_results(query, limit)


if __name__ == '__main__':
    print(' Log Analysis Report '.center(80, '*') + '\n')
    # Report 1
    top_articles = get_most_popular_articles()
    for i, article in enumerate(top_articles):
        if i == 0:
            print('Report #1 : Most popular articles of all time\n')
        print("\t{}. {} — {:>6} views".format(i+1, article[0], article[1]))
    # Report 2
    top_authors = get_most_popular_article_authors()
    for i, author in enumerate(top_authors):
        if i == 0:
            print('\nReport #2 : Most popular article authors of all time\n')
        print("\t{}. {:<22} — {:>6} views".format(i+1, author[0], author[1]))
