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
    query = r"""SELECT title, COUNT(log.id) AS views
                FROM articles
                JOIN log
                   ON path LIKE '%' || slug || '%'
                   AND status = '200 OK'
                GROUP BY articles.id
                ORDER BY views DESC"""
    return fetch_results(query, limit)


def get_most_popular_article_authors(limit=5):
    """Fetches and returns the most popular authors"""
    query = r"""SELECT name, COUNT(log.id) AS views
                FROM log
                JOIN (
                    SELECT name, slug, title
                    FROM articles
                    LEFT JOIN authors
                        ON authors.id = articles.author
                ) AS article_authors
                   ON path LIKE '%' || slug || '%'
                   AND status = '200 OK'
                GROUP BY name
                ORDER BY views DESC"""
    return fetch_results(query, limit)


def get_erroneous_days(limit=5):
    """Fetches and returns the erroneous day (>1% requests resulted in error)"""
    query = r"""SELECT requests_by_day.day,
                       errors * 100.0 / requests AS err_pct
                FROM (
                    SELECT date("time") AS day, count(log.id) AS requests
                    FROM log
                    GROUP BY day
                ) AS requests_by_day
                LEFT JOIN (
                    SELECT date("time") AS day, count(log.id) AS errors
                    FROM log
                    WHERE status NOT like '2%'
                    GROUP BY day
                ) AS errors_by_day
                ON requests_by_day.day = errors_by_day.day
                WHERE errors * 100.0 / requests > 1.0
                ORDER BY requests_by_day.day"""
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

    # Report 3
    erroneous_days = get_erroneous_days()
    for i, err_day in enumerate(erroneous_days):
        if i == 0:
            print('\nReport #3 : '
                  'Days when more than 1% of requests lead to errors\n')
        pretty_day = '{dt:%B} {dt.day}, {dt.year}'.format(dt=err_day[0])
        print("\t{}. {} — {:.2f}% errors".format(i+1, pretty_day, err_day[1]))
