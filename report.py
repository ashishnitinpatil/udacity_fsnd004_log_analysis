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
    """Fetches & returns the erroneous day (>1% requests resulted in error)"""
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
    print(' Log Analysis Report '.center(80, '*'))

    reports = [
        {'method': get_most_popular_articles,
         'title': 'Most popular articles of all time',
         'format': '{count}. {item} — {aggregate:>6} views'},
        {'method': get_most_popular_article_authors,
         'title': 'Most popular article authors of all time',
         'format': '{count}. {item:<22} — {aggregate:>6} views'},
        {'method': get_erroneous_days,
         'title': 'Days when more than 1% of requests lead to errors',
         'format': '{count}. {item:%B} {item.day}, {item.year}'
                   ' — {aggregate:.2f} errors'},
    ]
    for i, report in enumerate(reports):
        results = report['method']()
        for count, result in enumerate(results):
            if count == 0:
                print('\nReport #{} : {}\n'.format(i + 1, report['title']))
            print('\t' + report['format'].format(count=count + 1,
                                                 item=result[0],
                                                 aggregate=result[1]))
