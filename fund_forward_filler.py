#!/usr/bin/env python3

# Fineco coding homework by Ekaterina Lunacharskaia
#
# Task:
#
# Create a python script that forward fills a new table called
# fund_nav_daily with the data from the fund_nav table. This new table is
# the same as the fund_nav table except it has an additional column that
# you can call as_of_date. For each as_of_date there should be a row for
# each of the funds with the latest available price at the time.

import datetime as dt
import sqlite3
import logging
import argparse
import re

import db
import config

DEFAULT_DATE = dt.datetime.today().date().strftime("%Y-%m-%d")
DATE_REG_EXP = re.compile("^\d{4}-\d{2}-\d{2}$")


def is_valid_input_date(input_date):
    """
    Validity of input date format check
    """
    return DATE_REG_EXP.match(input_date) is not None


def update_table_daily(as_of_date):
    try:
        # establish connection
        connection = db.get_connection()

        # get coursor object
        cursor = connection.cursor()

        # if fund_nav_daily not exists -> create
        cursor.executescript(db.SQL["FUND_NAV_DAILY"])

        # try to add as_of_date column
        try:
            cursor.executescript(db.SQL["ADD_AS_OF_DATE_COLUMN"])

        except sqlite3.OperationalError as e:
            # expected if table has been already created
            if str(e).startswith("duplicate column name"):
                pass

            # if something went wrong
            else:
                logging.error("Failed to add column as_of_date: " + str(e))
                raise e

        # appned the most recent data from fund_nav
        cursor.executescript(db.SQL["UPDATE_RECENT_PRICE"].format(date=as_of_date))
        connection.close()

    except Exception as e:
        logging.error("Database Error: " + str(e))


if __name__ == "__main__":
    logging.basicConfig(format=config.LOG_FORMAT, level=logging.INFO)

    parser = argparse.ArgumentParser(
        prog="fund_forward_filler",
        description="Upadtes funds table with the daily data in database",
    )
    parser.add_argument(
        "-d",
        "--date",
        help="as of date in YYYY-MM-DD format",
        default=DEFAULT_DATE,
    )

    args = parser.parse_args()
    input_date = args.date

    if not is_valid_input_date(input_date):
        logging.error(
            "Invalid input date format: " + input_date + " YYYY-mm-dd is expected."
        )
        raise Exception

    logging.info("Updating fund_nav_daily in database for " + input_date)
    update_table_daily(input_date)
