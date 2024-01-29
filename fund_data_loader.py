#!/usr/bin/env python3

# Fineco coding homework by Ekaterina Lunacharskaia
#
# Task:
#
# The dataset provided contains information around a set of funds managed
# by FAM. The dataset contains the fund codes, share classes, prices and
# other information related to the funds.

# Create a python script that loads the sample data into a SQL table
# called fund_nav from the excel file provided.

# Possible extensions
# add missing values handling (drop, fill)


# imports
import pandas as pd
import argparse
import logging

import db
import config


DEFAULT_INPUT_FILE = "file_drop/fund_dataset_sample-python.xlsx"


# data columns and types
COLUMN_TYPES = {
    "Fund Code": "object",
    "Fund Currency": "object",
    "Share Code": "int64",
    "Share Type": "object",
    "ISIN Code": "object",
    "Share Currency": "object",
    "NAV Date": "datetime64[ns]",
    "Price In Share Currency": "float64",
    "FX Rate": "float64",
    "Price In Fund": "float64",
    "Number of Oustanding Shares": "float64",
    "TNA Share In Share Currency": "float64",
    "TNA Share In Fund Currency": "float64",
    "FX Rate Date": "datetime64[ns]",
}

STRING_COLUMNS = [
    "Fund Code",
    "Fund Currency",
    "Share Type",
    "ISIN Code",
    "Share Currency",
]

NUMERIC_COLUMNS = [
    "Price In Share Currency",
    "FX Rate",
    "Price In Fund",
    "Number of Oustanding Shares",
    "TNA Share In Share Currency",
    "TNA Share In Fund Currency",
]


def read_data_from_file(input_file_path, drop_zero=False):
    """
    Returns a dataframe with the funds data obtained from the goven file.
    """

    try:
        # read from an Excel file
        data = pd.read_excel(input_file_path)

        # check if the given file has the expected structure
        data.columns = [col.strip() for col in data.columns]

        expected_columns = set(COLUMN_TYPES.keys())
        actual_columns = set(data.columns)

        # if mismatch between expected and actual
        if not expected_columns == actual_columns:
            missing_columns = expected_columns - actual_columns

            # raise exception as the data integrity is breached
            if missing_columns:
                logging.error(
                    "Expected colums not found: " + ", ".join(missing_columns)
                )
                raise Exception

            extra_columns = actual_columns - expected_columns

            # if extra columns found -> keep relevant columns only
            if extra_columns:
                data = data[COLUMN_TYPES.keys()]
                logging.error(
                    "Unexpected colums found in the source file: "
                    + ", ".join(extra_columns)
                )

        # drop duplicates (assuming duplicated rows are not allowed by the dataset nature)
        original_length = len(data)
        data.drop_duplicates(subset=None, keep="first", inplace=True)
        new_length = len(data)
        dropped_rows = original_length - new_length
        logging.info("Dropped: " + str(dropped_rows) + " duplicated rows")

        # handle missing numeric values
        data[NUMERIC_COLUMNS] = data[NUMERIC_COLUMNS].fillna(0)

        # drop zero prices if selected
        if drop_zero:
            mask = data["Price In Share Currency"] == 0
            data = data[~mask]
            logging.info("Dropped " + str(mask.sum()) + " rows with zero proces")

        # set column types
        data = data.astype(COLUMN_TYPES)

        # precess dates
        data["FX Rate Date"] = pd.to_datetime(
            data["FX Rate Date"], format="%Y-%m-%d"
        ).dt.date
        data["NAV Date"] = pd.to_datetime(data["NAV Date"], format="%Y-%m-%d").dt.date

        # convert string columns to uppercase
        data[STRING_COLUMNS].map(lambda x: x.upper())

        return data

    except Exception as e:
        logging.error("Data Import Error: " + str(e))
        raise e


def store_to_database(data: pd.DataFrame):
    """
    Stores a given dataframe to a database table.
    """

    try:
        # db connection
        connection = db.get_connection()
        # column names normalisation for sql
        data.columns = [col.lower().replace(" ", "_") for col in data.columns]

        data.to_sql(db.FUND_TABLE_NAME, connection, if_exists="replace", index=False)

        connection.close()

    except Exception as e:
        logging.error("Database Error: " + str(e))
        raise e


if __name__ == "__main__":
    logging.basicConfig(format=config.LOG_FORMAT, level=logging.INFO)

    parser = argparse.ArgumentParser(
        prog="fund_data_loader", description="Loads XLSX funds data to database"
    )

    parser.add_argument(
        "-i", "--input", help="input XLXS file", default=DEFAULT_INPUT_FILE
    )

    parser.add_argument(
        "-z", "--dropzero", help="drop rows with zero price", action="store_true"
    )

    args = parser.parse_args()
    input_file_path = args.input
    drop_zero = args.dropzero

    logging.info("Reading data from " + input_file_path)
    data = read_data_from_file(input_file_path, drop_zero=drop_zero)

    logging.info("Storing data to fund_nav from " + input_file_path)
    store_to_database(data)
