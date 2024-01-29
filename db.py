import sqlite3


# names
FUND_TABLE_NAME = "fund_nav"
FUND_TABLE_NAME_DAILY = "fund_nav_daily"
DATABASE_URI = "db_files/funds.db"


# requests
SQL = {
    "FUND_NAV_DAILY": """
        CREATE TABLE IF NOT EXISTS fund_nav_daily AS SELECT * FROM fund_nav LIMIT 0
        """,
    "ADD_AS_OF_DATE_COLUMN": """
        ALTER TABLE fund_nav_daily ADD COLUMN as_of_date TEXT;
        CREATE UNIQUE INDEX pkUnique ON fund_nav_daily
        (
            fund_code, 
            share_code,
            share_type,
            isin_code,
            as_of_date
        )

    """,
    "UPDATE_RECENT_PRICE": """
        REPLACE INTO fund_nav_daily SELECT 
            t.fund_code,
            t.fund_currency,
            t.share_code,
            t.share_type,
            t.isin_code,
            t.share_currency,
            t.nav_date,
            t.price_in_share_currency,
            t.fx_rate,
            t.price_in_fund,
            t.number_of_oustanding_shares,
            t.tna_share_in_share_currency,
            t.tna_share_in_fund_currency,
            t.fx_rate_date, 
            '{date}'  AS as_of_date 
            FROM fund_nav t
        INNER JOIN (
        SELECT 
            fund_code, 
            share_code,
            share_type,
            isin_code,
            max(nav_date) AS max_date
        FROM fund_nav 
        WHERE nav_date <= '{date}'
        GROUP BY 	
            fund_code, 
            share_code,
            share_type) md
            ON
            t.fund_code = md.fund_code 
            AND t.share_code = md.share_code
            AND t.share_type = md.share_type	
            AND t.isin_code = md.isin_code
            AND t.nav_date = md.max_date;
    """,
}


def get_connection():
    return sqlite3.Connection(DATABASE_URI)
