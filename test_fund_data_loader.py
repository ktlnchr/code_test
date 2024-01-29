import fund_data_loader
import unittest


# possible tests for fund_data_loader:
# columns type check
# check if zeroes dropped
# check if duplicates dropped
# check if stored to db, ...

# possible tests for fund_data_loader:
# check if fund_nav_daily is updated
# check if no duplicates by key
# chek if nav_date vals are filtered properly
# check if duplicates are not insrted, ...


class TestLoader(unittest.TestCase):
    def test_missing_columns(self):
        with self.assertRaises(Exception) as context:
            fund_data_loader.read_data_from_file(
                "test_data/fund_dataset_missing_cols.xlsx"
            )
            self.assertTrue(
                str(context.exception) == "Expected colums not found: Fund Code"
            )

    def test_extra_columns(self):
        data = fund_data_loader.read_data_from_file(
            "test_data/fund_dataset_extra_cols.xlsx"
        )

        self.assertTrue(
            set(data.columns)
            == {
                "Fund Code",
                "Fund Currency",
                "Share Code",
                "Share Type",
                "ISIN Code",
                "Share Currency",
                "NAV Date",
                "Price In Share Currency",
                "FX Rate",
                "Price In Fund",
                "Number of Oustanding Shares",
                "TNA Share In Share Currency",
                "TNA Share In Fund Currency",
                "FX Rate Date",
            }
        )


if __name__ == "__main__":
    unittest.main()
