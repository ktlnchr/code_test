Prerequisites:
Python 3.11.7 

Install dependencies:
python3 -m pip install -r requirements.txt

Scripts:
fund_data_loader.py - loads fund data from the given xlsx file to fund_nav table in database
Use ./fund_data_loader.py --help for parameters information

fund_forward_filler.py - appends as of date prices from fund_nav to fund_nav_daily in database
Use ./fund_forward_filler.py --help for parameters information
