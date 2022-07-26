#!/bin/bash
source /home/$(whoami)/anaconda3/etc/profile.d/conda.sh
conda activate bolsa
export PYTHONPATH=${PYTHONPATH}:../../
python main_market_data.py
cd ./get_stocks_easy_logic
export PYTHONPATH=${PYTHONPATH}:../../../
python Europe_stocks_data.py
python US_stocks_data.py
cd ../webscraping/
python us_earning_results.py
