from solution import solution_start
import pandas as pd
import json
from pathlib import Path


df = pd.DataFrame({
    'Name': 'Viganesha',
    'basket': [{'Phone':'OnePlus','Car':'Ertiga'}],
    'email':'viganeshahr@gmail.com'})

df2 = pd.DataFrame({
    'Name': ['Viganesha','Vinay'],
    'basket': [[{'Phone':'OnePlus','Car':'Ertiga'}],[{'Phone':'IPhone','Car':'Porsche'}]],
    'email':['viganeshahr@gmail.com','vinaybr@gmail.com']})


def test_merge():
	with open('tests/merge_sample.txt') as f:
		data = f.read()
	assert data == solution_start.merge_dataframes(pd.read_json('[{"Name":"Viganesha H R","Age":"24"}]'),
					pd.read_json('[{"Name":"Viganesha H R","Age":"26"}]'),"Name").to_string(index=False)


def test_explode():
	with open('tests/explode_sample.txt') as f:
		data = f.read()
	assert data == solution_start.normalize_customer_products(df,'basket').to_string(index=False)

def test_readcsv():
	with open('tests/read_sample.txt') as f:
		data = f.read()
	assert data == solution_start.read_file("tests/sample_csv_read.csv").to_string(index=False)


def test_writjson():
	solution_start.write_to_json(df2,"output.json")
	assert Path("output.json").exists()

