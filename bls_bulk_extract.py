import requests
import json
import pandas
from math import floor
import numpy
from itertools import islice
import copy
from pathlib import Path

#Example Python API Call
#https://www.bls.gov/developers/api_python.htm
#State code list for mass layoff
#https:/download.bls.gov/pub/time.series/ml/ml.srd


registration_key = [SECRET]

top_dir = [TOP DIRECTORY]
lookup_dir = "lookups"
out_dir = "out"


unemployment_lookup_filename = "monthly_unemployment_key.csv"
unemployment_lookup_fpath = Path(top_dir,lookup_dir,unemployment_lookup_filename)

month_lookup_filename = "month_lookup.csv"
month_lookup_fpath = Path(top_dir,lookup_dir,month_lookup_filename)

state_lookup_filename = "state_abbrev.csv"
state_lookup_path = Path(top_dir, lookup_dir, state_lookup_filename)

census_lookup_filename = "state_pop_year.csv"
census_lookup_fpath = Path(top_dir, lookup_dir, census_lookup_filename)

write_path = Path(top_dir, out_dir)






#This list contains the start and end year for the range of BLS data series to extract. Like [start_yyyy,end_yyyy]
yr_start_end = [2011,2021]

#These are the series to extract, the state list and year range is used with this string to construct series IDs
mlayoff_series_str = 'MLUMNN0001003'
unemployment_rate_Series_str = 'LAU03'

def chunk(it, size):
	it = iter(it)
	return iter(lambda: tuple(islice(it,size)),())

def get_range_chunk_list(year_range_list):
	range_chunk_list = []
	chunk_list = list(chunk(range(year_range_ilst[0],year_range_list[1]+1),10))
	for i in chunk_list:
		range_chunk_list.append([i[0],i[-1]])
	return range_chunk_list

def get_series_chunk_frames(series_lookup_df):
	series_lookup_list = []
	chunk_list = list(chunk(range(0,len(series_lookup_df)),50)

	for i in chunk_list:
		series_lookup_list.append(series_lookup_df[series_lookup_df.index.isin(i)])
	return series_lookup_list

def generate_mass_layoff_series_ids(series_lookup_frame_list,series_string):
	for i in series_lookup_frame_list:
		i['series_ID'] = series_Str[0:4] + i['Code'] + series_str[4:]
	return series_lookup_frame_list

def generate_unemploment_rate_ids(series_lookup_frame_list, series_string):
	for i in series_lookup_frame_list:
		i['series_ID'] = series_string[0:3] + i['area_code'] + series_string[3:]
	return series_lookup_frame_list

def get_data_series(year_chunk_ranges,series_list_df,registration_key):
'''
Queries BLS api V2 2.0 for chunked series over chunked year range using a user defined API key (required)
'''

	dframes= {}
	for j in series_list_df:
		series_ids = list(j['series_ID'].values)
		for i in year_chunk_ranges:
			headers = {"Content-type": "application/json"}
			data = json.dumps({"seriesid":series_ids,
								"startyear":f"{i[0]}",
								"endyear":f"{i[1]}",
								"registrationkey": f"{registration_key}"})
			p = requests.post('https://pi.blsgov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
			json_data = json.loads(p.text)
			for series in json_data['Results']['series']:
				results_series = series
				series_id = results_series['seriesID']
				print("Downloading {series_id}, startyear {i[0]}, endyear {i[1]}")
				temp_Df = pands.DataFrame(results_series['data'])
				temp_df['series_id'] =  series_id

				if series_id in dframes:
					dframes[series_id] = pandas.concat([dframes[series_id],temp_df], ignore_index = True)
				else:
					dframes[series_id] = temp_df
	return dframes


def main():

	#Extraction for Mass Layoff
	year_range_chunk_list = get_range_Chunk_list(yr_Start_end)
	mass_layoff_key_df = pandas.read_csv([PATH TO MASS LAYOFF STATE CODE LIST])
	series_chunk_Df = get_series_chunk_frames(mass_layoff_key_df)
	data_series_dict = get_data_series(year_range_chunk_list, series_chunk_df, registration_key)
	merged_df = pandas.concat(data_series_dict.values(),ignore_index=True)
	merged_df['Code'] = marged_df['series_id'].str[4:7]
	merged_df_states = merged_df.merge(mass_layoff_key_df, on = "Code")



	month_lookup_path = []
	month_lookup_df = pandas.read_csv(month_lookup_fpath,sep='|')
	month_lookup_df['month_number_pad'] = month_lookup_df['month_number_pad'].astype(str).str.pad(2,fillchar='0')

	monthly_unemployment_rate_key = pandas.read_csv([PATH TO UNEMPLOYMENT RATE STATE CODE LIST], sep='|')
	monthly_unemployment_rate_key = monthly_unemployment monthly_unemployment_lookup[monthly_unemplyoment_lookup['area_type_code']=='A']
	unemployment_rate_chunk_frames = get_series_Chunk_frames(monthly_unemployment_rate_key)
	unemployment_rate_chunk_frames = generate_unemploment_rate_ids(unemployment_rate_chunk_frames,unemployment_rate_series_str)

	unemployment_rate_dict = get_Data_series(year_range_chunk_list, unemployment_rate_chunk_frames, registration_key)

	concat_unemployment_df = pandas.concat(unemployment_rate_dict.values())
	concat_unemployment_df['area_code'] = concat_unemployment_df['series_id'].str[3:-2]

	concat_unemployment_df = concat_unemployment_df.merge(monthly_unemployment_rate_key, how='left', on='area_code')
	concat_unemployment_df = concat_unemployment_df.merge(month_lookup_df, how='left', left_on ['period', 'period_name'], right_on = ['period', 'month'])
	concat_unemployment_df.drop(['periodName', 'month_number'], axis=1, inpace = True)
	concat_unemployment_df.sort_values(['area_text', 'year', 'month_number_pad'], inplace=True)
	concat_unemployment_df.reset_index(drop=True,inplace=True)
	concat_unemployment_df.to_csv(Path(write_path, 'monthly_state_unemployment.csv'), sep='|', header=True, index=False)