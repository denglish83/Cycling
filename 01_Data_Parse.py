import pandas as pd

def parse_season(year):
    parse_tour(1, 'Paris', year)
    parse_tour(2, 'Tireno', year)
    parse_one_day(3, 'Milan_San_Remo', year)

def parse_one_day(race_no, race_name, year):
    file_name = f'/Users/USER/Documents/Cycling/Data/{year}/{race_no}_{race_name}.xlsx'
    print(file_name)
    results = pd.read_excel(file_name)
    print(results.head())
    results.drop()
    #return results

def parse_tour(race_no, race_name, year):
    file_name = f'/Users/USER/Documents/Cycling/Data/{year}/{race_no}_1_{race_name}_Start_list.xlsx'
    print(file_name)
    results = pd.read_excel(file_name)


parse_season(2020)