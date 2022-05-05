import pandas as pd
from sqlalchemy import create_engine
import pymysql
import urllib
import mysql.connector
from mysql.connector import Error
import numpy as np

pd.set_option('display.max_columns', None)

def parse_season(year):
    paris = parse_tour(1, 'Paris', year)
    tireno = parse_tour(2, 'Tireno', year)
    milan = parse_one_day(3, 'Milan_San_Remo', year)
    if year != 2020:
        catalunya = parse_tour(4, 'Catalunya', year)
    flanders = parse_one_day(5, 'Flanders', year)
    if year != 2020:
        basque = parse_tour(6, 'Basque', year)
        roubaix = parse_one_day(7, 'Roubaix', year)
    liege = parse_one_day(8, 'Liege', year)
    if year != 2020:
        romandie = parse_tour(9, 'Romandie', year)
    giro = parse_tour(10, 'Giro', year)
    dauphine = parse_tour(11, 'Dauphine', year)
    if year != 2020:
        suisse = parse_tour(12, 'Suisse', year)
    tdf = parse_tour(13, 'TDF', year)
    vuelta = parse_tour(14, 'Vuelta', year)
    lombardy = parse_one_day(15, 'Il_Lombardia', year)

    merge1 = pd.merge(paris, tireno, how = "outer", on=["First_Name", "Last_Name"], suffixes = ('', '_Tireno'))
    merge2 = pd.merge(merge1, milan, how = "outer", on=["First_Name", "Last_Name"], suffixes = ('', '_Milan'))
    merge3 = pd.merge(merge2, flanders, how = "outer", on=["First_Name", "Last_Name"], suffixes = ('', '_Flanders'))
    merge4 = pd.merge(merge3, liege, how = "outer", on=["First_Name", "Last_Name"], suffixes = ('', '_Liege'))
    merge5 = pd.merge(merge4, giro, how = "outer", on=["First_Name", "Last_Name"], suffixes = ('', '_Giro'))
    merge6 = pd.merge(merge5, dauphine, how = "outer", on=["First_Name", "Last_Name"], suffixes = ('', '_Dauphine'))
    merge7 = pd.merge(merge6, tdf, how = "outer", on=["First_Name", "Last_Name"], suffixes = ('', '_TDF'))
    merge8 = pd.merge(merge7, vuelta, how = "outer", on=["First_Name", "Last_Name"], suffixes = ('', '_Vuelta'))
    merge9 = pd.merge(merge8, lombardy, how = "outer", on=["First_Name", "Last_Name"], suffixes = ('', '_Lombardy'))

    if year != 2020:
        merge10 = pd.merge(merge9, catalunya, how="outer", on=["First_Name", "Last_Name"], suffixes=('', '_Catalunya'))
        merge11 = pd.merge(merge10, basque, how="outer", on=["First_Name", "Last_Name"], suffixes=('', '_Basque'))
        merge12 = pd.merge(merge11, roubaix, how="outer", on=["First_Name", "Last_Name"], suffixes=('', '_Roubaix'))
        merge13 = pd.merge(merge12, romandie, how="outer", on=["First_Name", "Last_Name"], suffixes=('', '_Romandie'))
        merge14 = pd.merge(merge13, suisse, how="outer", on=["First_Name", "Last_Name"], suffixes=('', '_Suisse'))
        final = merge14.copy()
        final['Age'] = final[['Age', 'Age_Tireno', 'Age_Milan', 'Age_Catalunya', 'Age_Flanders', 'Age_Basque', 'Age_Roubaix',
                              'Age_Liege', 'Age_Romandie', 'Age_Giro', 'Age_Dauphine', 'Age_Suisse', 'Age_TDF', 'Age_Vuelta',
                              'Age_Lombardy']].bfill(axis=1).iloc[:, 0]
        final['Team'] = final[['Team', 'Team_Tireno', 'Team_Milan', 'Team_Catalunya', 'Team_Flanders', 'Team_Basque',
                               'Team_Roubaix', 'Team_Liege', 'Team_Romandie', 'Team_Giro', 'Team_Dauphine', 'Team_Suisse',
                               'Team_TDF', 'Team_Vuelta', 'Team_Lombardy']].bfill(axis=1).iloc[:, 0]
        final = final.drop(
            ['Age_Tireno', 'Age_Milan', 'Age_Catalunya', 'Age_Flanders', 'Age_Basque', 'Age_Roubaix', 'Age_Liege',
             'Age_Romandie', 'Age_Giro', 'Age_Dauphine', 'Age_Suisse', 'Age_TDF', 'Age_Vuelta', 'Age_Lombardy'], axis=1)
        final = final.drop(
            ['Team_Tireno', 'Team_Milan', 'Team_Catalunya', 'Team_Flanders', 'Team_Basque', 'Team_Roubaix', 'Team_Liege',
             'Team_Romandie', 'Team_Giro', 'Team_Dauphine', 'Team_Suisse', 'Team_TDF', 'Team_Vuelta', 'Team_Lombardy'], axis=1)
    else:
        final = merge9.copy()
        final['Age'] = final[['Age', 'Age_Tireno', 'Age_Milan', 'Age_Flanders', 'Age_Liege', 'Age_Giro', 'Age_Dauphine', 'Age_TDF', 'Age_Vuelta', 'Age_Lombardy']].bfill(axis=1).iloc[:, 0]
        final['Team'] = final[['Team', 'Team_Tireno', 'Team_Milan', 'Team_Flanders', 'Team_Liege', 'Team_Giro', 'Team_Dauphine', 'Team_TDF', 'Team_Vuelta', 'Team_Lombardy']].bfill(axis=1).iloc[:, 0]
        final = final.drop(['Age_Tireno', 'Age_Milan', 'Age_Flanders', 'Age_Liege', 'Age_Giro', 'Age_Dauphine', 'Age_TDF', 'Age_Vuelta', 'Age_Lombardy'], axis=1)
        final = final.drop(['Team_Tireno', 'Team_Milan', 'Team_Flanders', 'Team_Liege','Team_Giro', 'Team_Dauphine', 'Team_TDF', 'Team_Vuelta', 'Team_Lombardy'], axis=1)

        final['Overall_Rank_Catalunya'] = np.nan
        final['Time_Dif_Catalunya'] = np.nan
        final['Points_Rank_Catalunya'] = np.nan
        final['Points_Catalunya'] = np.nan
        final['Mountains_Rank_Catalunya'] = np.nan
        final['Mountains_Points_Catalunya'] = np.nan

        final['Overall_Rank_Basque'] = np.nan
        final['Time_Dif_Basque'] = np.nan
        final['Points_Rank_Basque'] = np.nan
        final['Points_Basque'] = np.nan
        final['Mountains_Rank_Basque'] = np.nan
        final['Mountains_Points_Basque'] = np.nan

        final['Overall_Rank_Roubaix'] = np.nan
        final['Time_Dif_Roubaix'] = np.nan

        final['Overall_Rank_Romandie'] = np.nan
        final['Time_Dif_Romandie'] = np.nan
        final['Points_Rank_Romandie'] = np.nan
        final['Points_Romandie'] = np.nan
        final['Mountains_Rank_Romandie'] = np.nan
        final['Mountains_Points_Romandie'] = np.nan

        final['Overall_Rank_Suisse'] = np.nan
        final['Time_Dif_Suisse'] = np.nan
        final['Points_Rank_Suisse'] = np.nan
        final['Points_Suisse'] = np.nan
        final['Mountains_Rank_Suisse'] = np.nan
        final['Mountains_Points_Suisse'] = np.nan

    final = final.rename(columns={"Overall_Rank":"Overall_Rank_Paris", "Time_Dif":"Time_Dif_Paris", "Points_Rank":"Points_Rank_Paris", "Points":"Points_Paris", "Mountains_Rank":"Mountains_Rank_Paris", "Mountains_Points":"Mountains_Points_Paris"})

    final = final.sort_values(by = ['Last_Name', 'First_Name'])
    final['Season'] = year
    final.reset_index(inplace = True, drop = True)
    print(final.shape)
    #print(final.head())

    sqlEngine = create_engine('mysql+pymysql://root:k2udaeV%40@localhost:3306/cycling', pool_recycle=3600)
    connection = sqlEngine.connect()

    try:
        frame = final.to_sql(f"season_{year}", connection, if_exists='fail');

    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print(f"Table season_{year} created successfully.");
    finally:
        connection.close()


def parse_one_day(race_no, race_name, year):
    file_name = f'/Users/USER/Documents/Cycling/Data/{year}/{race_no}_{race_name}.xlsx'
    results = pd.read_excel(file_name)
    results['First_Name'] = results['First Name']
    results['Last_Name'] = results['Last Name']
    results['Overall_Rank'] = results['Rank']
    results['Time_Dif'] = results['Result']
    results = results[['First_Name', 'Last_Name', 'Age', 'Team', 'Overall_Rank', 'Time_Dif']]
    results['Overall_Rank'] = results['Overall_Rank'].fillna(999)
    return results

def parse_tour(race_no, race_name, year):
    file_name = f'/Users/USER/Documents/Cycling/Data/{year}/{race_no}_1_{race_name}_Start_list.xlsx'
    start = pd.read_excel(file_name)
    start['First_Name'] = start['First Name']
    start['Last_Name'] = start['Last Name']
    start = start[['First_Name', 'Last_Name', 'Age', 'Team']]

    file_name = f'/Users/USER/Documents/Cycling/Data/{year}/{race_no}_2_{race_name}_Overall.xlsx'
    overall = pd.read_excel(file_name)
    overall['Overall_Rank'] = overall['Rank']
    overall['Time_Dif'] = overall['Result']
    overall['First_Name'] = overall['First Name']
    overall['Last_Name'] = overall['Last Name']
    overall = overall[['First_Name', 'Last_Name', 'Overall_Rank', 'Time_Dif']]

    if race_no != 4 or year not in (2017, 2018):
        file_name = f'/Users/USER/Documents/Cycling/Data/{year}/{race_no}_3_{race_name}_Points.xlsx'
        points = pd.read_excel(file_name)
        points['Points_Rank'] = points['Rank']
        points['Points'] = points['Result']
        points['First_Name'] = points['First Name']
        points['Last_Name'] = points['Last Name']
        points = points[['First_Name', 'Last_Name', 'Points_Rank', 'Points']]

    file_name = f'/Users/USER/Documents/Cycling/Data/{year}/{race_no}_4_{race_name}_Mountains.xlsx'
    mountains = pd.read_excel(file_name)
    mountains['First_Name'] = mountains['First Name']
    mountains['Last_Name'] = mountains['Last Name']
    mountains['Mountains_Rank'] = mountains['Rank']
    mountains['Mountains_Points'] = mountains['Result']
    mountains = mountains[['First_Name', 'Last_Name', 'Mountains_Rank', 'Mountains_Points']]


    inter1 = pd.merge(start, overall, how = "left", on=["First_Name", "Last_Name"])
    if race_no != 4 or year not in (2017, 2018):
        inter2 = pd.merge(inter1, points, how = "left", on=["First_Name", "Last_Name"])
        final = pd.merge(inter2, mountains, how = "left", on=["First_Name", "Last_Name"])
    else:
        final = pd.merge(inter1, mountains, how="left", on=["First_Name", "Last_Name"])
        final['Points_Rank'] = np.nan
        final['Points'] = np.nan

    final['Overall_Rank'] = final['Overall_Rank'].fillna(999)

    return final

#parse_season(2009)
parse_season(2010)
parse_season(2011)
parse_season(2012)
parse_season(2013)
parse_season(2014)
parse_season(2015)
parse_season(2016)
parse_season(2017)
parse_season(2018)
parse_season(2019)
parse_season(2020)
parse_season(2021)



