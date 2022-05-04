import pandas as pd

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

    merge1 = pd.merge(paris, tireno, how = "outer", on=["First Name", "Last Name"], suffixes = ('', 'Tireno'))
    merge2 = pd.merge(merge1, milan, how = "outer", on=["First Name", "Last Name"], suffixes = ('', 'Milan'))
    merge3 = pd.merge(merge2, flanders, how = "outer", on=["First Name", "Last Name"], suffixes = ('', 'Flanders'))
    merge4 = pd.merge(merge3, liege, how = "outer", on=["First Name", "Last Name"], suffixes = ('', 'Liege'))
    merge5 = pd.merge(merge4, giro, how = "outer", on=["First Name", "Last Name"], suffixes = ('', 'Giro'))
    merge6 = pd.merge(merge5, dauphine, how = "outer", on=["First Name", "Last Name"], suffixes = ('', 'Dauphine'))
    merge7 = pd.merge(merge6, tdf, how = "outer", on=["First Name", "Last Name"], suffixes = ('', 'TDF'))
    merge8 = pd.merge(merge7, vuelta, how = "outer", on=["First Name", "Last Name"], suffixes = ('', 'Vuelta'))
    merge9 = pd.merge(merge8, lombardy, how = "outer", on=["First Name", "Last Name"], suffixes = ('', 'Lombardy'))

    if year != 2020:
        merge10 = pd.merge(merge9, catalunya, how="outer", on=["First Name", "Last Name"], suffixes=('', 'Catalunya'))
        merge11 = pd.merge(merge10, basque, how="outer", on=["First Name", "Last Name"], suffixes=('', 'Basque'))
        merge12 = pd.merge(merge11, roubaix, how="outer", on=["First Name", "Last Name"], suffixes=('', 'Roubaix'))
        merge13 = pd.merge(merge12, romandie, how="outer", on=["First Name", "Last Name"], suffixes=('', 'Romandie'))
        merge14 = pd.merge(merge13, suisse, how="outer", on=["First Name", "Last Name"], suffixes=('', 'Suisse'))
        final = merge14.copy()
        final['Age'] = final[['Age', 'AgeTireno', 'AgeMilan', 'AgeCatalunya', 'AgeFlanders', 'AgeBasque', 'AgeRoubaix',
                              'AgeLiege', 'AgeRomandie', 'AgeGiro', 'AgeDauphine', 'AgeSuisse', 'AgeTDF', 'AgeVuelta',
                              'AgeLombardy']].bfill(axis=1).iloc[:, 0]
        final['Team'] = final[['Team', 'TeamTireno', 'TeamMilan', 'TeamCatalunya', 'TeamFlanders', 'TeamBasque',
                               'TeamRoubaix', 'TeamLiege', 'TeamRomandie', 'TeamGiro', 'TeamDauphine', 'TeamSuisse',
                               'TeamTDF', 'TeamVuelta', 'TeamLombardy']].bfill(axis=1).iloc[:, 0]
        final = final.drop(
            ['AgeTireno', 'AgeMilan', 'AgeCatalunya', 'AgeFlanders', 'AgeBasque', 'AgeRoubaix', 'AgeLiege',
             'AgeRomandie', 'AgeGiro', 'AgeDauphine', 'AgeSuisse', 'AgeTDF', 'AgeVuelta', 'AgeLombardy'], axis=1)
        final = final.drop(
            ['TeamTireno', 'TeamMilan', 'TeamCatalunya', 'TeamFlanders', 'TeamBasque', 'TeamRoubaix', 'TeamLiege',
             'TeamRomandie', 'TeamGiro', 'TeamDauphine', 'TeamSuisse', 'TeamTDF', 'TeamVuelta', 'TeamLombardy'], axis=1)
    else:
        final = merge9.copy()
        final['Age'] = final[['Age', 'AgeTireno', 'AgeMilan', 'AgeFlanders', 'AgeLiege', 'AgeGiro', 'AgeDauphine', 'AgeTDF', 'AgeVuelta', 'AgeLombardy']].bfill(axis=1).iloc[:, 0]
        final['Team'] = final[['Team', 'TeamTireno', 'TeamMilan', 'TeamFlanders', 'TeamLiege', 'TeamGiro', 'TeamDauphine', 'TeamTDF', 'TeamVuelta', 'TeamLombardy']].bfill(axis=1).iloc[:, 0]
        final = final.drop(['AgeTireno', 'AgeMilan', 'AgeFlanders', 'AgeLiege', 'AgeGiro', 'AgeDauphine', 'AgeTDF', 'AgeVuelta', 'AgeLombardy'], axis=1)
        final = final.drop(['TeamTireno', 'TeamMilan', 'TeamFlanders', 'TeamLiege','TeamGiro', 'TeamDauphine', 'TeamTDF', 'TeamVuelta', 'TeamLombardy'], axis=1)

    final = final.rename(columns={"Overall Rank":"Overall RankParis", "Time Dif":"Time DifParis", "Points Rank":"Points RankParis", "Points":"PointsParis", "Mountains Rank":"Mountains RankParis", "Mountains Points":"Mountains PointsParis"})
    final = final.sort_values(by = ['Last Name', 'First Name'])
    final['Season'] = year
    final.reset_index(inplace = True, drop = True)
    print(final.shape)
    print(final.head())

def parse_one_day(race_no, race_name, year):
    file_name = f'/Users/USER/Documents/Cycling/Data/{year}/{race_no}_{race_name}.xlsx'
    results = pd.read_excel(file_name)
    results = results[['First Name', 'Last Name', 'Age', 'Team', 'Rank', 'Result']]
    return results

def parse_tour(race_no, race_name, year):
    file_name = f'/Users/USER/Documents/Cycling/Data/{year}/{race_no}_1_{race_name}_Start_list.xlsx'
    start = pd.read_excel(file_name)
    start = start[['First Name', 'Last Name', 'Age', 'Team']]

    file_name = f'/Users/USER/Documents/Cycling/Data/{year}/{race_no}_2_{race_name}_Overall.xlsx'
    overall = pd.read_excel(file_name)
    overall['Overall Rank'] = overall['Rank']
    overall['Time Dif'] = overall['Result']
    overall = overall[['First Name', 'Last Name', 'Overall Rank', 'Time Dif']]

    if race_no != 4 or year not in (2017, 2018):
        file_name = f'/Users/USER/Documents/Cycling/Data/{year}/{race_no}_3_{race_name}_Points.xlsx'
        points = pd.read_excel(file_name)
        points['Points Rank'] = points['Rank']
        points['Points'] = points['Result']
        points = points[['First Name', 'Last Name', 'Points Rank', 'Points']]

    file_name = f'/Users/USER/Documents/Cycling/Data/{year}/{race_no}_4_{race_name}_Mountains.xlsx'
    mountains = pd.read_excel(file_name)
    mountains['Mountains Rank'] = mountains['Rank']
    mountains['Mountains Points'] = mountains['Result']
    mountains = mountains[['First Name', 'Last Name', 'Mountains Rank', 'Mountains Points']]


    inter1 = pd.merge(start, overall, how = "left", on=["First Name", "Last Name"])
    if race_no != 4 or year not in (2017, 2018):
        inter2 = pd.merge(inter1, points, how = "left", on=["First Name", "Last Name"])
        final = pd.merge(inter2, mountains, how = "left", on=["First Name", "Last Name"])
    else:
        final = pd.merge(inter1, mountains, how="left", on=["First Name", "Last Name"])

    return final

#parse_season(2015)
#parse_season(2016)
#parse_season(2017)
#parse_season(2018)
#parse_season(2019)
parse_season(2020)
#parse_season(2021)