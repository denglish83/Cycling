from sqlalchemy import create_engine
import pymysql
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


sqlEngine = create_engine('mysql+pymysql://root:k2udaeV%40@localhost:3306/cycling', pool_recycle=3600)
connection = sqlEngine.connect()

def parse_race(name):
    query_piece = f"SUM(CASE WHEN overall_rank_{name} = 1 THEN 1 ELSE 0 END) AS {name}_wins," \
                  f"SUM(CASE WHEN overall_rank_{name} <= 3 AND overall_rank_{name} IS NOT NULL THEN 1 ELSE 0 END) AS {name}_podium," \
                  f"SUM(CASE WHEN overall_rank_{name} <= 10 AND overall_rank_{name} IS NOT NULL THEN 1 ELSE 0 END) AS {name}_top_ten," \
                  f"SUM(CASE WHEN overall_rank_{name} < 999 AND overall_rank_{name} IS NOT NULL THEN 1 ELSE 0 END) AS {name}_completed," \
                  f"SUM(CASE WHEN overall_rank_{name} IS NOT NULL THEN 1 ELSE 0 END) AS {name}_started,"
    return query_piece

seasons = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]

for season in seasons:
    prior_season = season - 1
    career_start = season - 2

    query_begin = f"SELECT First_name, Last_name,"
    query_end = f"COUNT(*) AS num_season FROM cycling.all_seasons WHERE season <= {career_start} GROUP BY first_name, last_name ORDER BY first_name, last_name"
    races = ["paris", "tireno", "milan", "catalunya", "flanders", "basque", "roubaix", "liege", "romandie", "giro",
             "dauphine", "suisse", "tdf", "vuelta", "lombardy"]

    for race in races:
        piece = parse_race(race)
        query_begin += piece
        # print(query_begin)

    query = query_begin + query_end

    career = pd.read_sql(query, connection)

    query = f"SELECT * FROM cycling.all_seasons WHERE season = {prior_season}"
    prior = pd.read_sql(query, connection)

    query = f"SELECT * FROM cycling.all_seasons WHERE season = {season}"
    current = pd.read_sql(query, connection)

    query_begin = f"SELECT Team,"

    for race in races:
        piece = parse_race(race)
        query_begin += piece
        # print(query_begin)

    query_end = f"COUNT(*) AS num_races FROM cycling.all_seasons WHERE season = {season} GROUP BY Team ORDER BY Team"
    query = query_begin + query_end

    team = pd.read_sql(query, connection)


    merge1 = pd.merge(current, prior, how = "outer", on=["First_Name", "Last_Name"], suffixes = ('', '_prior'))
    merge2 = pd.merge(merge1, career, how = "outer", left_on=["First_Name", "Last_Name"], right_on=["First_name", "Last_name"], suffixes = ('', '_career'))
    final = pd.merge(merge2, team, how = "outer", left_on=["Team"], right_on=['Team'], suffixes = ('', '_team'))

    #print(final.head())
    #for column in final.columns:
        #print(column)

    final["career_1_day_wins"] = final.loc[:,['milan_wins','flanders_wins', 'roubaix_wins', 'liege_wins', "lombardy_wins"]].sum(axis = 1)
    final["career_1_day_podiums"] = final.loc[:,['milan_podium','flanders_podium', 'roubaix_podium', 'liege_podium', "lombardy_podium"]].sum(axis = 1)
    final["career_1_day_top_tens"] = final.loc[:,['milan_top_ten','flanders_top_ten', 'roubaix_top_ten', 'liege_top_ten', "lombardy_top_ten"]].sum(axis = 1)
    final["career_1_day_completed"] = final.loc[:,['milan_completed','flanders_completed', 'roubaix_completed', 'liege_completed', "lombardy_completed"]].sum(axis = 1)
    final["career_1_day_started"] = final.loc[:,['milan_started','flanders_started', 'roubaix_started', 'liege_started', "lombardy_started"]].sum(axis = 1)

    final["career_1_week_wins"] = final.loc[:,['paris_wins','tireno_wins', 'catalunya_wins', 'basque_wins', "romandie_wins", "dauphine_wins", "suisse_wins"]].sum(axis = 1)
    final["career_1_week_podiums"] = final.loc[:,['paris_podium','tireno_podium', 'catalunya_podium', 'basque_podium', "romandie_podium", "dauphine_podium", "suisse_podium"]].sum(axis = 1)
    final["career_1_week_top_tens"] = final.loc[:,['paris_top_ten','tireno_top_ten', 'catalunya_top_ten', 'basque_top_ten', "romandie_top_ten", "dauphine_top_ten", "suisse_top_ten"]].sum(axis = 1)
    final["career_1_week_completed"] = final.loc[:,['paris_completed','tireno_completed', 'catalunya_completed', 'basque_completed', "romandie_completed", "dauphine_completed", "suisse_completed"]].sum(axis = 1)
    final["career_1_week_started"] = final.loc[:,['paris_started','tireno_started', 'catalunya_started', 'basque_started', "romandie_started", "dauphine_started", "suisse_started"]].sum(axis = 1)

    final["career_tour_wins"] = final.loc[:,['tdf_wins','vuelta_wins', 'giro_wins']].sum(axis = 1)
    final["career_tour_podiums"] = final.loc[:,['tdf_podium','vuelta_podium', 'giro_podium']].sum(axis = 1)
    final["career_tour_top_tens"] = final.loc[:,['tdf_top_ten','vuelta_top_ten', 'giro_top_ten']].sum(axis = 1)
    final["career_tour_completed"] = final.loc[:,['tdf_completed','vuelta_completed', 'giro_completed']].sum(axis = 1)
    final["career_tour_started"] = final.loc[:,['tdf_started','vuelta_started', 'giro_started']].sum(axis = 1)

    final["team_1_day_wins"] = final.loc[:,['milan_wins_team','flanders_wins_team', 'roubaix_wins_team', 'liege_wins_team', "lombardy_wins_team"]].sum(axis = 1)
    final["team_1_day_podiums"] = final.loc[:,['milan_podium_team','flanders_podium_team', 'roubaix_podium_team', 'liege_podium_team', "lombardy_podium_team"]].sum(axis = 1)
    final["team_1_day_top_tens"] = final.loc[:,['milan_top_ten_team','flanders_top_ten_team', 'roubaix_top_ten_team', 'liege_top_ten_team', "lombardy_top_ten_team"]].sum(axis = 1)
    final["team_1_day_completed"] = final.loc[:,['milan_completed_team','flanders_completed_team', 'roubaix_completed_team', 'liege_completed_team', "lombardy_completed_team"]].sum(axis = 1)
    final["team_1_day_started"] = final.loc[:,['milan_started_team','flanders_started_team', 'roubaix_started_team', 'liege_started_team', "lombardy_started_team"]].sum(axis = 1)

    final["team_1_week_wins"] = final.loc[:,['paris_wins_team','tireno_wins_team', 'catalunya_wins_team', 'basque_wins_team', "romandie_wins_team", "dauphine_wins_team", "suisse_wins_team"]].sum(axis = 1)
    final["team_1_week_podiums"] = final.loc[:,['paris_podium_team','tireno_podium_team', 'catalunya_podium_team', 'basque_podium_team', "romandie_podium_team", "dauphine_podium_team", "suisse_podium_team"]].sum(axis = 1)
    final["team_1_week_top_tens"] = final.loc[:,['paris_top_ten_team','tireno_top_ten_team', 'catalunya_top_ten_team', 'basque_top_ten_team', "romandie_top_ten_team", "dauphine_top_ten_team", "suisse_top_ten_team"]].sum(axis = 1)
    final["team_1_week_completed"] = final.loc[:,['paris_completed_team','tireno_completed_team', 'catalunya_completed_team', 'basque_completed_team', "romandie_completed_team", "dauphine_completed_team", "suisse_completed_team"]].sum(axis = 1)
    final["team_1_week_started"] = final.loc[:,['paris_started_team','tireno_started_team', 'catalunya_started_team', 'basque_started_team', "romandie_started_team", "dauphine_started_team", "suisse_started_team"]].sum(axis = 1)

    final["team_tour_wins"] = final.loc[:,['tdf_wins_team','vuelta_wins_team', 'giro_wins_team']].sum(axis = 1)
    final["team_tour_podiums"] = final.loc[:,['tdf_podium_team','vuelta_podium_team', 'giro_podium_team']].sum(axis = 1)
    final["team_tour_top_tens"] = final.loc[:,['tdf_top_ten_team','vuelta_top_ten_team', 'giro_top_ten_team']].sum(axis = 1)
    final["team_tour_completed"] = final.loc[:,['tdf_completed_team','vuelta_completed_team', 'giro_completed_team']].sum(axis = 1)
    final["team_tour_started"] = final.loc[:,['tdf_started_team','vuelta_started_team', 'giro_started_team']].sum(axis = 1)


    final = final.drop(["Time_Dif_Paris","Points_Rank_Paris", "Points_Paris", "Mountains_Rank_Paris", "Mountains_Points_Paris",
                        "Time_Dif_Tireno", "Points_Rank_Tireno", "Points_Tireno", "Mountains_Rank_Tireno", "Mountains_Points_Tireno",
                        "Time_Dif_Milan", "Time_Dif_Flanders", "Time_Dif_Liege",
                        "Time_Dif_Giro", "Points_Rank_Giro", "Points_Giro", "Mountains_Rank_Giro", "Mountains_Points_Giro",
                        "Time_Dif_Dauphine", "Points_Rank_Dauphine", "Points_Dauphine", "Mountains_Rank_Dauphine", "Mountains_Points_Dauphine",
                        "Time_Dif_TDF", "Points_Rank_TDF", "Points_TDF", "Mountains_Rank_TDF", "Mountains_Points_TDF",
                        "Time_Dif_Vuelta","Points_Rank_Vuelta","Points_Vuelta","Mountains_Rank_Vuelta", "Mountains_Points_Vuelta",
                        "Time_Dif_Lombardy", "Time_Dif_Roubaix",
                        "Time_Dif_Catalunya", "Points_Rank_Catalunya", "Points_Catalunya", "Mountains_Rank_Catalunya", "Mountains_Points_Catalunya",
                        "Time_Dif_Basque", "Points_Rank_Basque", "Points_Basque", "Mountains_Rank_Basque", "Mountains_Points_Basque",
                        "Time_Dif_Romandie", "Points_Rank_Romandie", "Points_Romandie", "Mountains_Rank_Romandie", "Mountains_Points_Romandie",
                        "Time_Dif_Suisse", "Points_Rank_Suisse", "Points_Suisse", "Mountains_Rank_Suisse", "Mountains_Points_Suisse",
                        'index_prior', "Age_prior", "Team_prior",
                        "Time_Dif_Paris_prior","Points_Rank_Paris_prior", "Points_Paris_prior", "Mountains_Rank_Paris_prior", "Mountains_Points_Paris_prior",
                        "Time_Dif_Tireno_prior", "Points_Rank_Tireno_prior", "Points_Tireno_prior", "Mountains_Rank_Tireno_prior", "Mountains_Points_Tireno_prior",
                        "Time_Dif_Milan_prior", "Time_Dif_Flanders_prior", "Time_Dif_Liege_prior",
                        "Time_Dif_Giro_prior", "Points_Rank_Giro_prior", "Points_Giro_prior", "Mountains_Rank_Giro_prior", "Mountains_Points_Giro_prior",
                        "Time_Dif_Dauphine_prior", "Points_Rank_Dauphine_prior", "Points_Dauphine_prior", "Mountains_Rank_Dauphine_prior", "Mountains_Points_Dauphine_prior",
                        "Time_Dif_TDF_prior", "Points_Rank_TDF_prior", "Points_TDF_prior", "Mountains_Rank_TDF_prior", "Mountains_Points_TDF_prior",
                        "Time_Dif_Vuelta_prior","Points_Rank_Vuelta_prior","Points_Vuelta_prior","Mountains_Rank_Vuelta_prior", "Mountains_Points_Vuelta_prior",
                        "Time_Dif_Lombardy_prior", "Time_Dif_Roubaix_prior",
                        "Time_Dif_Catalunya_prior", "Points_Rank_Catalunya_prior", "Points_Catalunya_prior", "Mountains_Rank_Catalunya_prior", "Mountains_Points_Catalunya_prior",
                        "Time_Dif_Basque_prior", "Points_Rank_Basque_prior", "Points_Basque_prior", "Mountains_Rank_Basque_prior", "Mountains_Points_Basque_prior",
                        "Time_Dif_Romandie_prior", "Points_Rank_Romandie_prior", "Points_Romandie_prior", "Mountains_Rank_Romandie_prior", "Mountains_Points_Romandie_prior",
                        "Time_Dif_Suisse_prior", "Points_Rank_Suisse_prior", "Points_Suisse_prior", "Mountains_Rank_Suisse_prior", "Mountains_Points_Suisse_prior",
                        "Season_prior", "First_name", "Last_name"
                        ]
                        , axis = 1)

    try:
        frame = final.to_sql(f"season_{season}_model_input", connection, if_exists='fail');

    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print(f"Table season_{season} created successfully.");

connection.close()