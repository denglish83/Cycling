from sqlalchemy import create_engine
import pymysql
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


sqlEngine = create_engine('mysql+pymysql://root:k2udaeV%40@localhost:3306/cycling', pool_recycle=3600)
connection = sqlEngine.connect()


# Read in all sets

seasons = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]
for season in seasons:
    query = f"SELECT * FROM cycling.season_{season}_model_input"
    temp = pd.read_sql(query, connection)
    if season == 2014:
        model_input = temp.copy()
    else:
        model_input = pd.concat([model_input, temp])
    print(model_input.shape)

#for column in model_input.columns:
#    print(column)

# Remove races after the tour
model_input["team_tour_wins"] = model_input["team_tour_wins"].sub(model_input["tdf_wins_team"].fillna(0))
model_input["team_tour_wins"] = model_input["team_tour_wins"].sub(model_input["vuelta_wins_team"].fillna(0))
model_input["team_tour_podiums"] = model_input["team_tour_podiums"].sub(model_input["tdf_podium_team"].fillna(0))
model_input["team_tour_podiums"] = model_input["team_tour_podiums"].sub(model_input["vuelta_podium_team"].fillna(0))
model_input["team_tour_top_tens"] = model_input["team_tour_top_tens"].sub(model_input["tdf_top_ten_team"].fillna(0))
model_input["team_tour_top_tens"] = model_input["team_tour_top_tens"].sub(model_input["vuelta_top_ten_team"].fillna(0))
model_input["team_tour_completed"] = model_input["team_tour_completed"].sub(model_input["tdf_completed_team"].fillna(0))
model_input["team_tour_completed"] = model_input["team_tour_completed"].sub(model_input["vuelta_completed_team"].fillna(0))
model_input["team_tour_started"] = model_input["team_tour_started"].sub(model_input["tdf_started_team"].fillna(0))
model_input["team_tour_started"] = model_input["team_tour_started"].sub(model_input["vuelta_started_team"].fillna(0))

model_input["team_1_day_wins"] = model_input["team_1_day_wins"].sub(model_input["lombardy_wins_team"].fillna(0))
model_input["team_1_day_podiums"] = model_input["team_1_day_podiums"].sub(model_input["lombardy_podium_team"].fillna(0))
model_input["team_1_day_top_tens"] = model_input["team_1_day_top_tens"].sub(model_input["lombardy_top_ten_team"].fillna(0))
model_input["team_1_day_completed"] = model_input["team_1_day_completed"].sub(model_input["lombardy_completed_team"].fillna(0))
model_input["team_1_day_started"] = model_input["team_1_day_started"].sub(model_input["lombardy_started_team"].fillna(0))

model_input["career_tour_wins"] = model_input["career_tour_wins"].sub(model_input["tdf_wins"].fillna(0))
model_input["career_tour_podiums"] = model_input["career_tour_podiums"].sub(model_input["tdf_podium"].fillna(0))
model_input["career_tour_top_tens"] = model_input["career_tour_top_tens"].sub(model_input["tdf_top_ten"].fillna(0))
model_input["career_tour_completed"] = model_input["career_tour_completed"].sub(model_input["tdf_completed"].fillna(0))
model_input["career_tour_started"] = model_input["career_tour_started"].sub(model_input["tdf_started"].fillna(0))



# Drop all extra vars

model_input = model_input.drop(["Overall_Rank_Vuelta", "Overall_Rank_Lombardy", "Team",
                                "paris_wins_team", "paris_podium_team", "paris_top_ten_team", "paris_completed_team", "paris_started_team",
                                "tireno_wins_team", "tireno_podium_team", "tireno_top_ten_team", "tireno_completed_team", "tireno_started_team",
                                "milan_wins_team", "milan_podium_team", "milan_top_ten_team", "milan_completed_team", "milan_started_team",
                                "catalunya_wins_team", "catalunya_podium_team", "catalunya_top_ten_team", "catalunya_completed_team", "catalunya_started_team",
                                "flanders_wins_team", "flanders_podium_team", "flanders_top_ten_team", "flanders_completed_team", "flanders_started_team",
                                "basque_wins_team", "basque_podium_team", "basque_top_ten_team", "basque_completed_team", "basque_started_team",
                                "roubaix_wins_team", "roubaix_podium_team", "roubaix_top_ten_team", "roubaix_completed_team", "roubaix_started_team",
                                "liege_wins_team", "liege_podium_team", "liege_top_ten_team", "liege_completed_team", "liege_started_team",
                                "romandie_wins_team", "romandie_podium_team", "romandie_top_ten_team", "romandie_completed_team", "romandie_started_team",
                                "giro_wins_team", "giro_podium_team", "giro_top_ten_team", "giro_completed_team", "giro_started_team",
                                "dauphine_wins_team", "dauphine_podium_team", "dauphine_top_ten_team", "dauphine_completed_team", "dauphine_started_team",
                                "suisse_wins_team", "suisse_podium_team", "suisse_top_ten_team", "suisse_completed_team", "suisse_started_team",
                                "tdf_wins_team", "tdf_podium_team", "tdf_top_ten_team", "tdf_completed_team", "tdf_started_team",
                                "vuelta_wins_team", "vuelta_podium_team", "vuelta_top_ten_team", "vuelta_completed_team", "vuelta_started_team",
                                "lombardy_wins_team", "lombardy_podium_team", "lombardy_top_ten_team", "lombardy_completed_team", "lombardy_started_team",
                                'milan_wins', 'flanders_wins', 'roubaix_wins', 'liege_wins', "lombardy_wins",
                                'milan_podium', 'flanders_podium', 'roubaix_podium', 'liege_podium', "lombardy_podium",
                                'milan_top_ten', 'flanders_top_ten', 'roubaix_top_ten', 'liege_top_ten',
                                "lombardy_top_ten",
                                'milan_completed', 'flanders_completed', 'roubaix_completed', 'liege_completed',
                                "lombardy_completed",
                                'milan_started', 'flanders_started', 'roubaix_started', 'liege_started',
                                "lombardy_started",
                                'paris_wins', 'tireno_wins', 'catalunya_wins', 'basque_wins', "romandie_wins",
                                "dauphine_wins", "suisse_wins",
                                'paris_podium', 'tireno_podium', 'catalunya_podium', 'basque_podium', "romandie_podium",
                                "dauphine_podium", "suisse_podium",
                                'paris_top_ten', 'tireno_top_ten', 'catalunya_top_ten', 'basque_top_ten',
                                "romandie_top_ten", "dauphine_top_ten", "suisse_top_ten",
                                'paris_completed', 'tireno_completed', 'catalunya_completed', 'basque_completed',
                                "romandie_completed", "dauphine_completed", "suisse_completed",
                                'paris_started', 'tireno_started', 'catalunya_started', 'basque_started',
                                "romandie_started", "dauphine_started", "suisse_started",
                                'vuelta_wins', 'giro_wins',
                                'vuelta_podium', 'giro_podium',
                                'vuelta_top_ten', 'giro_top_ten',
                                'vuelta_completed', 'giro_completed',
                                'vuelta_started', 'giro_started',
                                'level_0', 'index'
                                ],
                               axis = 1)

#for column in model_input.columns:
#    print(column)
#print(model_input.head())

# One-hot encode some dummies

dummies = pd.get_dummies(model_input[["Age"]], drop_first=True)
model_input = pd.concat([model_input, dummies], axis=1)

# Drop riders who didnt ride the tour

model_input = model_input.loc[model_input['Overall_Rank_TDF'].isna() == False]

# Null Handeling
model_input["Overall_Rank_Paris"] = model_input["Overall_Rank_Paris"].fillna(9999) # Note: 999 means started but didnt finish and we want to distinguish that from never starting at all
model_input["Overall_Rank_Tireno"] = model_input["Overall_Rank_Tireno"].fillna(9999)
model_input["Overall_Rank_Milan"] = model_input["Overall_Rank_Milan"].fillna(9999)
model_input["Overall_Rank_Flanders"] = model_input["Overall_Rank_Flanders"].fillna(9999)
model_input["Overall_Rank_Liege"] = model_input["Overall_Rank_Liege"].fillna(9999)
model_input["Overall_Rank_Giro"] = model_input["Overall_Rank_Giro"].fillna(9999)
model_input["Overall_Rank_Dauphine"] = model_input["Overall_Rank_Dauphine"].fillna(9999)
model_input["Overall_Rank_Catalunya"] = model_input["Overall_Rank_Catalunya"].fillna(9999)
model_input["Overall_Rank_Basque"] = model_input["Overall_Rank_Basque"].fillna(9999)
model_input["Overall_Rank_Roubaix"] = model_input["Overall_Rank_Roubaix"].fillna(9999)
model_input["Overall_Rank_Romandie"] = model_input["Overall_Rank_Romandie"].fillna(9999)
model_input["Overall_Rank_Suisse"] = model_input["Overall_Rank_Suisse"].fillna(9999)

model_input["Overall_Rank_Paris_prior"]  = model_input["Overall_Rank_Paris_prior"].fillna(9999)
model_input["Overall_Rank_Tireno_prior"] = model_input["Overall_Rank_Tireno_prior"].fillna(9999)
model_input["Overall_Rank_Milan_prior"] = model_input["Overall_Rank_Milan_prior"].fillna(9999)
model_input["Overall_Rank_Flanders_prior"] = model_input["Overall_Rank_Flanders_prior"].fillna(9999)
model_input["Overall_Rank_Liege_prior"] = model_input["Overall_Rank_Liege_prior"].fillna(9999)
model_input["Overall_Rank_Giro_prior"] = model_input["Overall_Rank_Giro_prior"].fillna(9999)
model_input["Overall_Rank_Dauphine_prior"] = model_input["Overall_Rank_Dauphine_prior"].fillna(9999)
model_input["Overall_Rank_Catalunya_prior"] = model_input["Overall_Rank_Catalunya_prior"].fillna(9999)
model_input["Overall_Rank_Basque_prior"] = model_input["Overall_Rank_Basque_prior"].fillna(9999)
model_input["Overall_Rank_Roubaix_prior"] = model_input["Overall_Rank_Roubaix_prior"].fillna(9999)
model_input["Overall_Rank_Romandie_prior"] = model_input["Overall_Rank_Romandie_prior"].fillna(9999)
model_input["Overall_Rank_Suisse_prior"] = model_input["Overall_Rank_Suisse_prior"].fillna(9999)
model_input["Overall_Rank_TDF_prior"] = model_input["Overall_Rank_TDF_prior"].fillna(9999)
model_input["Overall_Rank_Vuelta_prior"] = model_input["Overall_Rank_Vuelta_prior"].fillna(9999)
model_input["Overall_Rank_Lombardy_prior"] = model_input["Overall_Rank_Lombardy_prior"].fillna(9999)

model_input['tdf_started'] = model_input['tdf_started'].fillna(0)
model_input['tdf_completed'] = model_input['tdf_completed'].fillna(0)
model_input['tdf_top_ten'] = model_input['tdf_top_ten'].fillna(0)
model_input['tdf_podium'] = model_input['tdf_podium'].fillna(0)
model_input['tdf_wins'] = model_input['tdf_wins'].fillna(0)


mask1 = model_input['Overall_Rank_Paris_prior'] < 9999
mask2 = model_input['num_season'].isna()

model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Tireno_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Milan_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Flanders_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Liege_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Giro_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Dauphine_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Catalunya_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Basque_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Roubaix_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Romandie_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Suisse_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_TDF_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Vuelta_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

mask1 = model_input['Overall_Rank_Lombardy_prior'] < 9999
model_input.loc[mask1 & mask2, 'num_season'] = 2

model_input['num_season'] = model_input['num_season'].fillna(1)



print(model_input[model_input.isna().any(axis=1)].head())
print(model_input.shape)


# Train/Test Split

test = model_input.loc[model_input['Season'] >= 2020]
test_y = test["Overall_Rank_TDF"]
test_x = test.drop(["Overall_Rank_TDF", "First_Name", "Last_Name", "Age"], axis = 1)

train = model_input.loc[model_input['Season'] < 2020]
train_y = train["Overall_Rank_TDF"]
train_x = train.drop(["Overall_Rank_TDF", "First_Name", "Last_Name", "Age"], axis = 1)


# Model Fit

rf = RandomForestRegressor(n_estimators = 50, random_state = 42, max_depth=6, max_features= 2)
rf.fit(train_x, train_y)

# Test
pred = pd.DataFrame(rf.predict(test_x))

compare = test.copy()
compare["model_pred"] = pred[0]

compare_2020 = compare.loc[compare['Season'] == 2020]
print("2020 Results:")
compare_2020 = compare_2020.sort_values(by = ["Overall_Rank_TDF"])
print(compare_2020[["First_Name", "Last_Name", "Overall_Rank_TDF", "model_pred"]].head(10))
print("2020 Prediciton:")
compare_2020 = compare_2020.sort_values(by = ["model_pred"])
print(compare_2020[["First_Name", "Last_Name", "Overall_Rank_TDF", "model_pred"]].head(10))


compare_2021 = compare.loc[compare['Season'] == 2021]
print("2021 Results:")
compare_2021 = compare_2021.sort_values(by = ["Overall_Rank_TDF"])
print(compare_2021[["First_Name", "Last_Name", "Overall_Rank_TDF", "model_pred"]].head(10))
print("2021 Prediciton:")
compare_2021 = compare_2021.sort_values(by = ["model_pred"])
print(compare_2021[["First_Name", "Last_Name", "Overall_Rank_TDF", "model_pred"]].head(10))
