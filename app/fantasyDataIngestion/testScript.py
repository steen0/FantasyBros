import pandas as pd


fProsUrl = "https://www.fantasypros.com/mlb/projections/ros-1b.php"

playersDf = pd.read_html(fProsUrl)[0]
playersDf["Team"] = playersDf["Player"].str.split("\(").str[1].str.split(" \-").str[0]
playersDf["Player"] = playersDf["Player"].str.split(" \(").str[0]

print(playersDf)

print(playersDf.shape)

playersDf = playersDf[playersDf["VBR"].notnull()]
playersDf = playersDf.drop(columns=["Unnamed: 17", "Unnamed: 18"], axis=1)

print(playersDf)
