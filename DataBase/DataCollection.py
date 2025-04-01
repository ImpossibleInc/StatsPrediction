import pandas as pd
import random
import time


#profootball reference tracks player stats from 1932 till present
def get_passing (num1, num2, eng):
    i = 0
    seasons =  [str(season) for season in range(num1, num2+1)]

    passing_table = pd.DataFrame()
    for season in seasons:
        url = "https://www.pro-football-reference.com/years/" + season + "/passing.htm"
        print(url)
        passing_season = pd.read_html(url, header=0, attrs={'id': 'passing'})[0]
        print("Table Scraped")
        passing_season['Pos'] = passing_season['Pos'].fillna('ATH')
        passing_season = passing_season.fillna(0)
        passing_season = passing_season.drop(passing_season.columns[[0,passing_season.shape[1]-1]] , axis=1)
        passing_season.insert(0, 'Season', season)
        passing_season['Player'] = passing_season['Player'].str.replace("'", "''")
        passing_season['Player'] = passing_season['Player'].str.replace("*", "")
        passing_season['Player'] = passing_season['Player'].str.replace("+", "")
        mask = passing_season[passing_season.columns[1]] == "League Average"
        passing_season = passing_season[~mask]
        if int(season) < 1947:
            passing_season.insert(21, 'Yds.1', 'Not Tracked')
        if int(season) < 1950:
            passing_season.insert(7, 'QBrec', 'Not Tracked')
        if int(season) < 1960:
            passing_season.insert(22, 'Sk', 'Not Tracked')
            passing_season.insert(24, 'Sk%', 'Not Tracked')
            passing_season.insert(25, 'NY/A', 'Not Tracked')
            passing_season.insert(26, 'ANY/A', 'Not Tracked')
        if int(season) < 1994:
            passing_season.insert(16, '1D', 'Not Tracked')
            passing_season.insert(17, 'Succ%', 'Not Tracked')
        if int(season) < 2006 and int(season) != 2003:
            passing_season.insert(24, 'QBR', 'Not Tracked')
        print("Table Cleaned")
        print(passing_season)
        passing_table = pd.concat([passing_table, passing_season], ignore_index=True)
        i += 1
        print(str(round((i / len(seasons) * 100), 1)) + "% Complete\n")
        time.sleep(random.randint(3, 4))
    print(passing_table)

    passing_table.to_sql('Passing_Stats', eng, if_exists='replace', index=False)