import pandas as pd
import random
import time


#profootball reference tracks player stats from 1932 till present
def get_passing (num1, num2, eng):
    i = 0
    seasons =  [str(season) for season in range(num1, num2)]

    passing_table = pd.DataFrame()
    for season in seasons:
        url = "https://www.pro-football-reference.com/years/" + season + "/passing.htm"
        print(url)
        passing_season = pd.read_html(url, header=0, attrs={'id': 'passing'})[0]
        print("Table Scraped")
        passing_season = basic_clean(passing_season, season)
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

def get_rushing (num1, num2, eng):
    i = 0
    seasons =  [str(season) for season in range(num1, num2)]

    rushing_table = pd.DataFrame()
    for season in seasons:
        url = "https://www.pro-football-reference.com/years/" + season + "/rushing.htm"
        print(url)
        rushing_season = pd.read_html(url, header=1, attrs={'id': 'rushing'})[0]
        print("Table Scraped")
        rushing_season = basic_clean(rushing_season, season)
        if int(season) < 1945:
            rushing_season.insert(14, 'Fmb', 'Not Tracked')
        if int(season) < 1994:
            rushing_season.insert(10, '1D', 'Not Tracked')
            rushing_season.insert(11, 'Succ%', 'Not Tracked')
        print("Table Cleaned")
        print(rushing_season)
        rushing_table = pd.concat([rushing_table, rushing_season], ignore_index=True)
        i += 1
        print(str(round((i / len(seasons) * 100), 1)) + "% Complete\n")
        time.sleep(random.randint(3, 4))
    print(rushing_table)

    rushing_table.to_sql('Rushing_Stats', eng, if_exists='replace', index=False)

def get_receiving (num1, num2, eng):
    i = 0
    seasons =  [str(season) for season in range(num1, num2)]

    receiving_table = pd.DataFrame()
    for season in seasons:
        url = "https://www.pro-football-reference.com/years/" + season + "/receiving.htm"
        print(url)
        receiving_season = pd.read_html(url, header=1, attrs={'id': 'receiving'})[0]
        print("Table Scraped")
        receiving_season = basic_clean(receiving_season, season)
        if int(season) < 1945:
            receiving_season.insert(14, 'Fmb', 'Not Tracked')
        if int(season) < 1992:
            receiving_season.insert(7, 'Tgt', 'Not Tracked')
            receiving_season.insert(15, 'Ctch%', 'Not Tracked')
            receiving_season.insert(16, 'Y/Tgt', 'Not Tracked')
        if int(season) < 1994:
            receiving_season.insert(12, '1D', 'Not Tracked')
            receiving_season.insert(13, 'Succ%', 'Not Tracked')
        print("Table Cleaned")
        print(receiving_season)
        receiving_table = pd.concat([receiving_table, receiving_season], ignore_index=True)
        i += 1
        print(str(round((i / len(seasons) * 100), 1)) + "% Complete\n")
        time.sleep(random.randint(3, 4))
    print(receiving_table)

    receiving_table.to_sql('Receiving_Stats', eng, if_exists='replace', index=False)

def get_all (num1, num2, eng):
    get_passing(num1, num2, eng)
    get_receiving(num1, num2, eng)
    get_rushing(num1, num2, eng)

def basic_clean(stat_season, season):
    stat_season['Pos'] = stat_season['Pos'].fillna('ATH')
    stat_season = stat_season.fillna(0)
    stat_season = stat_season.drop(stat_season.columns[[0, stat_season.shape[1] - 1]], axis=1)
    stat_season.insert(0, 'Season', season)
    stat_season['Player'] = stat_season['Player'].str.replace("'", "''")
    stat_season['Player'] = stat_season['Player'].str.replace("*", "")
    stat_season['Player'] = stat_season['Player'].str.replace("+", "")
    mask = stat_season[stat_season.columns[1]] == "League Average"
    stat_season = stat_season[~mask]
    return stat_season

def column_diff(stat_season, prev_column_number, prev_columns, season, output):
    if len(stat_season.columns) != prev_column_number:
        x = 0
        prev_column_number = stat_season.shape[1]
        if len(prev_columns) < prev_column_number:
            for column in stat_season.columns:
                if column not in prev_columns:
                    output.append(season + ' ' + column + ' ' + str(x))
                x = x + 1
        if len(prev_columns) > prev_column_number:
            for column in prev_columns:
                if column not in stat_season.columns:
                    output.append(season + ' -' + column + ' ' + str(x))
                x = x + 1
        prev_column_number = len(prev_columns)
    return output