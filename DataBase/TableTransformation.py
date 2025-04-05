import psycopg2
import sqlalchemy



def establish_connection():
    connection_params = {
        'dbname': 'postgres',
        'user': 'datascrape',
        'password': 'fill',
        'host': 'localhost',
        'port': 5432
    }

    engine = sqlalchemy.create_engine("postgresql://datascrape:fill@localhost:5432/postgres?options=-csearch_path%3Dstats")
    connection = psycopg2.connect(**connection_params)

    cursor = connection.cursor()
    print("Connection established!")
    return cursor, connection, engine

def close_connection(cursor, connection, engine):
    cursor.close()
    connection.close()
    engine.dispose()
    print("Connection closed!")

def beautify_passing(cursor, connection):
    cursor.execute("""
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "QBrec" TO "QB_Record";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Cmp" TO "Passing_Completions";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Att" TO "Passing_Attempts";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Cmp%" TO "Completion_Percentage";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Yds" TO "Passing_Yards";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "TD" TO "Passing_TDs";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "TD%" TO "Touchdown_Percentage";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Int" TO "Interceptions";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Int%" TO "Interception_Percentage";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "1D" TO "First_Downs";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Succ%" TO "Success_Percentage";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Lng" TO "Longest_Pass";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Y/A" TO "Yards_Per_Attempt";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "AY/A" TO "Adjusted_Yards_Per_Attempt";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Y/C" TO "Yards_Per_Completion";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Y/G" TO "Passing_Yards_Per_Game";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Rate" TO "Passer_Rating";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "QBR" TO "Quarterback_Rating";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Sk" TO "Sacks_Taken";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Yds.1" TO "Sack_Yards_Lost";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "Sk%" TO "Sack_Percentage";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "NY/A" TO "Net_Yards_Per_Attempt";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "ANY/A" TO "Adjusted_Net_Yards_Per_Attempt";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "4QC" TO "Fourth_Quarter_Comebacks";
    ALTER TABLE stats."Passing_Stats" RENAME COLUMN "GWD" TO "Game_Winning_Drives";
    """)
    connection.commit()
    print("Passing Stats readability improved!")

def beautify_rushing(cursor, connection):
    cursor.execute("""
    ALTER TABLE stats."Rushing_Stats" RENAME COLUMN "Att" TO "Rushing_Attempts";
    ALTER TABLE stats."Rushing_Stats" RENAME COLUMN "Yds" TO "Rushing_Yards";
    ALTER TABLE stats."Rushing_Stats" RENAME COLUMN "TD" TO "Rushing_TDs";
    ALTER TABLE stats."Rushing_Stats" RENAME COLUMN "1D" TO "First_Downs";
    ALTER TABLE stats."Rushing_Stats" RENAME COLUMN "Succ%" TO "Success_Percentage";
    ALTER TABLE stats."Rushing_Stats" RENAME COLUMN "Lng" TO "Longest_Rush";
    ALTER TABLE stats."Rushing_Stats" RENAME COLUMN "Y/A" TO "Yards_Per_Attempt";
    ALTER TABLE stats."Rushing_Stats" RENAME COLUMN "Y/G" TO "Rushing_Yards_Per_Game";
    ALTER TABLE stats."Rushing_Stats" RENAME COLUMN "A/G" TO "Attempts_Per_Game";
    ALTER TABLE stats."Rushing_Stats" RENAME COLUMN "Fmb" TO "Fumbles";
    """)
    connection.commit()
    print("Rushing Stats readability improved!")

def beautify_receiving(cursor, connection):
    cursor.execute("""
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "Tgt" TO "Targets";
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "Rec" TO "Receptions";
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "Yds" TO "Receiving_Yards";
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "Y/R" TO "Yards_Per_Reception";
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "TD" TO "Receiving_TDs";
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "1D" TO "First_Downs";
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "Succ%" TO "Success_Percentage";
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "Lng" TO "Longest_Reception";
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "R/G" TO "Receptions_Per_Game";
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "Y/G" TO "Receiving_Yards_Per_Game";
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "Ctch%" TO "Catch_Percentage";
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "Y/Tgt" TO "Yards_Per_Target";
        ALTER TABLE stats."Receiving_Stats" RENAME COLUMN "Fmb" TO "Fumbles";
        """)
    connection.commit()
    print("Receiving Stats readability improved!")

def beautify(cursor, connection):
    beautify_passing(cursor, connection)
    beautify_rushing(cursor, connection)
    beautify_receiving(cursor, connection)

def create_complete_stats_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stats."Passing_Table"
(
    "Season" integer NOT NULL,
    "Player" "char"[] NOT NULL,
    "Age" integer NOT NULL,
    "Team" "char"[] NOT NULL,
    "Position" "char"[],
    "Games_Played" integer,
    "Games_Started" integer,
    "QB_Record" integer,
    "Completions" integer,
    "Attempts" integer,
    "Completion_Percentage" bigint,
    "Yards" integer,
    "Touchdown_Passes" integer,
    "Passing_Touchdown_Percentage" bigint,
    "Interceptions" integer,
    "Interception_Percentage" bigint,
    "First_Downs" integer,
    "Success_Percentage" bigint,
    "Long" integer,
    "Yards_per_Attempt" bigint,
    "Adjusted_Yards_per_Attempt" bigint,
    "Yards_per_Completion" bigint,
    "Yards_per_Game" bigint,
    "Passer_Rating" bigint,
    "QBR" bigint,
    "Sacked" integer,
    "Yards_Lost_By_Sacks" bigint,
    "Sacked_Percentage" bigint,
    "Net_Yards_Gained_Per_Attempt" bigint,
    "Adjusted_Net_Yards" bigint,
    "Fourth_Quarter_Comeback" integer,
    "Game_Winning_Drives" integer
)""")
