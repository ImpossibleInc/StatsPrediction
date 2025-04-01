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

def close_connection(cursor, connection):
    cursor.close()
    connection.close()
    print("Connection closed!")

def create_passing_table(cursor):
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
