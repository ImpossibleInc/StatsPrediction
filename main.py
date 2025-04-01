from DataBase.DataCollection import *
from DataBase.TableTransformation import *

connection = establish_connection()
create_passing_table(connection[0])
get_passing(1932, 2024, connection[2])