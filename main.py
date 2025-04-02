from DataBase.DataCollection import *
from DataBase.TableTransformation import *

connection = establish_connection()
get_receiving(1932, 2024, connection[2])