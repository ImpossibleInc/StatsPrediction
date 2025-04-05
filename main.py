from DataBase.DataCollection import *
from DataBase.TableTransformation import *

connection = establish_connection()
beautify(connection[0], connection[1])