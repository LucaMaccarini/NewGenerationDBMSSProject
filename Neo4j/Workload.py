from DB_functions import *
import pandas as pd

df = pd.DataFrame(query_a("2023-05-10"))

print(df)