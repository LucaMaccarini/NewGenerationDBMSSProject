from DB_functions import *
import pandas as pd

create_index_if_not_exists_for_query_a()

print("\n\n==========================\n")
print("Q_A1")
df = pd.DataFrame(query_a1("2023-05-10"))
print(df.head())

print("\n\n==========================\n")
print("Q_A2")
df = pd.DataFrame(query_a2("2023-05-10"))
print(df.head())

