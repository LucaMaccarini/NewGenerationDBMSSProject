from DB_functions import *
import pandas as pd

df = pd.DataFrame(query_a("2023-05-10"))
print(df)

#apply_optimizations_for_query_a()

df = pd.DataFrame(query_a_optimized("2023-05-10"))

print(df)

