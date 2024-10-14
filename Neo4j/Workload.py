from DB_functions import *
import pandas as pd

df = pd.DataFrame(query_a("2023-05-10"))

create_compound_index_on_tx_datetime_year_and_month()

df = pd.DataFrame(query_a("2023-05-10"))

print(df)