import pandas as pd
from DB_functions import query_di, create_transaction_extended_schema, query_dii, query_e1, query_e2

query_di()

create_transaction_extended_schema()

query_dii()



#print("\n\n==========================\n")
#print("Q_E1")
#df = pd.DataFrame(query_e1("2022-10", "2023-02"))
#print(df.head())


#print("\n\n==========================\n")
#print("Q_E2")
#df = pd.DataFrame(query_e2("2022-10", "2023-02"))
#print(df.head())


