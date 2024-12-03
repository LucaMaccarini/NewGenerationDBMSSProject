import pandas as pd
from DB_functions import extend_transactions_property_keys, create_transaction_extended_schema, query_dii

extend_transactions_property_keys()

create_transaction_extended_schema()

print("\n\n==========================\n")
print("Q_Dii")
df = pd.DataFrame(query_dii())
print(df.head())

