import pandas as pd
from DB_functions import query_di, create_transaction_extended_schema, query_dii, query_e1, query_e2

print(f"query_di()")
query_di()
print("\n\n")

print(f"create_transaction_extended_schema()")
create_transaction_extended_schema()
print("\n\n")

print(f"query_dii()")
query_dii()
print("\n\n")


startMonthYear = "2023-01"
endMonthYear = "2023-04"


print(f"query_e1({startMonthYear}, {endMonthYear})")
df = pd.DataFrame(query_e1(startMonthYear, endMonthYear))
print(df.head())
print("\n\n")


#print(f"query_e2({startMonthYear}, {endMonthYear})")
#df = pd.DataFrame(query_e2(startMonthYear, endMonthYear))
#print(df.head())
#print("\n\n")

