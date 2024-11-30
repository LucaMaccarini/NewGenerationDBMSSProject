from DB_functions import *
import pandas as pd

create_composite_index_if_not_exists_on_Make_transaction_tx_date_month_and_tx_date_year()

print("\n\n==========================\n")
print("Q_A1")
df = pd.DataFrame(query_a1("2023-05"))
print(df.head())

print("\n\n==========================\n")
print("Q_A2")
df = pd.DataFrame(query_a2("2023-05"))
print(df.head())


print("\n\n==========================\n")
print("Q_B1")
df = pd.DataFrame(query_b1("2020-05"))
print(df.head())


print("\n\n==========================\n")
print("Q_B2")
df = pd.DataFrame(query_b2("2020-05"))
print(df.head())

print("\n\n==========================\n")
print("Q_c")
df = pd.DataFrame(query_c(1,15))
print(df.head())


print("\n\n==========================\n")
print("Q_e")
df = pd.DataFrame(query_e("2020-01", "2023-05"))
print(df.head())


