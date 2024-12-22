from DB_functions import query_a1, query_a2, query_b1, query_b2, query_c, query_e1, query_e2, create_composite_index_if_not_exists_on_Make_transaction_tx_date_month_and_tx_date_year
import pandas as pd

create_composite_index_if_not_exists_on_Make_transaction_tx_date_month_and_tx_date_year()

print("\n\n==========================\n")
print("Q_A1")
df = pd.DataFrame(query_a1("2023-02"))
print(df.head())

print("\n\n==========================\n")
print("Q_A2")
df = pd.DataFrame(query_a2("2024-02"))
print(df.head())


print("\n\n==========================\n")
print("Q_B1")
df = pd.DataFrame(query_b1("2024-02"))
print(df.head())


print("\n\n==========================\n")
print("Q_B2")
df = pd.DataFrame(query_b2("2024-02"))
print(df.head())

print("\n\n==========================\n")
print("Q_C")
df = pd.DataFrame(query_c(2,15))
print(df.head())

