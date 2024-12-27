from DB_functions import query_a1, query_a2, query_b1, query_b2, query_c, query_e1, query_e2, create_transaction_date_index
import pandas as pd

print("create_transaction_date_index()")
create_transaction_date_index()
print("\n\n")

year_and_month_under_analesis = "2023-04"

print(f"query_a1({year_and_month_under_analesis})")
df = pd.DataFrame(query_a1(year_and_month_under_analesis))
print(df.head())
print("\n\n")


print(f"query_a2({year_and_month_under_analesis})")
df = pd.DataFrame(query_a2(year_and_month_under_analesis))
print(df.head())
print("\n\n")


print(f"query_b1({year_and_month_under_analesis})")
df = pd.DataFrame(query_b1(year_and_month_under_analesis))
print(df.head())
print("\n\n")


print(f"query_b2({year_and_month_under_analesis})")
df = pd.DataFrame(query_b2(year_and_month_under_analesis))
print(df.head())
print("\n\n")


customer_id = 2
k = 15
print(f"query_c({customer_id}, {k})")
df = pd.DataFrame(query_c(customer_id, k))
print(df.head())
print("\n\n")

