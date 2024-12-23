from DB_functions import query_a1, query_a2, query_b1, query_b2, query_c, query_e1, query_e2, create_composite_index_if_not_exists_on_Make_transaction_tx_date_month_and_tx_date_year
import pandas as pd

print("create_composite_index_if_not_exists_on_Make_transaction_tx_date_month_and_tx_date_year()")
create_composite_index_if_not_exists_on_Make_transaction_tx_date_month_and_tx_date_year()
print("\n\n")

year_and_month_under_analesis = "2024-02"

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

