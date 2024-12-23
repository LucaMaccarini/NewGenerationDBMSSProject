from DB_functions import clear_database, create_terminals_schema, create_customers_schema, create_transaction_schema, load_terminals_from_csv, load_customers_with_available_terminals_from_csv, load_transactions_from_csv


print(f"clear_database()")
clear_database()
print("\n\n")

print(f"create_terminals_schema()")
create_terminals_schema()
print("\n\n")

print(f"create_customers_schema()")
create_customers_schema()
print("\n\n")

print(f"create_transaction_schema()")
create_transaction_schema()
print("\n\n")

print(f"load_terminals_from_csv()")
load_terminals_from_csv()
print("\n\n")

print(f"load_customers_with_available_terminals_from_csv()")
load_customers_with_available_terminals_from_csv()
print("\n\n")

print(f"load_transactions_from_csv()")
load_transactions_from_csv()
print("\n\n")

