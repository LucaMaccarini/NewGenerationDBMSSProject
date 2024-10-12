from DB_functions import *

clear_database()

create_terminals_schema()
create_customers_schema()
create_transaction_schema()

load_terminals_from_csv()
load_customers_with_available_terminals_from_csv()
load_transactions_from_csv()



