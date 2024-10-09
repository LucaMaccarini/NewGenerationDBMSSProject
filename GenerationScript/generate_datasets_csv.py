import os
import sys
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), 'Transaction_data_simulator_code'))

import config

from add_frauds import add_frauds
from generate_dataset import generate_dataset



# Loop sui DB definiti nel file di configurazione
for db in config.DBs:
    # Generazione delle tabelle del DB usando i valori di configurazione
    (customer_profiles_table, terminal_profiles_table, transactions_df) = generate_dataset(
        n_customers=db["n_customers"], 
        n_terminals=db["n_terminals"], 
        nb_days=db["n_days"], 
        start_date=db["start_date"], 
        r=db["radius"]
    )

    # Aggiungere frodi alle transazioni
    transactions_df = add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df)

    
    # Converto i valori della serie available_terminals dato che gli interi nella lista sono interi numpy
    customer_profiles_table['available_terminals'] = customer_profiles_table['available_terminals'].apply(
        lambda lst: [int(i) if isinstance(i, np.integer) else i for i in lst] if isinstance(lst, (list, np.array)) else lst
    )

    # Preparazione al salvataggio del DB
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Generated_DBs', db["DB_name"])


    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Salvataggio dei customers
    customer_profiles_table.to_csv(output_dir + '/customers.csv', sep=';', encoding='utf-8', index=False)

    # Salvataggio dei terminals
    terminal_profiles_table.to_csv(output_dir + '/terminals.csv', sep=';', encoding='utf-8', index=False)

    # Salvataggio delle transactions
    transactions_df.to_csv(output_dir + '/transactions.csv', sep=';', encoding='utf-8', index=False)

    print(f"Database data saved in: {os.path.abspath(output_dir)}/\n")


print(f"DONE! All DBs have been created ")
