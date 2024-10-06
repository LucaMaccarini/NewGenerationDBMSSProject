import os
import pandas as pd
from DB_functions import create_customer, create_terminal, create_transaction
import config

# Funzione per importare i dati dei clienti da un CSV
def import_customers_from_csv():
    # Costruisco il percorso del CSV
    customers_csv_path = os.path.join(config.import_DB_absolute_path.rstrip('/'), 'customers.csv')

    # Verifico se il file esiste
    if not os.path.isfile(customers_csv_path):
        print(f"File {customers_csv_path} not found.")
        return False

    try:
        # Leggo il CSV
        customer_data = pd.read_csv(customers_csv_path, sep=';')

        # Creo i nodi Customer in Neo4j
        has_errors = False
        for _, row in customer_data.iterrows():
            success = create_customer(
                customer_id=row['CUSTOMER_ID'],
                x_customer_id=row['x_customer_id'],
                y_customer_id=row['y_customer_id'],
                mean_amount=row['mean_amount'],
                std_amount=row['std_amount'],
                mean_nb_tx_per_day=row['mean_nb_tx_per_day'],
                available_terminals=row['available_terminals']
            )

            if success:
                print(f"Customer {row['CUSTOMER_ID']} created.")
            else:
                print(f"Error during the creation of customer {row['CUSTOMER_ID']}.")
                if not has_errors:
                    has_errors=True

            return not has_errors
    except Exception as e:
        print(f"An error occurred while processing {customers_csv_path}: {e}")
        return False

# Funzione per importare i dati dei terminali da un CSV
def import_terminals_from_csv():
    # Costruisco il percorso del CSV
    terminals_csv_path = os.path.join(config.import_DB_absolute_path.rstrip('/'), 'terminals.csv')

    # Verifico se il file esiste
    if not os.path.isfile(terminals_csv_path):
        print(f"File {terminals_csv_path} not found.")
        return False

    try:
        # Leggo il CSV
        terminal_data = pd.read_csv(terminals_csv_path, sep=';')

        # Creo i nodi Terminal in Neo4j
        has_errors = False
        for _, row in terminal_data.iterrows():
            success = create_terminal(  # Assicurati che questa funzione esista
                terminal_id=row['TERMINAL_ID'],
                x_terminal_id=row['x_terminal_id'],
                y_terminal_id=row['y_terminal_id']
            )

            if success:
                print(f"Terminal {row['TERMINAL_ID']} created.")
            else:
                print(f"Error during the creation of terminal {row['TERMINAL_ID']}.")
                has_errors = True  

        return not has_errors
    except Exception as e:
        print(f"An error occurred while processing {terminals_csv_path}: {e}")
        return False
    
def import_transactions_from_csv():
    # Costruisco il percorso del CSV
    transactions_csv_path = os.path.join(config.import_DB_absolute_path.rstrip('/'), 'transactions.csv')

    # Verifico se il file esiste
    if not os.path.isfile(transactions_csv_path):
        print(f"File {transactions_csv_path} not found.")
        return False

    try:
        # Leggo il CSV
        transaction_data = pd.read_csv(transactions_csv_path, sep=';')

        # Creo i nodi Transaction in Neo4j
        has_errors = False
        for _, row in transaction_data.iterrows():
            success = create_transaction(  # Assicurati che questa funzione esista
                transaction_id=row['TRANSACTION_ID'],
                tx_time_seconds=row['TX_TIME_SECONDS'],
                tx_time_days=row['TX_TIME_DAYS'],
                customer_id=row['CUSTOMER_ID'],
                terminal_id=row['TERMINAL_ID'],
                tx_amount=row['TX_AMOUNT'],
                tx_datetime=row['TX_DATETIME'],
                tx_fraud=row['TX_FRAUD'],
                tx_fraud_scenario=row['TX_FRAUD_SCENARIO']
            )

            if success:
                print(f"Transaction {row['TRANSACTION_ID']} created.")
            else:
                print(f"Error during the creation of transaction {row['TRANSACTION_ID']}.")
                has_errors = True 

        return not has_errors
    except Exception as e:
        print(f"An error occurred while processing {transactions_csv_path}: {e}")
        return False


import_customers_from_csv()