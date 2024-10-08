import os
import config
from neo4j import GraphDatabase

def get_neo4j_connection():
    uri = os.getenv('NEO4J_URI')
    user = os.getenv('NEO4J_USERNAME')
    password = os.getenv('NEO4J_PASSWORD')

    return GraphDatabase.driver(uri, auth=(user, password))

def close_neo4j_connection(driver):
    driver.close()

def load_customers_from_csv():
    driver = get_neo4j_connection()
    
    # Usa il link diretto per scaricare il CSV
    csv_file_url = "https://drive.google.com/uc?id=1aOAeJb7LY41sqekzPdL3bJx1omQeKc0H"
    
    query = f"""
    CALL apoc.periodic.iterate(
        'LOAD CSV WITH HEADERS FROM "{csv_file_url}" AS row RETURN row',
        'MERGE (c:Customer {{customer_id: row.CUSTOMER_ID}})
         ON CREATE SET 
            c.x_customer_id = toFloat(row.x_customer_id),
            c.y_customer_id = toFloat(row.y_customer_id),
            c.mean_amount = toFloat(row.mean_amount),
            c.std_amount = toFloat(row.std_amount),
            c.mean_nb_tx_per_day = toFloat(row.mean_nb_tx_per_day)
        ',
        {{batchSize: {config.lines_per_commit}, parallel: true}}
    )
    """

    try:
        with driver.session() as session:
            result = session.run(query)
    except Exception as e:
        print(f"ERROR load_customers_from_csv: {e}")
    finally:
        close_neo4j_connection(driver)



def create_customer(customer_id, x_customer_id, y_customer_id, mean_amount, std_amount, mean_nb_tx_per_day, available_terminals):
    driver = get_neo4j_connection()
    try:
        with driver.session() as session:
            session.write_transaction(
                lambda tx: tx.run(
                    """
                    CREATE (c:Customer {
                        customer_id: $customer_id,
                        x_customer_id: $x_customer_id,
                        y_customer_id: $y_customer_id,
                        mean_amount: $mean_amount,
                        std_amount: $std_amount,
                        mean_nb_tx_per_day: $mean_nb_tx_per_day,
                    })
                    """,
                    customer_id=customer_id,
                    x_customer_id=x_customer_id,
                    y_customer_id=y_customer_id,
                    mean_amount=mean_amount,
                    std_amount=std_amount,
                    mean_nb_tx_per_day=mean_nb_tx_per_day,
                )
            )
        return True
    except Exception as e:
        print(f"create_customer error: {e}")
        return False
    finally:
        driver.close()

def create_terminal(terminal_id, x_terminal_id, y_terminal_id):
    driver = get_neo4j_connection()
    try:
        with driver.session() as session:
            session.write_transaction(
                lambda tx: tx.run(
                    """
                    CREATE (t:Terminal {
                        terminal_id: $terminal_id,
                        x_terminal_id: $x_terminal_id,
                        y_terminal_id: $y_terminal_id
                    })
                    """,
                    terminal_id=terminal_id,
                    x_terminal_id=x_terminal_id,
                    y_terminal_id=y_terminal_id
                )
            )
        return True
    except Exception as e:
        print(f"create_terminal error: {e}")
        return False
    finally:
        driver.close()

def create_transaction(transaction_id, tx_time_seconds, tx_time_days, customer_id, terminal_id, tx_amount, tx_datetime, tx_fraud, tx_fraud_scenario):
    driver = get_neo4j_connection()
    try:
        with driver.session() as session:
            session.write_transaction(
                lambda tx: tx.run(
                    """
                    CREATE (tr:Transaction {
                        transaction_id: $transaction_id,
                        tx_time_seconds: $tx_time_seconds,
                        tx_time_days: $tx_time_days,
                        customer_id: $customer_id,
                        terminal_id: $terminal_id,
                        tx_amount: $tx_amount,
                        tx_datetime: $tx_datetime,
                        tx_fraud: $tx_fraud,
                        tx_fraud_scenario: $tx_fraud_scenario
                    })
                    """,
                    transaction_id=transaction_id,
                    tx_time_seconds=tx_time_seconds,
                    tx_time_days=tx_time_days,
                    customer_id=customer_id,
                    terminal_id=terminal_id,
                    tx_amount=tx_amount,
                    tx_datetime=tx_datetime,
                    tx_fraud=tx_fraud,
                    tx_fraud_scenario=tx_fraud_scenario
                )
            )
        return True
    except Exception as e:
        print(f"create_transaction error: {e}")
        return False
    finally:
        driver.close()