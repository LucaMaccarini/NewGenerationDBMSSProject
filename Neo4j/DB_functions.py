import os
from neo4j import GraphDatabase

def get_neo4j_connection():
    uri = os.getenv('NEO4J_URI')
    user = os.getenv('NEO4J_USER')
    password = os.getenv('NEO4J_PASSWORD')
    return GraphDatabase.driver(uri, auth=(user, password))

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