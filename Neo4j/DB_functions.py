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

def clear_database():
    driver = get_neo4j_connection()
    
    query = """
        MATCH (n)
        DETACH DELETE n
    """
    
    try:
        with driver.session() as session:
            session.run(query)
        return True
    except Exception as e:
        print(f"ERROR clear_database: {e}")
        return False
    finally:
        close_neo4j_connection(driver)

# load terminals
def load_terminals_from_csv():
    driver = get_neo4j_connection()

    query = f"""
        CALL apoc.periodic.iterate(
            'LOAD CSV WITH HEADERS FROM "{config.terminals_csv_link}" AS row FIELDTERMINATOR ";" 
            RETURN row',
            'MERGE (t:Terminal {{terminal_id: toInteger(row.TERMINAL_ID)}})
            ON CREATE SET 
                t.x_terminal_id = toFloat(row.x_terminal_id),
                t.y_terminal_id = toFloat(row.y_terminal_id)
            ',
            {{batchSize: {config.lines_per_commit}, parallel: true}}
        )
    """

    try:
        with driver.session() as session:
            session.run(query)
        return True
    except Exception as e:
        print(f"ERROR load_terminals_from_csv: {e}")
        return False
    finally:
        close_neo4j_connection(driver)


# load customers and available relations to terminals
# Preconditions: terminals must exist
def load_customers_with_available_terminals_from_csv():
    driver = get_neo4j_connection()
    
    query = f"""
        CALL apoc.periodic.iterate(
            'LOAD CSV WITH HEADERS FROM "{config.customers_csv_link}" AS row FIELDTERMINATOR ";" 
            RETURN row',
            'MERGE (c:Customer {{customer_id: toInteger(row.CUSTOMER_ID)}})
            ON CREATE SET  
                c.x_customer_id = toFloat(row.x_customer_id),
                c.y_customer_id = toFloat(row.y_customer_id),
                c.mean_amount = toFloat(row.mean_amount),
                c.std_amount = toFloat(row.std_amount),
                c.mean_nb_tx_per_day = toFloat(row.mean_nb_tx_per_day),
                c.nb_terminals = toInteger(row.nb_terminals)
            WITH c, row
            WITH c, apoc.convert.fromJsonList(row.available_terminals) AS available_terminal_ids
            UNWIND available_terminal_ids AS available_terminal_id
            MATCH (t:Terminal {{terminal_id: available_terminal_id}})
            MERGE (c)-[:Available]->(t)
            ',
            {{batchSize: {config.lines_per_commit}, parallel: true}}
        )
    """
    
    try:
        with driver.session() as session:
            session.run(query)
        return True
    except Exception as e:
        print(f"ERROR load_customers_with_available_terminals_from_csv: {e}")
        return False
    finally:
        close_neo4j_connection(driver)


# Load transactions
def load_transactions_from_csv():
    driver = get_neo4j_connection()

    query = f"""
        CALL apoc.periodic.iterate(
            'LOAD CSV WITH HEADERS FROM "{config.transactions_csv_link}" AS row FIELDTERMINATOR ";" 
            RETURN row',
            'MATCH (c:Customer {{customer_id: toInteger(row.CUSTOMER_ID)}}), 
                   (t:Terminal {{terminal_id: toInteger(row.TERMINAL_ID)}})
            MERGE (c)-[transaction:Make_transaction {{transaction_id: toInteger(row.TRANSACTION_ID)}}]->(t)
            ON CREATE SET 
                transaction.tx_time_seconds = toInteger(row.TX_TIME_SECONDS), 
                transaction.tx_time_days = toInteger(row.TX_TIME_DAYS),
                transaction.tx_amount = toFloat(row.TX_AMOUNT), 
                transaction.tx_datetime = row.TX_DATETIME, 
                transaction.tx_fraud = toInteger(row.TX_FRAUD), 
                transaction.tx_fraud_scenario = toInteger(row.TX_FRAUD_SCENARIO)
            ',
            {{batchSize: {config.lines_per_commit}, parallel: true}}
        )
    """

    try:
        with driver.session() as session:
            session.run(query)
        return True
    except Exception as e:
        print(f"ERROR load_transactions_from_csv: {e}")
        return False
    finally:
        close_neo4j_connection(driver)
