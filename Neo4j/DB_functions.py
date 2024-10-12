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

def get_neo4j_connection():
    uri = os.getenv('NEO4J_URI')
    user = os.getenv('NEO4J_USERNAME')
    password = os.getenv('NEO4J_PASSWORD')
    return GraphDatabase.driver(uri, auth=(user, password))

def close_neo4j_connection(driver):
    driver.close()

def clear_database():
    driver = get_neo4j_connection()
    
    delete_nodes_query = """
        MATCH (n)
        DETACH DELETE n
    """
    
    try:
        with driver.session() as session:

            session.run(delete_nodes_query)
            
            constraints_result = session.run("SHOW CONSTRAINTS")
            for record in constraints_result:
                drop_constraint_query = f"DROP CONSTRAINT {record['name']}"
                session.run(drop_constraint_query)
            
            indexes_result = session.run("SHOW INDEXES")
            for record in indexes_result:
                drop_index_query = f"DROP INDEX {record['name']}"
                session.run(drop_index_query)
        
        return True

    except Exception as e:
        print(f"ERROR clear_database: {e}")
        return False
    
    finally:
        close_neo4j_connection(driver)
       
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
                c.mean_nb_tx_per_day = toFloat(row.mean_nb_tx_per_day)
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
                transaction.tx_datetime = datetime(replace(row.TX_DATETIME, " ", "T")),
                transaction.tx_fraud = toBoolean(toInteger(row.TX_FRAUD)), 
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

def create_terminals_schema():
    driver = get_neo4j_connection()
    
    # Queries to create constraints and index using modern syntax
    queries = [
        """
        CREATE CONSTRAINT terminal_id_is_integer
        FOR (t:Terminal)
        REQUIRE t.terminal_id IS :: INTEGER;
        """,
        """
        CREATE CONSTRAINT terminal_id_key
        FOR (t:Terminal)
        REQUIRE t.terminal_id IS NODE KEY;
        """,
        """
        CREATE CONSTRAINT terminal_x_is_float
        FOR (t:Terminal)
        REQUIRE t.x_terminal_id IS :: FLOAT;
        """,
        """
        CREATE CONSTRAINT terminal_x_required
        FOR (t:Terminal)
        REQUIRE t.x_terminal_id IS NOT NULL;
        """,
        """
        CREATE CONSTRAINT terminal_y_is_float
        FOR (t:Terminal)
        REQUIRE t.y_terminal_id IS :: FLOAT;
        """,
        """
        CREATE CONSTRAINT terminal_y_required
        FOR (t:Terminal)
        REQUIRE t.y_terminal_id IS NOT NULL;
        """
    ]


    try:
        with driver.session() as session:
            for query in queries:
                session.run(query)
        return True
    except Exception as e:
        print(f"ERROR create_constraints_and_index: {e}")
        return False
    finally:
        close_neo4j_connection(driver)

def create_customers_schema():
    driver = get_neo4j_connection()
    
    queries = [
        """
        CREATE CONSTRAINT customer_id_is_integer
        FOR (c:Customer)
        REQUIRE c.customer_id IS :: INTEGER;
        """,
        """
        CREATE CONSTRAINT customer_id_key
        FOR (c:Customer)
        REQUIRE c.customer_id IS NODE KEY;
        """,
        """
        CREATE CONSTRAINT customer_x_is_float
        FOR (c:Customer)
        REQUIRE c.x_customer_id IS :: FLOAT;
        """,
        """
        CREATE CONSTRAINT customer_x_required
        FOR (c:Customer)
        REQUIRE c.x_customer_id IS NOT NULL;
        """,
        """
        CREATE CONSTRAINT customer_y_is_float
        FOR (c:Customer)
        REQUIRE c.y_customer_id IS :: FLOAT;
        """,
        """
        CREATE CONSTRAINT customer_y_required
        FOR (c:Customer)
        REQUIRE c.y_customer_id IS NOT NULL;
        """,
        """
        CREATE CONSTRAINT customer_mean_amount_is_float
        FOR (c:Customer)
        REQUIRE c.mean_amount IS :: FLOAT;
        """,
        """
        CREATE CONSTRAINT customer_mean_amount_required
        FOR (c:Customer)
        REQUIRE c.mean_amount IS NOT NULL;
        """,
        """
        CREATE CONSTRAINT customer_std_amount_is_float
        FOR (c:Customer)
        REQUIRE c.std_amount IS :: FLOAT;
        """,
        """
        CREATE CONSTRAINT customer_std_amount_required
        FOR (c:Customer)
        REQUIRE c.std_amount IS NOT NULL;
        """,
        """
        CREATE CONSTRAINT customer_mean_nb_tx_per_day_is_float
        FOR (c:Customer)
        REQUIRE c.mean_nb_tx_per_day IS :: FLOAT;
        """,
        """
        CREATE CONSTRAINT customer_mean_nb_tx_per_day_required
        FOR (c:Customer)
        REQUIRE c.mean_nb_tx_per_day IS NOT NULL;
        """
    ]

    try:
        with driver.session() as session:
            for query in queries:
                session.run(query)
        return True
    except Exception as e:
        print(f"ERROR create_customers_schema: {e}")
        return False
    finally:
        close_neo4j_connection(driver)

def create_transaction_schema():
    driver = get_neo4j_connection()

    queries = [
        """
        CREATE CONSTRAINT transaction_id_key
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.transaction_id IS RELATIONSHIP KEY;
        """,
        """
        CREATE CONSTRAINT transaction_id_is_integer
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.transaction_id IS :: INTEGER;
        """,
        """
        CREATE CONSTRAINT tx_time_seconds_is_integer
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.tx_time_seconds IS :: INTEGER;
        """,
        """
        CREATE CONSTRAINT tx_time_seconds_required
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.tx_time_seconds IS NOT NULL;
        """,
        """
        CREATE CONSTRAINT tx_time_days_is_integer
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.tx_time_days IS :: INTEGER;
        """,
        """
        CREATE CONSTRAINT tx_time_days_required
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.tx_time_days IS NOT NULL;
        """,
        """
        CREATE CONSTRAINT tx_fraud_is_boolean
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.tx_fraud IS :: BOOLEAN;
        """,
        """
        CREATE CONSTRAINT tx_fraud_required
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.tx_fraud IS NOT NULL;
        """,
        """
        CREATE CONSTRAINT tx_fraud_scenario_is_integer
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.tx_fraud_scenario IS :: INTEGER;
        """,
        """
        CREATE CONSTRAINT tx_fraud_scenario_required
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.tx_fraud_scenario IS NOT NULL;
        """,
        #"""
        #CREATE CONSTRAINT tx_datetime_is_local_datetime
        #FOR ()-[transaction:Make_transaction]->() 
        #REQUIRE transaction.tx_datetime IS :: LOCAL DATETIME;
        #""",
        """
        CREATE CONSTRAINT tx_datetime_required
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.tx_datetime IS NOT NULL;
        """,
        """
        CREATE CONSTRAINT tx_amount_is_float
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.tx_amount IS :: FLOAT;
        """,
        """
        CREATE CONSTRAINT tx_amount_required
        FOR ()-[transaction:Make_transaction]->() 
        REQUIRE transaction.tx_amount IS NOT NULL;
        """
    ]


    try:
        with driver.session() as session:
            for query in queries:
                session.run(query)
        return True
    except Exception as e:
        print(f"ERROR create_transaction_schema: {e}")
        return False
    finally:
        close_neo4j_connection(driver)

