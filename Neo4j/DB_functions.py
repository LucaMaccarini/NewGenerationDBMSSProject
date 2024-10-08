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
    
    query = f"""
        CALL apoc.periodic.iterate(
            'LOAD CSV WITH HEADERS FROM "{config.customers_csv_link}" AS row FIELDTERMINATOR ";" 
            RETURN row',
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
            session.run(query)
        return True 
    except Exception as e:
        print(f"ERROR load_customers_from_csv: {e}")
        return False
    finally:
        close_neo4j_connection(driver)