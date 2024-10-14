import os
import time 
import config
import neo4j

def get_neo4j_connection():
    try:
        uri = os.getenv('NEO4J_URI')
        user = os.getenv('NEO4J_USERNAME')
        password = os.getenv('NEO4J_PASSWORD')

        return neo4j.GraphDatabase.driver(uri, auth=(user, password))
    
    except Exception as e:
        print(f"ERROR: An unexpected error occurred while connecting to Neo4j: {e}")
        return None

def close_neo4j_connection(driver):
    if driver is not None:
        driver.close()

def clear_database():
    driver = get_neo4j_connection()
    if driver is None:
        return False

    delete_nodes_query = "MATCH (n) DETACH DELETE n"
    
    try:
        start_time=time.time()
        driver.execute_query(delete_nodes_query)

        constraints_result = driver.execute_query("SHOW CONSTRAINTS").records
        for record in constraints_result:
            drop_constraint_query = "DROP CONSTRAINT $name"
            driver.execute_query(drop_constraint_query, {"name": record["name"]})

        indexes_result = driver.execute_query("SHOW INDEXES").records
        for record in indexes_result:
            drop_index_query = "DROP INDEX $name"
            driver.execute_query(drop_index_query, {"name": record["name"]})

        print("clear_database execution time: {:.2f}s".format(time.time()-start_time))
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
            {{batchSize: {config.lines_per_commit}, parallel: {config.parallel_loading}}}
        )
    """

    try:
        start_time=time.time()
        driver.execute_query(query)
        print("load_terminals_from_csv execution time: {:.2f}s".format(time.time()-start_time))
        return True
    except Exception as e:
        print(f"ERROR load_terminals_from_csv: {e}")
        return False
    finally:
        close_neo4j_connection(driver)

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
            {{batchSize: {config.lines_per_commit}, parallel: {config.parallel_loading}}}
        )
    """
    
    try:
        start_time=time.time()
        driver.execute_query(query)
        print("load_customers_with_available_terminals_from_csv execution time: {:.2f}s".format(time.time()-start_time))
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
                transaction.tx_datetime = localdatetime(replace(row.TX_DATETIME, " ", "T")),
                transaction.tx_datetime_month = localdatetime(replace(row.TX_DATETIME, " ", "T")).month,
                transaction.tx_datetime_year = localdatetime(replace(row.TX_DATETIME, " ", "T")).year,
                transaction.tx_fraud = toBoolean(toInteger(row.TX_FRAUD)), 
                transaction.tx_fraud_scenario = toInteger(row.TX_FRAUD_SCENARIO)
            ',
            {{batchSize: {config.lines_per_commit}, parallel: {config.parallel_loading}}}
        )
    """

    try:
        start_time=time.time()
        driver.execute_query(query)
        print("load_transactions_from_csv execution time: {:.2f}s".format(time.time()-start_time))
        return True
    except Exception as e:
        print(f"ERROR load_transactions_from_csv: {e}")
        return False
    finally:
        close_neo4j_connection(driver)

def create_terminals_schema():
    driver = get_neo4j_connection()
    if driver is None:
        return False

    queries = [
        "CREATE CONSTRAINT terminal_id_is_integer FOR (t:Terminal) REQUIRE t.terminal_id IS :: INTEGER;",
        "CREATE CONSTRAINT terminal_id_key FOR (t:Terminal) REQUIRE t.terminal_id IS NODE KEY;",
        "CREATE CONSTRAINT terminal_x_is_float FOR (t:Terminal) REQUIRE t.x_terminal_id IS :: FLOAT;",
        "CREATE CONSTRAINT terminal_x_required FOR (t:Terminal) REQUIRE t.x_terminal_id IS NOT NULL;",
        "CREATE CONSTRAINT terminal_y_is_float FOR (t:Terminal) REQUIRE t.y_terminal_id IS :: FLOAT;",
        "CREATE CONSTRAINT terminal_y_required FOR (t:Terminal) REQUIRE t.y_terminal_id IS NOT NULL;"
    ]

    try:
        start_time=time.time()
        for query in queries:
            driver.execute_query(query)
        print("create_terminals_schema execution time: {:.2f}s".format(time.time()-start_time))
        return True
    except Exception as e:
        print(f"ERROR create_constraints_and_index: {e}")
        return False
    finally:
        close_neo4j_connection(driver)

def create_customers_schema():
    driver = get_neo4j_connection()
    if driver is None:
        return False

    queries = [
        "CREATE CONSTRAINT customer_id_is_integer FOR (c:Customer) REQUIRE c.customer_id IS :: INTEGER;",
        "CREATE CONSTRAINT customer_id_key FOR (c:Customer) REQUIRE c.customer_id IS NODE KEY;",
        "CREATE CONSTRAINT customer_x_is_float FOR (c:Customer) REQUIRE c.x_customer_id IS :: FLOAT;",
        "CREATE CONSTRAINT customer_x_required FOR (c:Customer) REQUIRE c.x_customer_id IS NOT NULL;",
        "CREATE CONSTRAINT customer_y_is_float FOR (c:Customer) REQUIRE c.y_customer_id IS :: FLOAT;",
        "CREATE CONSTRAINT customer_y_required FOR (c:Customer) REQUIRE c.y_customer_id IS NOT NULL;",
        "CREATE CONSTRAINT customer_mean_amount_is_float FOR (c:Customer) REQUIRE c.mean_amount IS :: FLOAT;",
        "CREATE CONSTRAINT customer_mean_amount_required FOR (c:Customer) REQUIRE c.mean_amount IS NOT NULL;",
        "CREATE CONSTRAINT customer_std_amount_is_float FOR (c:Customer) REQUIRE c.std_amount IS :: FLOAT;",
        "CREATE CONSTRAINT customer_std_amount_required FOR (c:Customer) REQUIRE c.std_amount IS NOT NULL;",
        "CREATE CONSTRAINT customer_mean_nb_tx_per_day_is_float FOR (c:Customer) REQUIRE c.mean_nb_tx_per_day IS :: FLOAT;",
        "CREATE CONSTRAINT customer_mean_nb_tx_per_day_required FOR (c:Customer) REQUIRE c.mean_nb_tx_per_day IS NOT NULL;"
    ]

    try:
        start_time=time.time()
        for query in queries:
            driver.execute_query(query)
        print("create_customers_schema execution time: {:.2f}s".format(time.time()-start_time))
        return True
    except Exception as e:
        print(f"ERROR create_customers_schema: {e}")
        return False
    finally:
        close_neo4j_connection(driver)

def create_transaction_schema():
    driver = get_neo4j_connection()
    if driver is None:
        return False

    queries = [
        "CREATE CONSTRAINT transaction_id_is_integer FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.transaction_id IS :: INTEGER;",
        "CREATE CONSTRAINT transaction_id_key FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.transaction_id IS RELATIONSHIP KEY;",
        "CREATE CONSTRAINT tx_time_seconds_is_integer FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_time_seconds IS :: INTEGER;",
        "CREATE CONSTRAINT tx_time_seconds_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_time_seconds IS NOT NULL;",
        "CREATE CONSTRAINT tx_time_days_is_integer FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_time_days IS :: INTEGER;",
        "CREATE CONSTRAINT tx_time_days_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_time_days IS NOT NULL;",
        "CREATE CONSTRAINT tx_amount_is_float FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_amount IS :: FLOAT;",
        "CREATE CONSTRAINT tx_amount_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_amount IS NOT NULL;",
        "CREATE CONSTRAINT tx_datetime_is_localdatetime FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_datetime :: LOCAL DATETIME;",
        "CREATE CONSTRAINT tx_datetime_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_datetime IS NOT NULL;",
        "CREATE CONSTRAINT tx_fraud_is_boolean FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_fraud IS :: BOOLEAN;",
        "CREATE CONSTRAINT tx_fraud_is_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_fraud IS NOT NULL;",
        "CREATE CONSTRAINT tx_fraud_scenario_is_integer FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_fraud_scenario IS :: INTEGER;",
        "CREATE CONSTRAINT tx_fraud_scenario_is_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_fraud_scenario IS NOT NULL;"
    ]

    try:
        start_time=time.time()
        for query in queries:
            driver.execute_query(query)
        print("create_transaction_schema execution time: {:.2f}s".format(time.time()-start_time))
        return True
    except Exception as e:
        print(f"ERROR create_transaction_schema: {e}")
        return False
    finally:
        close_neo4j_connection(driver)

#day_under_analesis is a string that contains a date in the format yyyy-MM-dd 
def query_a(day_under_analesis):
    driver = get_neo4j_connection()
    if driver is None:
        return False

    query = f"""
            WITH date.truncate('month', date("{day_under_analesis}") ) - duration('P1M') AS first_of_previous_month

            MATCH (c:Customer)

            OPTIONAL MATCH (c)-[tx_prev_month_all_prev_year:Make_transaction]->(:Terminal)
            WHERE 
                tx_prev_month_all_prev_year.tx_datetime.month = first_of_previous_month.month
                AND tx_prev_month_all_prev_year.tx_datetime.year < first_of_previous_month.year
            WITH
                first_of_previous_month,
                c,
                tx_prev_month_all_prev_year.tx_datetime.year as year, 
                CASE 
                    WHEN COUNT(tx_prev_month_all_prev_year)>0 THEN SUM(tx_prev_month_all_prev_year.tx_amount)
                    ELSE NULL
                END AS tx_prev_month_prev_year_total_amount, 

                CASE 
                    WHEN  COUNT(tx_prev_month_all_prev_year)>0 THEN COUNT(tx_prev_month_all_prev_year)
                    ELSE NULL
                END AS tx_prev_month_prev_year_montly_freq
            WITH
            first_of_previous_month,
            c, 
            AVG(tx_prev_month_prev_year_total_amount) AS tx_prev_month_all_prev_year_total_amount_avg, 
            AVG(tx_prev_month_prev_year_montly_freq) AS tx_prev_month_all_prev_year_montly_freq_avg

            OPTIONAL MATCH (c)-[tx:Make_transaction]->(:Terminal)
            WHERE 
                tx.tx_datetime.month = first_of_previous_month.month AND 
                tx.tx_datetime.year = first_of_previous_month.year
            WITH
                c,
                SUM(tx.tx_amount) AS total_amount_prev_month, 
                COUNT(tx) AS monthly_freq_prev_month,
                tx_prev_month_all_prev_year_total_amount_avg,
                tx_prev_month_all_prev_year_montly_freq_avg

            RETURN
                c, 
                CASE 
                    WHEN tx_prev_month_all_prev_year_total_amount_avg IS NULL THEN NULL
                    ELSE total_amount_prev_month < tx_prev_month_all_prev_year_total_amount_avg
                END AS is_under_total_amount_avg_of_same_period,

                CASE 
                    WHEN tx_prev_month_all_prev_year_montly_freq_avg IS NULL THEN NULL
                    ELSE monthly_freq_prev_month < tx_prev_month_all_prev_year_montly_freq_avg
                END AS is_under_monthly_freq_avg_of_same_period
    """

    try:
        start_time=time.time()
        result = driver.execute_query(query, result_transformer_= neo4j.Result.to_df)
        print("query_a execution time: {:.2f}s".format(time.time() - start_time))

        return result
    except Exception as e:
        print(f"ERROR create_transaction_schema: {e}")
        return None
    finally:
        close_neo4j_connection(driver)

def create_compound_index_on_tx_datetime_year_and_month():
    driver = get_neo4j_connection()
    if driver is None:
        return False

    query = """
            CREATE INDEX composite_index_on_tx_datetime_year_and_month FOR ()-[tx:Make_transaction]-() ON (tx.tx_datetime.month, tx.tx_datetime.year)
            """

    try:
        start_time=time.time()
        result = driver.execute_query(query, result_transformer_= neo4j.Result.to_df)
        print("create_compound_index_on_tx_datetime_year_and_month execution time: {:.2f}s".format(time.time() - start_time))

        return result
    except Exception as e:
        print(f"ERROR create_compound_index_on_tx_datetime_year_and_month: {e}")
        return None
    finally:
        close_neo4j_connection(driver)

#day_under_analesis is a string that contains a date in the format yyyy-MM-dd 
def query_a_optimized(day_under_analesis):
    driver = get_neo4j_connection()
    if driver is None:
        return False

    query = f"""
            WITH date.truncate('month', date("{day_under_analesis}") ) - duration('P1M') AS first_of_previous_month

            MATCH (c:Customer)

            OPTIONAL MATCH (c)-[tx_prev_month_all_prev_year:Make_transaction]->(:Terminal)
            WHERE 
                tx_prev_month_all_prev_year.tx_datetime_month = first_of_previous_month_month
                AND tx_prev_month_all_prev_year.tx_datetime_year < first_of_previous_month_year
            WITH
                first_of_previous_month,
                c,
                tx_prev_month_all_prev_year.tx_datetime_year as year, 
                CASE 
                    WHEN COUNT(tx_prev_month_all_prev_year)>0 THEN SUM(tx_prev_month_all_prev_year.tx_amount)
                    ELSE NULL
                END AS tx_prev_month_prev_year_total_amount, 

                CASE 
                    WHEN  COUNT(tx_prev_month_all_prev_year)>0 THEN COUNT(tx_prev_month_all_prev_year)
                    ELSE NULL
                END AS tx_prev_month_prev_year_montly_freq
            WITH
            first_of_previous_month,
            c, 
            AVG(tx_prev_month_prev_year_total_amount) AS tx_prev_month_all_prev_year_total_amount_avg, 
            AVG(tx_prev_month_prev_year_montly_freq) AS tx_prev_month_all_prev_year_montly_freq_avg

            OPTIONAL MATCH (c)-[tx:Make_transaction]->(:Terminal)
            WHERE 
                tx.tx_datetime_month = first_of_previous_month_month AND 
                tx.tx_datetime_year = first_of_previous_month_year
            WITH
                c,
                SUM(tx.tx_amount) AS total_amount_prev_month, 
                COUNT(tx) AS monthly_freq_prev_month,
                tx_prev_month_all_prev_year_total_amount_avg,
                tx_prev_month_all_prev_year_montly_freq_avg

            RETURN
                c, 
                CASE 
                    WHEN tx_prev_month_all_prev_year_total_amount_avg IS NULL THEN NULL
                    ELSE total_amount_prev_month < tx_prev_month_all_prev_year_total_amount_avg
                END AS is_under_total_amount_avg_of_same_period,

                CASE 
                    WHEN tx_prev_month_all_prev_year_montly_freq_avg IS NULL THEN NULL
                    ELSE monthly_freq_prev_month < tx_prev_month_all_prev_year_montly_freq_avg
                END AS is_under_monthly_freq_avg_of_same_period
    """

    try:
        start_time=time.time()
        result = driver.execute_query(query, result_transformer_= neo4j.Result.to_df)
        print("query_a execution time: {:.2f}s".format(time.time() - start_time))

        return result
    except Exception as e:
        print(f"ERROR create_transaction_schema: {e}")
        return None
    finally:
        close_neo4j_connection(driver)

