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

    delete_nodes_query = f"""
        MATCH (n)
        CALL apoc.nodes.delete(n, {config.lines_per_commit}) YIELD value
        RETURN value
    """
    
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

def execute_query_command(name, query):
    driver = get_neo4j_connection()
    try:
        start_time=time.time()
        driver.execute_query(query)
        print(f"{name} execution time: {{:.2f}}s".format(time.time()-start_time))
        return True
    except Exception as e:
        print(f"ERROR {name}: {e}")
        return False
    finally:
        close_neo4j_connection(driver)

def execute_query_commands(name, queries):
    driver = get_neo4j_connection()
    try:
        start_time=time.time()
        
        for query in queries:
            driver.execute_query(query)

        print(f"{name} execution time: {{:.2f}}s".format(time.time()-start_time))
        return True
    except Exception as e:
        print(f"ERROR {name}: {e}")
        return False
    finally:
        close_neo4j_connection(driver)

def execute_query_df(name, query):
    driver = get_neo4j_connection()
    if driver is None:
        return False

    try:
        start_time=time.time()
        result = driver.execute_query(query, result_transformer_= neo4j.Result.to_df)
        print(f"{name} execution time: {{:.2f}}s".format(time.time() - start_time))

        return result
    except Exception as e:
        print(f"ERROR {name}: {e}")
        return None
    finally:
        close_neo4j_connection(driver)

def load_terminals_from_csv():
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
    return execute_query_command("load_terminals_from_csv", query)

def load_customers_with_available_terminals_from_csv():    
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
    return execute_query_command("load_customers_with_available_terminals_from_csv",query)

def load_transactions_from_csv():
    query = f"""
        CALL apoc.periodic.iterate(
            'LOAD CSV WITH HEADERS FROM "{config.transactions_csv_link}" AS row FIELDTERMINATOR ";" 
            RETURN row',
            'WITH row,
                  split(row.TX_DATETIME, " ") AS splitted_date_time
                  
            WITH row,
                 date(splitted_date_time[0]) AS parsed_date,
                 localtime(splitted_date_time[1]) AS parsed_local_time

            MATCH (c:Customer {{customer_id: toInteger(row.CUSTOMER_ID)}}), 
                (t:Terminal {{terminal_id: toInteger(row.TERMINAL_ID)}})
            MERGE (c)-[transaction:Make_transaction {{transaction_id: toInteger(row.TRANSACTION_ID)}}]->(t)
            ON CREATE SET 
                transaction.tx_time_seconds = toInteger(row.TX_TIME_SECONDS), 
                transaction.tx_time_days = toInteger(row.TX_TIME_DAYS),
                transaction.tx_amount = toFloat(row.TX_AMOUNT), 
                transaction.tx_fraud = toBoolean(toInteger(row.TX_FRAUD)), 
                transaction.tx_fraud_scenario = toInteger(row.TX_FRAUD_SCENARIO),

                transaction.tx_date_day = parsed_date.day,
                transaction.tx_date_month = parsed_date.month,
                transaction.tx_date_year = parsed_date.year, 
                transaction.tx_date_time = parsed_local_time 
            ',
            {{batchSize: {config.lines_per_commit}, parallel: {config.parallel_loading}}}
        )
    """
    return execute_query_command("load_transactions_from_csv",query)

def create_terminals_schema():
    queries = [
        "CREATE CONSTRAINT terminal_id_is_integer FOR (t:Terminal) REQUIRE t.terminal_id IS :: INTEGER;",
        "CREATE CONSTRAINT terminal_id_key FOR (t:Terminal) REQUIRE t.terminal_id IS NODE KEY;",
        "CREATE CONSTRAINT terminal_x_is_float FOR (t:Terminal) REQUIRE t.x_terminal_id IS :: FLOAT;",
        "CREATE CONSTRAINT terminal_x_required FOR (t:Terminal) REQUIRE t.x_terminal_id IS NOT NULL;",
        "CREATE CONSTRAINT terminal_y_is_float FOR (t:Terminal) REQUIRE t.y_terminal_id IS :: FLOAT;",
        "CREATE CONSTRAINT terminal_y_required FOR (t:Terminal) REQUIRE t.y_terminal_id IS NOT NULL;"
    ]
    
    return execute_query_commands("create_terminals_schema", queries)

def create_customers_schema():
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
    return execute_query_commands("create_customers_schema", queries)

def create_transaction_schema():
    queries = [
        "CREATE CONSTRAINT transaction_id_is_integer FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.transaction_id IS :: INTEGER;",
        "CREATE CONSTRAINT transaction_id_key FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.transaction_id IS RELATIONSHIP KEY;",
        "CREATE CONSTRAINT tx_time_seconds_is_integer FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_time_seconds IS :: INTEGER;",
        "CREATE CONSTRAINT tx_time_seconds_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_time_seconds IS NOT NULL;",
        "CREATE CONSTRAINT tx_time_days_is_integer FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_time_days IS :: INTEGER;",
        "CREATE CONSTRAINT tx_time_days_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_time_days IS NOT NULL;",
        "CREATE CONSTRAINT tx_amount_is_float FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_amount IS :: FLOAT;",
        "CREATE CONSTRAINT tx_amount_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_amount IS NOT NULL;",
        "CREATE CONSTRAINT tx_date_day_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_date_day IS NOT NULL;",
        "CREATE CONSTRAINT tx_date_day_is_integer FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_date_day IS :: INTEGER;",
        "CREATE CONSTRAINT tx_date_month_is_integer FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_date_month IS :: INTEGER;",
        "CREATE CONSTRAINT tx_date_month_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_date_month IS NOT NULL;",
        "CREATE CONSTRAINT tx_date_year_is_integer FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_date_year IS :: INTEGER;",
        "CREATE CONSTRAINT tx_date_year_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_date_year IS NOT NULL;",
        "CREATE CONSTRAINT tx_date_time_is_localtime FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_date_time IS :: LOCAL TIME;",
        "CREATE CONSTRAINT tx_date_time_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_date_time IS NOT NULL;",
        "CREATE CONSTRAINT tx_fraud_is_boolean FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_fraud IS :: BOOLEAN;",
        "CREATE CONSTRAINT tx_fraud_is_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_fraud IS NOT NULL;",
        "CREATE CONSTRAINT tx_fraud_scenario_is_integer FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_fraud_scenario IS :: INTEGER;",
        "CREATE CONSTRAINT tx_fraud_scenario_is_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_fraud_scenario IS NOT NULL;"
    ]
    return execute_query_commands("create_transaction_schema", queries)

#day_under_analesis is a string that contains a date in the format yyyy-MM-dd 
def query_a1(day_under_analesis):
    query = f"""
            WITH date.truncate('month', date("{day_under_analesis}") ) - duration('P1M') AS first_of_previous_month

            MATCH (c:Customer)

            OPTIONAL MATCH (c)-[tx_prev_month_all_prev_year:Make_transaction]->(:Terminal)
            WHERE 
                tx_prev_month_all_prev_year.tx_date_month = first_of_previous_month.month
                AND tx_prev_month_all_prev_year.tx_date_year < first_of_previous_month.year
            WITH
                first_of_previous_month,
                c,
                tx_prev_month_all_prev_year.tx_date_year as year, 
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
                tx.tx_date_month = first_of_previous_month.month AND 
                tx.tx_date_year = first_of_previous_month.year
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

    return execute_query_df("query_a1",query)

def create_composite_index_if_not_exists_on_Make_transaction_tx_date_month_and_tx_date_year():
    query = "CREATE INDEX composite_index_on_tx_date_year_and_month IF NOT EXISTS FOR ()-[tx:Make_transaction]-() ON (tx.tx_date_month, tx.tx_date_year)"
    return execute_query_command("create_composite_index_if_not_exists_on_Make_transaction_tx_date_month_and_tx_date_year", query)

#day_under_analesis is a string that contains a date in the format yyyy-MM-dd 
def query_a2(day_under_analesis):
    query = f"""
            WITH date.truncate('month', date("{day_under_analesis}") ) - duration('P1M') AS first_of_previous_month

            MATCH (c)-[tx_prev_month_all_prev_year:Make_transaction]->(:Terminal)
            WHERE 
                tx_prev_month_all_prev_year.tx_date_month = first_of_previous_month.month
                AND tx_prev_month_all_prev_year.tx_date_year < first_of_previous_month.year
            WITH
                first_of_previous_month,
                c,
                tx_prev_month_all_prev_year.tx_date_year as year,
                SUM(tx_prev_month_all_prev_year.tx_amount)  AS tx_prev_month_prev_year_total_amount, 
                COUNT(tx_prev_month_all_prev_year) AS tx_prev_month_prev_year_montly_freq
            WITH
            first_of_previous_month,
            c, 
            AVG(tx_prev_month_prev_year_total_amount) AS tx_prev_month_all_prev_year_total_amount_avg, 
            AVG(tx_prev_month_prev_year_montly_freq) AS tx_prev_month_all_prev_year_montly_freq_avg

            OPTIONAL MATCH (c)-[tx:Make_transaction]->(:Terminal)
            WHERE 
                tx.tx_date_month = first_of_previous_month.month AND 
                tx.tx_date_year = first_of_previous_month.year
            WITH
                c,
                SUM(tx.tx_amount) AS total_amount_prev_month, 
                COUNT(tx) AS monthly_freq_prev_month,
                tx_prev_month_all_prev_year_total_amount_avg,
                tx_prev_month_all_prev_year_montly_freq_avg

            RETURN
                c, 
                total_amount_prev_month < tx_prev_month_all_prev_year_total_amount_avg  AS is_under_total_amount_avg_of_same_period,
                monthly_freq_prev_month < tx_prev_month_all_prev_year_montly_freq_avg AS is_under_monthly_freq_avg_of_same_period
            """
    
    return execute_query_df("query_a2",query)

#day_under_analesis is a string that contains a date in the format yyyy-MM-dd 
def query_b1(day_under_analesis):
    query = f"""
            WITH date("{day_under_analesis}") AS today
            WITH today, date.truncate('month', today ) - duration('P1M') AS first_of_previous_month

            MATCH (t:Terminal)

            OPTIONAL MATCH (:Customer)-[tx_prev_month:Make_transaction]->(t)
            WHERE 
                tx_prev_month.tx_date_month = first_of_previous_month.month
                AND tx_prev_month.tx_date_year = first_of_previous_month.year

            with today, t, max(tx_prev_month.tx_amount) * 1.2 as tx_amount_fraud_limit

            OPTIONAL MATCH (:Customer)-[tx_current_month:Make_transaction]->(t)
            WHERE 
                tx_current_month.tx_date_month = today.month
                AND tx_current_month.tx_date_year = today.year

            WITH 
                t, 
                tx_amount_fraud_limit,
                COLLECT(CASE 
                    WHEN tx_current_month.tx_amount > tx_amount_fraud_limit THEN tx_current_month 
                    ELSE NULL 
                END) AS fraud_txs_current_month

            RETURN 
                t, 
                CASE 
                    WHEN tx_amount_fraud_limit IS NULL THEN NULL
                    ELSE fraud_txs_current_month
                END AS fraud_txs_current_month
            """

    return execute_query_df("query_b1",query)

#day_under_analesis is a string that contains a date in the format yyyy-MM-dd 
def query_b2(day_under_analesis):
    query = f"""
            WITH date("{day_under_analesis}") AS today
            WITH today, date.truncate('month', today ) - duration('P1M') AS first_of_previous_month

            MATCH (:Customer)-[tx_prev_month:Make_transaction]->(t:Terminal)
            WHERE 
                tx_prev_month.tx_date_month = first_of_previous_month.month
                AND tx_prev_month.tx_date_year = first_of_previous_month.year

            with today, t, max(tx_prev_month.tx_amount) * 1.2 as tx_amount_fraud_limit

            OPTIONAL MATCH (:Customer)-[tx_current_month:Make_transaction]->(t)
            WHERE 
                tx_current_month.tx_date_month = today.month
                AND tx_current_month.tx_date_year = today.year

            RETURN 
                t,
                COLLECT( 
                    CASE 
                        WHEN tx_current_month.tx_amount > tx_amount_fraud_limit THEN tx_current_month 
                        ELSE NULL 
                    END 
                )AS fraud_txs_current_month
            """
   
    return execute_query_df("query_b2",query)

#customer_id is an integer that indicates the customer_id property of :Customer
#k is an integer that indicates the different customers involved in the chain described in the project track
def query_c(customer_id, k):
    query = f"""
            WITH {k-1} * 2 AS k
            MATCH (start:Customer {{customer_id: {customer_id}}})
            CALL apoc.path.expandConfig(start, {{
                relationshipFilter: 'Make_transaction',
                labelFilter: 'Terminal|Customer',
                maxLevel: k,
                uniqueness: 'NODE_GLOBAL'
            }}) YIELD path

            WITH path
            WHERE length(path) = k
            RETURN nodes(path)[-1].customer_id AS CO_Customer
            """
    return execute_query_df("query_c",query)

def extend_transactions_property_keys():
    query = f"""
        CALL apoc.periodic.iterate(
            'MATCH (c:Customer)-[transaction:Make_transaction]->(t:Terminal) 
            RETURN transaction',
            'SET transaction.tx_day_period = CASE toInteger(rand() * 4)
                                                WHEN 0 THEN "morning" 
                                                WHEN 1 THEN "afternoon" 
                                                WHEN 2 THEN "evening" 
                                                ELSE "night" 
                                            END,
                transaction.tx_products_type = CASE toInteger(rand() * 5) 
                                                    WHEN 0 THEN "high-tech" 
                                                    WHEN 1 THEN "food" 
                                                    WHEN 2 THEN "clothing" 
                                                    WHEN 3 THEN "consumable" 
                                                    ELSE "other" 
                                                END,
                transaction.tx_security_feeling = toInteger(rand() * 5) + 1',
            {{batchSize: {config.lines_per_commit}, parallel: {config.parallel_loading}}}
        )
    """
    return execute_query_command("extend_transactions_property_keys", query)

def create_transaction_extended_schema():
    queries = [
        "CREATE CONSTRAINT tx_day_period_is_string FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_day_period IS :: STRING;",
        "CREATE CONSTRAINT tx_day_period_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_day_period IS NOT NULL;",
        "CREATE CONSTRAINT tx_products_type_is_string FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_products_type IS :: STRING;",
        "CREATE CONSTRAINT tx_products_type_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_products_type IS NOT NULL;",
        "CREATE CONSTRAINT tx_security_feeling_is_integer FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_security_feeling IS :: INTEGER;",
        "CREATE CONSTRAINT tx_security_feeling_required FOR ()-[transaction:Make_transaction]->() REQUIRE transaction.tx_security_feeling IS NOT NULL;",
    ]
    return execute_query_commands("create_transaction_extended_schema", queries)
