from DB_functions import load_customers_from_csv

def import_customers_from_csv(csv_file_path):
    lines_per_commit = 1000 
    merge_node_str = "(c:Customer {customer_id: row.CUSTOMER_ID})"
    on_create_set_str = """
        c.x_customer_id = toFloat(row.x_customer_id),
        c.y_customer_id = toFloat(row.y_customer_id),
        c.mean_amount = toFloat(row.mean_amount),
        c.std_amount = toFloat(row.std_amount),
        c.mean_nb_tx_per_day = toFloat(row.mean_nb_tx_per_day),
    """
    
    load_customers_from_csv(csv_file_path, lines_per_commit, merge_node_str, on_create_set_str)



load_customers_from_csv()