import json
import pymysql
import os
from datetime import datetime

# Environment variables for database connection
DB_HOST = os.getenv('SCM_DB_HOST')
DB_USER = os.getenv('SCM_DB_USER')
DB_PASSWORD = os.getenv('SCM_DB_PASSWORD')
DB_NAME = os.getenv('SCM_DB_NAME')

def lambda_handler(event, context):
    try:
        # Connect to the MySQL database
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = connection.cursor()
        print('Connection created successfully')
        # Query to find pending purchase orders for today
        today = datetime.now().date()
        find_pending_orders_query = """
            SELECT id, product_id, quantity
            FROM scm_purchaseorder
            WHERE status = 'Pending' AND delivery_date = %s
        """
        cursor.execute(find_pending_orders_query, (today,))
        pending_orders = cursor.fetchall()
        print('pending Orders: ', pending_orders)
        # Process each pending order
        for order in pending_orders:
            order_id, product_id, quantity = order

            # Update the stock in scm_product
            update_stock_query = """
                UPDATE scm_product
                SET stock_quantity = stock_quantity + %s
                WHERE id = %s
            """
            cursor.execute(update_stock_query, (quantity, product_id))

            # Mark the purchase order as delivered
            update_order_status_query = """
                UPDATE scm_purchaseorder
                SET status = 'Delivered'
                WHERE id = %s
            """
            cursor.execute(update_order_status_query, (order_id,))
            print('updated: ',order_id)
        # Commit the transaction
        connection.commit()

        return {
             'statusCode': 200,
             'headers': {
            'Content-Type': 'application/json',
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps({"data":f"Processed {len(pending_orders)} orders successfully."})
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"An error occurred: {str(e)}"
        }

    finally:
        if connection:
            connection.close()

