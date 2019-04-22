import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from helper_modules.create_table_from_select_operator import CreateTableFromSelectOperator
from helper_modules.create_table_from_sheet_operator import CreateTableFromSheetOperator
from helper_modules.blog_create_order_margin_table import blog_create_order_margin_table
from helper_modules.variables_and_constants import SNOWFLAKE_CONN_ID, DATA_ALERTS_ADDRESS

WORKFLOW_START_DATE = datetime.datetime(2018, 7, 5)

default_args = {
    'owner':'airflow',
    'start_date': WORKFLOW_START_DATE,
    'depends_on_past': False,
    'snowflake_conn_id': SNOWFLAKE_CONN_ID,
    'email_on_retry': True,
    'email_on_failure': True,
    'email': DATA_ALERTS_ADDRESS
}

dag = DAG(
    'blog_load_cost_info',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

inventory_items_table_op = CreateTableFromSheetOperator(
    schema_name='jeff',
    table_name='inventory_items',
    sheet_id='1DlAZRHui1DyLNmHiN-eJDKiC83VwBIavM7OgqsVEF1E',
    column_types={'product_id': 'integer'},
    default_column_type='varchar',
    dag=dag
)

inventory_cost_table_op = CreateTableFromSheetOperator(
    schema_name='jeff',
    table_name='inventory_cost_info',
    sheet_id='18hFmIcOB7w8xH2NjyTtH5_rSh_fSxjbWnuWeIrSx50Q',
    column_types={'period': 'date'},
    default_column_type='numeric(10,2)',
    dag=dag
)

baking_cost_table_op = CreateTableFromSheetOperator(
    schema_name='jeff',
    table_name='baking_cost_info',
    sheet_id='1Ofmz4IKaymDMAhZMW3YN9jL7hgoA2gHUZopNtLclk8I',
    column_types={
        'period': 'date',
        'bakery': 'varchar(32)'},
    default_column_type='numeric(10,2)',
    dag=dag
)

orders_table_op = CreateTableFromSheetOperator(
    schema_name='jeff',
    table_name='bakery_orders',
    sheet_id='1by8HYVcWOFzePlWto6ftKUfiRLRyNI6IHsdEg-f6Zcg',
    column_types={
        'period': 'date',
        'bakery': 'varchar(32)',
        'order_id': 'integer',
        'product_id': 'integer',
        'quantity': 'integer'},
    default_column_type='numeric(10,2)',
    dag=dag
)

order_margin_table_op = PythonOperator(
    task_id='blog_create_order_margin_table',
    provide_context=True,
    python_callable=blog_create_order_margin_table,
    dag=dag
)

inventory_cost_table_op.set_upstream(inventory_items_table_op)

order_margin_table_op.set_upstream(inventory_cost_table_op)
order_margin_table_op.set_upstream(baking_cost_table_op)
order_margin_table_op.set_upstream(orders_table_op)
