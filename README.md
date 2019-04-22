# Tracking Cost of Goods Sold at a transactional level

## Requirements

- [`snowflake-connector-python`](https://pypi.org/project/snowflake-connector-python/)
- [`create_table_from_sheet`](https://github.com/Ro-Data/Ro-Create-Table-From-Sheet) - Helper module for importing Google Sheets to Snowflake
- [`create_table_from_select`](https://github.com/Ro-Data/Ro-Create-Table-From-Select) - Helper module for creating a table from a select statement

## Background

A flexible system for tracking Cost of Goods Sold (COGS) at a transactional level.  The code and datasets found here accompany [this blog post](https://medium.com/ro-data-team-blog/controlling-costs-diy-cogs-system-fb75c2e49801).

## Contents

### Datasets
Datasets are provided as CSV files instead of Google Sheets.  All are loaded into a data warehouse using the create_table_from_sheet helper module.

**`inventory_items.csv`**
- Each product is tagged with its applicable inventory items

**`inventory_item_costs.csv`**
- The field-value pairs from `inventory_items.csv` are enumerated and assigned prices for each period (monthly in this case)

**`bakery_costs.csv`**
- Contains the labor and service costs associated with each order fulfillment at different centers

**`bakery_orders.csv`**
- Transactional file containing period, product_id, bakery_id, price, quantity, and amount charged.


### Scripts

**`blog_load_cost_info.py`**
- This defines the [Airflow](https://airflow.apache.org/) DAG that loads all datasets into a data warehouse (we use Snowflake) and then kicks off the `blog_create_order_margin_table.py`.

**`blog_create_order_margin_table.py`**
- A function to calculate COGS for each transaction and store the results in a table called blog_create_order_margin_table.
