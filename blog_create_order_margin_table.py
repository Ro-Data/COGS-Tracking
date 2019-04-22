#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals

import io
import collections

import snowflake.connector

from variables_and_constants import get_connection_dict
from helper_modules.table_from_select_with_keys import create_table_from_select_helper


def get_table_columns(cursor, schema, table):
    query = """
    SELECT
        column_name
    FROM
        information_schema.columns
    WHERE
        table_schema = UPPER(%s)
        AND table_name = UPPER(%s)
    ORDER BY
        ordinal_position
    """
    cursor.execute(query, (schema, table))
    rows = list(cursor)
    return [row[0] for row in rows]


def group_by_item(columns):
    groups = collections.defaultdict(set)
    for column in columns:
        parts = column.split(' ')
        key = parts[0]
        val = ' '.join(parts[1:])
        groups[key].add(val)
    for key in groups:
        groups[key] = sorted(groups[key])
    return sorted(groups.items())


def escape_for_like(string, metachars='_%'):
    """Return `string` with SQL LIKE metacharacters escaped."""
    # This is so we can use ILIKE for case-insensitive string comparison
    for char in metachars:
        string = string.replace(char, r'\\' + char)
    return string


def build_query(cursor, inventory_item_table, item_cost_table):
    out = io.StringIO()

    inventory_cost_columns = get_table_columns(
        cursor, 'jeff', item_cost_table
    )
    inventory_cost_columns = inventory_cost_columns[1:] # Column 0 is the key
    inventory_cost_columns = group_by_item(inventory_cost_columns)

    template = """WHEN inventory_item.{col} ILIKE '{escaped_val}' THEN ZEROIFNULL(inventory_cost_info."{col} {val}")\n"""
    template = "        " + template

    out.write("WITH cte1 AS (\n")
    out.write("SELECT\n    bakery_orders.order_id AS order_id,\n")
    out.write("bakery_orders.period,\n")
    out.write("bakery_orders.product_id,\n")
    out.write("bakery_orders.quantity,\n")
    out.write("bakery_orders.amount_charged,\n")
    out.write("bakery_orders.bakery,\n")
    inventory_col_names = []
    for i, (col, vals) in enumerate(inventory_cost_columns):
        cost_col_name = "{}_cost".format(col)
        inventory_col_names.append(cost_col_name)
        out.write("    CASE\n")
        for val in vals:
            escaped_val = escape_for_like(val)
            out.write(template.format(
                col=col.upper(), val=val, escaped_val=escaped_val
            ))
        out.write("        ELSE 0\n")
        out.write("    END AS {},\n".format(cost_col_name))
    out.write("    ZEROIFNULL(baking_cost_info.baking_cost) AS baking_cost\n")
    out.write("FROM\n")
    out.write("    jeff.bakery_orders\n")
    out.write("    LEFT JOIN jeff.{inventory_item_table} AS inventory_item ON bakery_orders.product_id = inventory_item.product_id\n")
    out.write("    LEFT JOIN jeff.{item_cost_table} AS inventory_cost_info ON bakery_orders.period = inventory_cost_info.period\n")
    out.write("    LEFT JOIN jeff.baking_cost_info AS baking_cost_info ON\n")
    out.write("        bakery_orders.period = baking_cost_info.period\n")
    out.write("        AND bakery_orders.BAKERY = baking_cost_info.BAKERY\n")
    out.write(")\n")
    out.write(", final as (\n")
    out.write("SELECT \n")
    out.write("    cte1.order_id\n")
    out.write("    ,cte1.period\n")
    out.write("    ,cte1.product_id\n")
    out.write("    ,cte1.bakery\n")
    out.write("    ,cte1.quantity\n")
    out.write("    ,cte1.amount_charged\n")
    out.write("    ,{} \n".format('\n    ,'.join(inventory_col_names)))
    out.write("    ,quantity * (INGREDIENT01_cost + INGREDIENT02_cost +INGREDIENT03_cost)              AS total_ingredient_cost\n")
    out.write("    ,LABEL_TYPE_cost                                                                    AS total_labeling_cost \n")
    out.write("    ,CASE WHEN QUANTITY > 1 THEN BATCH_PACKAGING_cost ELSE INDIVIDUAL_PACKAGING_cost END      AS total_packaging_cost\n")
    out.write("    ,total_ingredient_cost + total_labeling_cost + total_packaging_cost                 AS total_inventory_cost\n")
    out.write("    ,baking_cost\n")
    out.write("    ,total_inventory_cost + baking_cost AS total_variable_cost\n")
    out.write("    ,amount_charged - total_variable_cost AS gross_dollars\n")
    out.write("    ,CASE WHEN amount_charged != 0 THEN (amount_charged - total_variable_cost)/amount_charged END AS gross_margin\n")
    out.write("FROM\n")
    out.write("    cte1\n")
    out.write(")\n")
    out.write("SELECT * FROM final\n")

    sql_string = out.getvalue()
    sql_string = sql_string.format(inventory_item_table=inventory_item_table, item_cost_table=item_cost_table)
    return sql_string


def blog_create_order_margin_table(**kwargs):
    schema_name = kwargs.get('schema_name', 'jeff')
    table_name = kwargs.get('table_name', 'blog_order_margin_info')
    table = '{}.{}'.format(schema_name, table_name)
    connection_dict = get_connection_dict()
    with snowflake.connector.connect(**connection_dict) as connection:
        cursor = connection.cursor()
        query = build_query(cursor, 'inventory_items', 'inventory_cost_info')
    create_table_from_select_helper(connection_dict, query, table)


if __name__ == '__main__':
    blog_create_order_margin_table(schema_name='jeff', table_name='blog_order_margin_info')
