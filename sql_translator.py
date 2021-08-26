#!/usr/bin/env python3

from sql_generator import Select, Computed, GroupByTimeWindow, TIME_UNITS
from mo_sql_parsing import parse, format

import sys


def extract_time_window_args(time_window_funcall):
    time_window_argc = len(time_window_funcall)
    if not 2 <= time_window_argc <= 4:
        raise TypeError("TIME_WINDOW takes from 2 to 4 arguments")

    time_unit = time_window_funcall[0]
    if not isinstance(time_unit, str) or time_unit.upper() not in TIME_UNITS:
        raise TypeError("TIME_UNIT must be a valid time unit")

    column = time_window_funcall[1]
    if isinstance(column, dict):
        column = list(column.keys())[0]
    if not isinstance(column, str):
        raise TypeError("COLUMN must be a column name")

    width = None if time_window_argc <= 2 else time_window_funcall[2]
    if width is not None and not isinstance(width, int):
        raise TypeError("WIDTH must be an integer")

    offset = None if time_window_argc <= 3 else time_window_funcall[3]
    if offset is not None and not isinstance(offset, int):
        raise TypeError("OFFSET must be an integer")

    return [x for x in (column, time_unit, width, offset) if x is not None]


def transform_tree(sql_tree):
    select_columns = sql_tree["select"]
    groupby_expr = sql_tree["groupby"]["value"]
    if isinstance(groupby_expr, dict) and "time_window" in groupby_expr:
        time_window_funcall = groupby_expr["time_window"]
        args = extract_time_window_args(time_window_funcall)

        group_by = GroupByTimeWindow(None, *args)
        select_tree, datediff_tree = group_by.sql_tree()

        if isinstance(select_columns, list):
            sql_tree["select"].append(select_tree)
        else:
            sql_tree["select"] = [sql_tree["select"], select_tree]

        sql_tree["groupby"] = {"value": datediff_tree}

    return sql_tree


def main():
    sql_query = ""
    print("> ", end="", flush=True)
    for line in sys.stdin:
        if len(line) <= 1:
            sql_tree = parse(sql_query)
            transform_tree(sql_tree)
            sql_string = format(sql_tree)
            print(sql_string)
            sql_query = ""
        else:
            sql_query += line
        print("> ", end="", flush=True)


if __name__ == "__main__":
    main()
