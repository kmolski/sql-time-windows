#!/usr/bin/env python3

import sys

from sql_generator import GroupByTimeWindow, TIME_UNITS
from mo_sql_parsing import parse, format
from mo_parsing.exceptions import ParseException

HELP_TEXT = """sql-time-windows script generator v0.1.0

This utility converts time-window queries to standard SQL queries that
can be run on a real database. Time window queries have this general form:

SELECT <columns>
FROM <tables>
WHERE <conditions>
GROUP BY TIME_WINDOW(<timestamp column name>, <time unit>,
                     <optional window width>, <optional window offset>)

To convert a time-window query, type it below and press Enter two times.

At any point, you can enter the following commands to access additional functionality:
  ':clear' - Clear the query input
  ':quit'  - Quit the program
  ':help'  - Show the help screen
"""


def print_help():
    print(HELP_TEXT)


def print_prompt():
    print("> ", end="", flush=True)


def extract_time_window_args(time_window_funcall):
    time_window_argc = len(time_window_funcall)
    if not 2 <= time_window_argc <= 4:
        raise TypeError("TIME_WINDOW takes from 2 to 4 arguments")

    column = time_window_funcall[0]
    if isinstance(column, dict):
        column = list(column.keys())[0]
    if not isinstance(column, str):
        raise TypeError("COLUMN must be a column name")

    time_unit = time_window_funcall[1]
    if not isinstance(time_unit, str) or time_unit.upper() not in TIME_UNITS:
        raise TypeError(
            "TIME_UNIT must be a valid time unit: "
            + f"{', '.join(TIME_UNITS[:-1])}, or {TIME_UNITS[-1]}"
        )

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

        select_tree, datediff_tree = GroupByTimeWindow(None, *args).sql_tree()

        if isinstance(select_columns, list):
            sql_tree["select"].append(select_tree)
        else:
            sql_tree["select"] = [sql_tree["select"], select_tree]

        sql_tree["groupby"] = {"value": datediff_tree}

    return sql_tree


def main():
    print_help()
    print_prompt()

    sql_query = ""

    for line in sys.stdin:
        command = line.strip()
        if command.startswith(":"):
            if command == ":clear":
                sql_query = ""
            if command == ":help":
                print_help()
            elif command == ":quit":
                break
        elif len(line) > 1:
            sql_query += line
        elif sql_query:
            try:
                sql_tree = parse(sql_query)
                transform_tree(sql_tree)
                sql_string = format(sql_tree)
                print(sql_string)
            except ParseException as exc:
                print("SYNTAX ERROR:", exc.message)
            except TypeError as exc:
                print("TYPE ERROR:", exc)
            finally:
                sql_query = ""
        print_prompt()


if __name__ == "__main__":
    main()
