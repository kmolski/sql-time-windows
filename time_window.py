#!/usr/bin/env python3

import sql_generator
import sys, getopt

def print_help():
    help = "Example: time_window.py --column <timestamp column name> --unit <time unit> [--width <time window width>] [--offset <time window offset>]"
    print(help)


def main(argv):
    column = ""
    unit = ""
    width = 1
    offset = 0

    if len(argv) == 0:
        print_help()
        return

    try:
        opts, others = getopt.getopt(argv, "hc:u:w:o:", ["help", "column=", "unit=", "width=", "offset="])
        if len(others) != 0:
            print(f"Unnecessary arguments: {others}")
            print_help()
            return
    except getopt.GetoptError as err:
        print(f"Invalid arguments: {err.msg}")
        print_help()
        return
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print_help()
            sys.exit()    
        elif opt in ('-c', '--column'):
            column = arg
        elif opt in ('-u', '--unit'):
            unit = arg
        elif opt in ('-w', '--width'):
            width = arg
        elif opt in ('-o', '--offset'):
            offset = arg

    try:
        select, groupby = sql_generator.GroupByTimeWindow(
            None, column, unit, width, offset
        ).sql_strings()
    
        print(select)
        print(groupby)
    except Exception as ex:
        print(ex.args[0])


if __name__ == "__main__":
   ret = main(sys.argv[1:])
