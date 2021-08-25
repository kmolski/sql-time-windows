from sql_generator import Select, Computed, GroupByTimeWindow
import time_window

# Select(Computed("SUM(item_count)", "order_count")).from_("Orders").where(
#     "order_time BETWEEN '1970-01-01' AND '2000-01-01'"
# ).groupByTimeWindowFixed("order_time", "month").emit_print()

# Select("tstamp", Computed("SUM(item_count)", "ITEM_COUNT")).from_(
#     "Orders"
# ).groupByTimeWindowAdjustable("tstamp", "day", 10).emit_print()

# Select(Computed("SUM(item_count)", "CountPerTimeWindow")).from_(
#     "new_schema.bigtime"
# ).groupByTimeWindowFixed("timestamp", "hour").emit_print()

# Select(Computed("SUM(item_count)", "CountPerTimeWindow")).from_(
#    "new_schema.bigtime"
# ).groupByTimeWindowAdjustable("timestamp", "month", 2, 0).emit_print()

# print(GroupByTimeWindow(None, "timestamp", "month").sql_strings())

# time_window.main([])
time_window.main(["-c", "tstamp"])
time_window.main(["-c", "tstamp", "-u", "day"])
time_window.main(["-c", "tstamp", "-u", "day", "-w", "2"])
time_window.main(["--column", "tstamp", "-u", "day", "--width", "2", "-o", "3"])
time_window.main(["-c", "tstamp", "-u", "day", "--offset", "3"])
time_window.main(["--unit", "xdd", "-o", "3"])
# time_window.main(["-c", "tstamp", "-u", "day", "-w", "2", "-o", "3", "-x"])
# time_window.main(["-c", "tstamp", "-u", "day", "-w", "2", "-o", "3", "--xd"])
# time_window.main(["-c", "tstamp", "-u", "day", "-w", "2", "-o", "3", "xd"])