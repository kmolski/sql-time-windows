from sql_generator import Select, Computed, GroupByTimeWindow

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
