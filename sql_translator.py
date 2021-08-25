#!/usr/bin/env python3

from sql_generator import Select, Computed
from mo_sql_parsing import parse, format

sql_string = Select(Computed("SUM(item_count)", "CountPerTimeWindow")).from_(
    "new_schema.bigtime"
).groupByTimeWindowAdjustable("timestamp", "month", 2, 0).emit()

result = parse(sql_string)
print(result)
