class Computed:
    def __init__(self, expression, name):
        self.expression = expression
        self.name = name

    def emit(self):
        return f"{self.expression} AS {self.name}"


class Select:
    def __init__(self, *columns):
        self.columns = list(columns)
        self.from_clause = None
        self.where_clause = None
        self.group_by_clause = None

    def emit(self):
        columns_desc = "\n\t, ".join(
            c.emit() if isinstance(c, Computed) else str(c) for c in self.columns
        )
        sql_source = f"SELECT {columns_desc}"
        for clause in (self.from_clause, self.where_clause, self.group_by_clause):
            if clause:
                sql_source += f"\n{clause.emit()}"
        return sql_source

    def emit_print(self):
        print(self.emit(), "\n")

    def from_(self, tables):
        self.from_clause = From(tables)
        return self

    def where(self, condition):
        self.where_clause = Where(condition)
        return self

    def groupByTimeWindowFixed(self, field, time_unit):
        self.group_by_clause = GroupByTimeWindow(
            self, field, time_unit 
        )
        return self

    def groupByTimeWindowAdjustable(self, field, time_unit, width=1, offset=0):
        self.group_by_clause = GroupByTimeWindow(
            self, field, time_unit, width, offset
        )
        return self


class From:
    def __init__(self, tables):
        self.tables = tables

    def emit(self):
        return f"FROM {self.tables}"


class Where:
    def __init__(self, condition):
        self.condition = condition

    def emit(self):
        return f"WHERE {self.condition}"


TIME_UNITS = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"]

class GroupByTimeWindow:
    def __init__(self, select_clause, column, time_unit, width=1, offset=0):
        time_unit = time_unit.upper()
        try:
            index = TIME_UNITS.index(time_unit)
        except ValueError:
            raise Exception(f"Invalid time unit '{time_unit}'.")

        named_group_exprs = []
        self.group_exprs = []

        datediff_expr = f"(TIMESTAMPDIFF({time_unit }, \"1970-01-01 00:00\", {column}) + {offset}) div {width}"
        select_expr = f"TIMESTAMPADD({time_unit }, {datediff_expr} * {width} - {offset}, \"1970-01-01 00:00\")"
        self.group_exprs.append(datediff_expr)
        if index < 3 : select_expr = f"DATE({select_expr})"
        named_group_exprs.append(Computed(select_expr, "TimeWindowStart"))

        if column in select_clause.columns: select_clause.columns.remove(column)
        select_clause.columns.extend(named_group_exprs)

    def emit(self):
        group_desc = "\n\t, ".join(self.group_exprs)
        return f"GROUP BY {group_desc}"


# Select(Computed("SUM(item_count)", "order_count")).from_("Orders").where(
#     "order_time BETWEEN '1970-01-01' AND '2000-01-01'"
# ).groupByTimeWindowFixed("order_time", "month").emit_print()

# Select("tstamp", Computed("SUM(item_count)", "ITEM_COUNT")).from_(
#     "Orders"
# ).groupByTimeWindowAdjustable("tstamp", "day", 10).emit_print()

Select(Computed("SUM(item_count)", "CountPerTimeWindow")).from_(
    "new_schema.bigtime"
).groupByTimeWindowFixed("timestamp", "hour").emit_print()

Select(Computed("SUM(item_count)", "CountPerTimeWindow")).from_(
   "new_schema.bigtime"
).groupByTimeWindowAdjustable("timestamp", "mnth", 2, 0).emit_print()
