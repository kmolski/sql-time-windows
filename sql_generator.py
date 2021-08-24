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
        self.group_by_clause = GroupByTimeWindowFixed(field, time_unit, self)
        return self

    def groupByTimeWindowAdjustable(self, field, time_unit, width, offset=0):
        self.group_by_clause = GroupByTimeWindowAdjustable(
            field, time_unit, width, self, offset
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


class GroupByTimeWindowFixed:
    def __init__(self, column, time_unit, select_clause):
        time_unit = time_unit.upper()
        time_units_slice = TIME_UNITS[: TIME_UNITS.index(time_unit) + 1]
        self.named_group_exprs = []
        self.group_exprs = []
        for u in time_units_slice:
            expr = f"EXTRACT({u} FROM {column})"
            self.group_exprs.append(expr)
            self.named_group_exprs.append(Computed(expr, u))

        if column in select_clause.columns: select_clause.columns.remove(column)
        select_clause.columns.extend(self.named_group_exprs)

    def emit(self):
        group_desc = "\n\t, ".join(self.group_exprs)
        return f"GROUP BY {group_desc}"


class GroupByTimeWindowAdjustable:
    def __init__(self, column, time_unit, width, select_clause, offset=0):
        time_unit = time_unit.upper()
        *time_units_prefix, last_unit = TIME_UNITS[: TIME_UNITS.index(time_unit) + 1]
        self.named_group_exprs = []
        self.group_exprs = []
        for u in time_units_prefix:
            expr = f"EXTRACT({u} FROM {column})"
            self.group_exprs.append(expr)
            self.named_group_exprs.append(Computed(expr, u))

        datediff_expr = f"(UNIX_TIMESTAMP({column}) + {offset}) div {width}"
        self.group_exprs.append(datediff_expr)
        self.named_group_exprs.append(Computed(datediff_expr, last_unit))
        
        # self.group_exprs = [f"DATEPART({u}, {column})" for u in time_units_prefix] + [
        #     f"(DATEDIFF_BIG({last_unit}, 0, {column}) + {offset}) / {width}"
        # ]

        if column in select_clause.columns: select_clause.columns.remove(column)
        select_clause.columns.extend(self.named_group_exprs)

    def emit(self):
        group_desc = "\n\t, ".join(self.group_exprs)
        return f"GROUP BY {group_desc}"


# Select(Computed("SUM(item_count)", "order_count")).from_("Orders").where(
#     "order_time BETWEEN '1970-01-01' AND '2000-01-01'"
# ).groupByTimeWindowFixed("order_time", "month").emit_print()

# Select("tstamp", Computed("SUM(item_count)", "ITEM_COUNT")).from_(
#     "Orders"
# ).groupByTimeWindowAdjustable("tstamp", "day", 10).emit_print()

Select(Computed("COUNT(id)", "CountPerTimeWindow")).from_(
    "new_schema.timestamps"
).groupByTimeWindowFixed("timestamp", "hour").emit_print()

Select(Computed("COUNT(id)", "CountPerTimeWindow")).from_(
   "new_schema.timestamps"
).groupByTimeWindowAdjustable("timestamp", "second", 15, 5).emit_print()
