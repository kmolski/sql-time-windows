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
        columns_desc = ", ".join(
            c.emit() if isinstance(c, Computed) else str(c) for c in self.columns
        )
        return "\n".join(
            (
                f"SELECT {columns_desc}",
                self.from_clause.emit(),
                self.where_clause.emit(),
                self.group_by_clause.emit(),
            )
        )

    def emit_print(self):
        return print(self.emit())

    def from_(self, tables):
        self.from_clause = From(tables)
        return self

    def where(self, condition):
        self.where_clause = Where(condition)
        return self

    def groupByTimeWindow(self, field, time_unit):
        self.group_by_clause = GroupByTimeWindow(field, time_unit, self)
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


class GroupByTimeWindow:
    TIME_UNITS = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"]

    def __init__(self, column, time_unit, select_clause):
        time_unit = time_unit.upper()
        time_units_slice = self.TIME_UNITS[: self.TIME_UNITS.index(time_unit) + 1]
        self.group_exprs = [f"DATEPART({u}, {column})" for u in time_units_slice]

        select_clause.columns.remove(column)
        select_clause.columns.extend(self.group_exprs)

    def emit(self):
        group_desc = ", ".join(self.group_exprs)
        return f"GROUP BY {group_desc}"


Select("order_time", Computed("SUM(item_count)", "order_count")).from_("Orders").where(
    "order_time BETWEEN '1970-01-01' AND '2000-01-01'"
).groupByTimeWindow("order_time", "month").emit_print()
