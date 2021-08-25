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
        self.group_by_clause = GroupByTimeWindow(self, field, time_unit)
        return self

    def groupByTimeWindowAdjustable(self, field, time_unit, width=1, offset=0):
        self.group_by_clause = GroupByTimeWindow(self, field, time_unit, width, offset)
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
        if time_unit not in TIME_UNITS:
            raise Exception(f"Invalid time unit '{time_unit}'.")

        if not column: column = "timestamp_column_name"

        self.group_exprs = []

        datediff_expr = f"(TIMESTAMPDIFF({time_unit}, '1970-01-01 00:00', {column}) + {offset}) div {width}"
        self.select_expr = f"TIMESTAMPADD({time_unit}, {datediff_expr} * {width} - {offset}, '1970-01-01 00:00')"
        self.group_exprs.append(datediff_expr)
        if TIME_UNITS.index(time_unit) < 3:
            self.select_expr = f"DATE({self.select_expr})"

        if select_clause is not None:
            if column in select_clause.columns:
                select_clause.columns.remove(column)
            select_clause.columns.append(Computed(self.select_expr, "TimeWindowStart"))

    def emit(self):
        group_desc = "\n\t, ".join(self.group_exprs)
        return f"GROUP BY {group_desc}"

    def sql_strings(self):
        return f"SELECT {self.select_expr}", self.emit()

