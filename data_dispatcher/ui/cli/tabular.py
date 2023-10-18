import sys

class Column(object):
    def __init__(self, title, left=False, max_width=None, min_width=None, format=None):
        self.Title = title
        self.LeftJustified = left
        self.MaxDisplayWidth = max_width
        self.DisplayWidth = min_width or 0
        self.update_display_width(len(title))
        self.Format = format or "%s"

    def update_display_width(self, n):
        new_width = max(self.DisplayWidth, n)
        if self.MaxDisplayWidth is not None:
            new_width = min(self.MaxDisplayWidth, new_width)
        self.DisplayWidth = new_width

class Table(object):

    def __init__(self, *columns):
        self.Columns = []
        for c in columns:
            self.add_column(c)
        self.Rows = []

    def add_column(self, c):
        if isinstance(c, Column):
            self.Columns.append(c)
        elif isinstance(c, str):
            self.Columns.append(Column(c))

    def add_row(self, *row):
        if len(row) == 1 and isinstance(row[0], (list, tuple)):
            row = row[0]
        assert len(row) == len(self.Columns)
        display_row = []
        for item, column in zip(row, self.Columns):
            display = ""
            if item is not None:
                if isinstance(column.Format, str):
                    display = column.Format % (item,)
                elif callable(column.Format):
                    display = column.Format(item)
                column.update_display_width(len(display))
            display_row.append(display)
        self.Rows.append(display_row)

    def format(self, eol="\n"):
        format_parts = []
        divider_parts = []
        for c in self.Columns:
            w = c.DisplayWidth
            if c.LeftJustified:
                format_parts.append(f"%-{w}s")
            else:
                format_parts.append(f"%{w}s")
            divider_parts.append("-"*w)
        divider = " ".join(divider_parts)
        row_format = " ".join(format_parts)
        lines = [
            row_format % tuple(c.Title for c in self.Columns),
            divider
        ]
        for row in self.Rows:
            formatted_row = []
            for x, c in zip(row, self.Columns):
                w = c.DisplayWidth
                if len(x) > w:
                    x = x[:w-1] + '.'
                formatted_row.append(x)
            lines.append(row_format % tuple(formatted_row))
        lines.append(divider)
        return [line + eol for line in lines]

    def print(self, file=None):
        file = file or sys.stdout
        if file is not None:
            for line in self.format(eol=""):
                print(line, file=file)
