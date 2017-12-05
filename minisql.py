from prettytable import PrettyTable
import sys
class database(object):
    """docstring for database"""
    def __init__(self, metafile_name):
        self.metafile_name = metafile_name
        self.tables_names = []
        self.columns_tables_names = {}
        self.tables = {}
        self.columns = set([])
        lines=[]
        with open(metafile_name) as f:
            for line in f:
                lines.append(line)
        lines = map(lambda x: x.strip(), lines)
        i=0
        while i!=len(lines):
            if lines[i]=="<begin_table>":
                i=i+1
                Tname = lines[i]
                i=i+1
                Columns = []
                while lines[i]!="<end_table>":
                    Columns.append(lines[i])
                    self.columns.add(lines[i])
                    i=i+1
                self.tables_names.append(Tname)
                self.columns_tables_names[Tname] = Columns
                self.tables[Tname] = table(Tname, Columns)
            i=i+1

    def add_table(self, table):
        self.tables_names.append(table.Tname)
        self.columns_tables_names[table.Tname] = table.col_names
        self.tables[table.Tname] = table
        for col_name in table.col_names:
            self.columns.add(col_name)

class table(object):
    """docstring for table"""
    def __init__(self, Tname, Col_names, read_from_csv = True):
        self.Tname = Tname
        self.col_names = Col_names
        self.col_dict = {}
        for col_nm in self.col_names:
            self.col_dict[col_nm] = []
        self.number_of_records = 0
        if read_from_csv:
            self.insert_records_file(Tname+".csv")

    def insert_records_file(self, filename):
        records=[]
        with open(filename) as f:
            for line in f:
                records.append([int(i) for i in line.replace("\"","").replace("\'","").split(',')])
        for i, clname in enumerate(self.col_names):
            self.col_dict[clname].extend([records[j][i] for j in range(len(records))])
        self.number_of_records = self.number_of_records+len(records)

    def get_column(self, column_name):
        return self.col_dict[column_name]
    def has_column(self, column_name):
        return self.col_dict.has_key(column_name)

    def print_table(self, fun = None):
        t = PrettyTable()
        fun = [lambda x: x]*len(self.col_names)
        print "Table Name: " + self.Tname
        for i, col_name in enumerate(self.col_names):
            t.add_column(self.Tname + "." + col_name,fun[i](self.col_dict[col_name]))
        print t
