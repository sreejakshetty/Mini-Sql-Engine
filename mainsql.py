from minisql import *
import re, operator
import sys

class query_processing(object):
    """docstring for query_processing"""
    def __init__(self, db, query_string):
        self.db = db
        self.query_string = query_string
        self.table_var_name = 'table_var'
        self.rec_no_var_name = 'rec_no_var'

    def where_eval_string(self, condition_string):
        cond_split = re.split('(\W)', condition_string)
        cond_split = [x for x in cond_split if x!='']
        condition = []
        i=0
        while i!=len(cond_split):
            if cond_split[i] in self.db.tables_names:
                if cond_split[i+1] == '.' and cond_split[i+2] in self.db.columns_tables_names[cond_split[i]]:
                    condition.append(self.table_var_name+'.col_dict[\''+cond_split[i+2]+'\']['+self.rec_no_var_name+']')
                    i=i+2
                elif cond_split[i+1] != '.' and cond_split[i] in self.db.columns:
                    condition.append(self.table_var_name+'.col_dict[\''+cond_split[i]+'\']['+self.rec_no_var_name+']')
            elif cond_split[i] in self.db.columns:
                condition.append(self.table_var_name+'.col_dict[\''+cond_split[i]+'\']['+self.rec_no_var_name+']')
            elif cond_split[i] == '=' and cond_split[i-1] not in '><!':
                condition.append('==')
            elif cond_split[i].lower() == 'and':
                condition.append('and')
            elif cond_split[i].lower() == 'or':
                condition.append('or')
            else:
                condition.append(cond_split[i])
            i=i+1
        return ''.join(condition)

    def eval_where_condition(self, table_var, rec_no_var, eval_string):
        return eval(eval_string)

    def mergeTables(self, all_table_names):
        if len(all_table_names) == 1:
            return self.db.tables[all_table_names[0]]
        table_names = all_table_names[:2]

        all_column_names = sum([self.db.tables[tab_name].col_names for tab_name in table_names], [])
        all_column_names_unique = list(set(all_column_names))
        mergeTable = table("_".join(table_names), all_column_names_unique, read_from_csv = False)

        common_cols = set(self.db.tables[table_names[0]].col_names).intersection(self.db.tables[table_names[1]].col_names)
        for i in range(self.db.tables[table_names[0]].number_of_records):
            for j in range(self.db.tables[table_names[1]].number_of_records):
                common_cond = [self.db.tables[table_names[0]].col_dict[common_col][i] == self.db.tables[table_names[1]].col_dict[common_col][j] for common_col in common_cols]
                if reduce(operator.mul, common_cond, 1):
                    for col_name in all_column_names_unique:
                        if col_name in self.db.tables[table_names[0]].col_names:
                            mergeTable.col_dict[col_name].append(self.db.tables[table_names[0]].col_dict[col_name][i])
                        else:
                            mergeTable.col_dict[col_name].append(self.db.tables[table_names[1]].col_dict[col_name][j])
                    mergeTable.number_of_records = mergeTable.number_of_records+1
        self.db.add_table(mergeTable)

        if len(all_table_names)>2:
            mergeTable = self.mergeTables(["_".join(table_names)]+all_table_names[2:])

        return mergeTable

    def select_columns(self, Table, eval_string, column_names = []):
        if column_names == []:
            column_names = Table.col_names
        nRecords = Table.number_of_records
        res_table = table(Table.Tname, column_names, read_from_csv = False)
        for i in range(nRecords):
            try:
                self.eval_where_condition(Table, i, eval_string)
            except:
                print "wrong \"Where\" Condition", sys.exc_info()[0]
                sys.exit(0)
            if self.eval_where_condition(Table, i, eval_string):
                for col_name in column_names:
                    res_table.col_dict[col_name].append(Table.col_dict[col_name][i])
                res_table.number_of_records = res_table.number_of_records+1
        return res_table

    def parse_get_select(self, Q):
        select_str = re.search('select(.*)from', Q).group(1).strip()
        if select_str == '*' or select_str == 'max(*)' or select_str == 'min(*)' or select_str == 'distinct(*)' or select_str == 'sum(*)' or select_str == 'avg(*)':
            return []
        else:
            select_str = re.split('(\W)',select_str)
            if 'max' in select_str:
                select_str = [x for x in select_str if x not in 'max()']
            if 'min' in select_str:
                select_str = [x for x in select_str if x not in 'min()']
            if 'distinct' in select_str:
                select_str = [x for x in select_str if x not in 'distinct()']
            if 'sum' in select_str:
                select_str = [x for x in select_str if x not in 'sum()']
            if 'avg' in select_str:
                select_str = [x for x in select_str if x not in 'avg()']

            sel_columns = []
            i=0
            while i!=len(select_str):
                if select_str[i] in self.db.tables_names:
                    if select_str[i+1] == '.' and select_str[i+2] in self.db.columns_tables_names[select_str[i]]:
                        sel_columns.append(select_str[i+2])
                        i=i+2
                    elif select_str[i+1] != '.' and select_str[i] in self.db.columns:
                        sel_columns.append(select_str[i])
                elif select_str[i] in self.db.columns:
                    sel_columns.append(select_str[i])
                elif select_str[i]!=',':
                    print select_str[i]
                    print "invalid select string"
                    sys.exit(0)
                i=i+1


            return sel_columns

    def parse_get_from(self, Q):
        from_str = []
        if re.search('where', Q)==None:
            from_str = re.search('from(.*)', Q).group(1).strip()
        else:
            from_str = re.search('from(.*)where', Q).group(1).strip()
        if from_str == '*':
            return []
        else:
            from_str = re.split('(\W)',from_str)
            sel_tables = []
            i=0
            while i!=len(from_str):
                if from_str[i] in self.db.tables_names:
                    sel_tables.append(from_str[i])
                elif from_str[i]!=',':
                    print "Invalid Table names"
                    sys.exit(0)
                i=i+1
            return sel_tables

    def parse_get_where(self, Q):
        where_str = []
        if re.search('where', Q)==None:
            where_str = "True"
        else:
            where_str = re.search('where(.*)', Q).group(1).strip()
        return where_str

if __name__ == "__main__":
    db=database('metadata.txt')
    qp = query_processing(db, "")
    Q=raw_input()
    while Q!='q':
        if 'select' in Q and 'from' in Q:
            pass
        else:
            print "Invalid SQL query"
            print "\n"
            Q=raw_input()
            continue
        sel_columns = qp.parse_get_select(Q)
        sel_tables = qp.parse_get_from(Q)
        where_str = qp.parse_get_where(Q)
        try:
            mT = qp.mergeTables(sel_tables)
        except IndexError:
            print "Invalid Table Names"
            sys.exit(0)

        test=qp.where_eval_string(where_str)
        res = qp.select_columns(mT, qp.where_eval_string(where_str), sel_columns)
        print_lambdas = []
        select_str = re.search('select(.*)from', Q).group(1).strip()
        sel_col_strs = [x.strip() for x in select_str.split(',')]
        if len(sel_columns)!=0:
            for sel_col_str in sel_col_strs:
                if sel_col_str.startswith('max('):
                    print_lambdas.append(lambda x: [max(x)])
                elif sel_col_str.startswith('min('):
                    print_lambdas.append(lambda x: [min(x)])
                elif sel_col_str.startswith('distinct('):
                    print_lambdas.append(lambda x: list(set(x)))
                elif sel_col_str.startswith('sum('):
                    print_lambdas.append(lambda x: [sum(x)])
                elif sel_col_str.startswith('avg('):
                    print_lambdas.append(lambda x: [sum(x)*1.0/len(x)])
                else:
                    print_lambdas.append(lambda x: x)
        res.print_table(print_lambdas)

        print "\n"
        Q=raw_input()
