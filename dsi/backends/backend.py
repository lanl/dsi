from abc import ABC, abstractmethod
import re

class Backend(ABC):
    @abstractmethod
    def __init__(self, data_source, **kwargs) -> None:
        pass

    # Can raise NotImplementedError for a read-only backend
    @abstractmethod
    def ingest_artifacts(self, artifacts, **kwargs) -> None:
        pass

    @abstractmethod
    def query_artifacts(self, query, **kwargs):
        pass

    @abstractmethod
    def get_table(self, table_name, **kwargs):
        pass

    @abstractmethod
    def process_artifacts(self, **kwargs):
        pass

    @abstractmethod
    def get_schema(self):
        pass

    ## DEPRECATING SOON
    @abstractmethod
    def find(self, query_object, **kwargs):
        pass

    ## DEPRECATING SOON
    @abstractmethod
    def find_table(self, query_object, **kwargs):
        pass

    ## DEPRECATING SOON
    @abstractmethod
    def find_column(self, query_object, **kwargs):
        pass

    @abstractmethod
    def find_cell(self, query_object, **kwargs):
        pass

    @abstractmethod
    def find_relation(self, column_name, relation, **kwargs):
        pass

    @abstractmethod
    def list(self, **kwargs):
        pass

    @abstractmethod
    def num_tables(self):
        pass

    @abstractmethod
    def display(self, table_name, **kwargs):
        pass

    @abstractmethod
    def summary(self, table_name, **kwargs):
        pass

    @abstractmethod
    def close(self):
        pass


    class ValueObject:
        """
        Data Structure used for search results returned from find* methods

        Attributes
        ----------
        t_name : str
            Table name
        c_name : list
            Column name(s)
        row_num : int or None
            Row index (if applicable)
        value : any
            Matched value
        type : str
            {'cell', 'relation'}
        """
        def __init__(self):
            self.t_name = ""
            self.c_name = []
            self.row_num = None
            self.value = None
            self.type = ""
            # implement this later once filesystem table incorporated into dsi
            # self.filesystem_match = [] # list of all elements in that matching row in filesystem table


    def manual_string_parsing(self, query):
        # splits on double quotes not within any single quotes
        # splits on operators outside of any quotes
        op_pattern = re.compile(r'==|!=|~~|>=|<=|~|=|<|>|\(')
        parts = []
        buffer = ''
        in_single = False
        in_double = False
        operator_found = False
        i = 0

        while i < len(query):
            char = query[i]
            if char == "'" and not in_double:
                in_single = not in_single
                buffer += char
                i += 1
            elif char == '"' and not in_single:
                in_double = not in_double
                buffer += char
                i += 1
            elif not in_single and not in_double:
                op_match = op_pattern.match(query, i)
                if op_match and not operator_found:
                    if buffer.strip():
                        parts.append(buffer.strip())
                    parts.append(op_match.group())
                    buffer = ''
                    operator_found = True
                    i += len(op_match.group())
                else:
                    buffer += char
                    i += 1
            else:
                buffer += char
                i += 1

        if buffer.strip():
            parts.append(buffer.strip())

        return parts


    def check_relational_query(self, query, operators):
        def is_literal(s):
            return s.startswith("'") and s.endswith("'")
        def wrap_in_quotes(value):
            value = value[1:-1] if is_literal(value) else value
            return "'" + re.sub(r"(?<!')'(?!')", "''", value) + "'"

        result = self.manual_string_parsing(query)
        if len(result) == 1:
            raise ValueError("Could not identify the operator in `query`. The operator cannot be nested in double quotes")
        elif len(result) == 2:
            raise ValueError("Input must include a column, operator, and value. Operator cannot be enclosed in quotes.")
        elif len(result) == 3 and not is_literal(result[2]) and any(op in result[2] for op in operators):
            extra = "or partial match [~,~~]. If matching value has an operator in it, make sure to wrap all in single quotes."
            raise ValueError(f"Only one operation allowed. Inequality [<,>,<=,>=,!=], equality [=,==], range [()], {extra}")
        elif len(result) == 3 and result[2].startswith('"') and result[2].endswith('"'):
            raise ValueError("The value in the relational query() cannot be enclosed in double quotes")

        column_name = result[0]
        relation = result[1] + result[2]
        if "'" in column_name:
            raise ValueError("Cannot have a single quote as part of a column name")
        if "'" in result[2] and result[2].count("'") % 2 != 0:
            raise TypeError("Found an unmatched single quote. For apostrophes use 2 single quotes. Ex: he's -> he''s NOT he\"s")
        elif len(result) == 3 and result[1] == '(':
            start_msg = f"When applying a range-based query on '{column_name}' using (),"
            if ',' not in result[2]:
                raise ValueError(f"{start_msg} values must be separated by a comma.")
            elif ')' != result[2][-1]:
                raise ValueError(f"{start_msg} it must end with closing parenthesis.")
            elif (result[2][result[2].rfind("'"):] if "'" in result[2] else result[2]).count(')') > 1:
                raise ValueError("Only one operation per query. Inequality [<,>,<=,>=,!=], equality [=,==], range [()], or partial match [~,~~].")

            values = result[2][:-1].strip()
            if values[0] == '"' or values[-1] == '"':
                raise ValueError("Neither value in the range-based query can be enclosed in double quotes. Only single quotes")
            if "'" not in values or ("'" in values and not is_literal(values)):
                if values.count(',') > 1 or ' ' in values.replace(', ', ','):
                    raise ValueError("Range-based queries require multi-word values to be enclosed in single quotes")
            values = re.sub(r"\s*,\s*(?=(?:[^']*'[^']*')*[^']*$)", ",", values)
            values = re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", values)
            if '' in values or len(values) != 2:
                raise ValueError("There needs to be two values for the range query. Ex: (1,2)")
            relation = f"({wrap_in_quotes(values[0])},{wrap_in_quotes(values[1])})"
            if values[0] > values[1]:
                raise ValueError(f"Invalid input range: '{relation}'. The lower value must come first.")
        else:
            relation = f"{result[1]} {wrap_in_quotes(result[2])}"

        return column_name, relation