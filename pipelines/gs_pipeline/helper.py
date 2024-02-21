from typing import List

class Column:
    def get(self,key,default):
        try:
            return self.__dict__[key]
        except:
            return default
    def __init__(self,name,column_type,constraints=None,default=None):
        self.name = name
        self.column_type = column_type
        self.constraints = constraints
        self.default = default
        
    def __repr__(self):
        return (f"Column(name={self.name}, column_type={self.column_type}, "
                f"constraints={self.constraints}, default={self.default})")

    def __str__(self):
        return self.__repr__()

    def __getitem__(self,key):
        return self.__dict__[key]

class Database:
    def __init__(self,
                 name : str,
                 **kwargs
                 ):
        self.name = name
        self.kwargs = kwargs

    def __repr__(self):
        strings = [f"name={self.name}"]
        strings+= [f"{key}={self.kwargs[key]}" for key in self.kwargs]

        return "Database(" + ','.join(strings) + ")"

    def __str__(self):
        return self.__repr__()

    def __getitem__(self,key):
        if key in __dict__:
            return self.__dict__[key]
        elif key in self.kwargs:
            return self.kwargs[key]
        else:
            raise KeyError(f"{key}")

class Schema:
    def __init__(self,
                 name : str,
                 database : Database,
                 **kwargs
                 ):
        self.name = name
        self.database = database
        self.kwargs = kwargs

    def __repr__(self):
        strings = [f"name={self.name}"]
        strings+=[f"database={self.database}"]
        strings+= [f"{key}={self.kwargs[key]}" for key in self.kwargs]

        return "Schema(" + ','.join(strings) + ")"

    def __str__(self):
        return self.__repr__()

    def __getitem__(self,key):
        if key in __dict__:
            return self.__dict__[key]
        elif key in self.kwargs:
            return self.kwargs[key]
        else:
            raise KeyError(f"{key}")

class Table:
    def __init__(self,
                 name : str,
                 schema : Schema,
                 database: Database,                 
                 columns : List[Column], 
                 **kwargs
                 ):
        self.name = name
        self.database = database
        self.schema = schema
        self.columns = columns
        self.kwargs = kwargs

    def __repr__(self):
        strings = [f"name={self.name}"]
        strings+= [f"{key}={self.kwargs[key]}" for key in self.kwargs]
        strings+= [f"schema={self.schema}"]
        strings+= [f"columns={self.columns}"]

        return "Table(\n" + ',\n'.join(strings) + "\n)"

    def __str__(self):
        return self.__repr__()

    def __getitem__(self,key):
        if key in __dict__:
            return self.__dict__[key]
        elif key in self.kwargs:
            return self.kwargs[key]
        else:
            raise KeyError(f"{key}")