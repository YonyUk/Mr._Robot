import sqlite3
from pathlib import Path
from os import listdir,getcwd
from enum import Enum

"""
This module manage the cache data using the library sqlite3 and contains definitions to interact with
sqlite databases
"""
class SQLiteType(Enum):
    
    """
    The data types supported for sqlite databases
    """
    
    TEXT = 'TEXT'
    
    NUMERIC = 'NUMERIC'
    
    REAL = 'REAL'
    
    INTEGER = 'INTEGER'
    
    pass

class SQLiteModifiers(Enum):
    
    """
    The modifiers supported for the fields in sqlite databases
    NN: NOT NULL
    PK: PRIMARY KEY
    AI: AUTO INCREMENT
    UT: UNIQUE
    """
    
    NN = 'NOT NULL'
    
    PK = 'PRIMARY KEY'
    
    AI = 'AUTO INCREMENT'
    
    UT = 'UNIQUE'
    
    pass

"""
this module is for store all the pre-computed values for the model
"""

def _dict_factory(cursor,row):
    """
    convert a row result to a dict with keys equals to the fields of the database
    """
    row_result = {}
    for idx ,col in enumerate(cursor.description):
        row_result[col[0]] = row[idx]
        pass
    return row_result

class DataBase:
    """
    class that will manage the database
    """
    
    _path = None
    _name = None
    _cursor = None
    _connection = None
    _verified = False
    def __init__(self,name,path=None):
        if name == None or len(name) == 0:
            raise Exception("name can't be empty or None value")
        self._name = name
        if path == None:
           self._path = Path(getcwd())
           pass
        else:
           self._path = Path(path)
           pass
        pass
    
    @property
    def Exists(self):
        databases = listdir(str(self._path))
        if databases.count(f'{self._name}.db') == 0:
            return False
        return True
    
    def create(self):
        database_path = str(self._path.joinpath(f'{self._name}.db'))
        conn = sqlite3.connect(database_path)
        conn.close()
        pass
    
    def connect(self):
        """
        open the connection to the database that this isntance represents
        """
        database_path = str(self._path.joinpath(f'{self._name}.db'))
        if not self.Exists:
            raise Exception(f"There is not exists a database named '{self._name}' in path {self._path}")
        self._connection = sqlite3.connect(database_path)
        self._connection.row_factory = _dict_factory
        pass
    
    def disconnect(self):
        if not self._cursor == None:
            self._cursor.close()
            self._cursor = None
            pass
        
        self._connection.close()
        self._connection = None
        pass

    def open(self):
        """
        set the cursor to work with the database
        """
        if self._connection == None:
            raise Exception('The connection most be opened first')
        self._cursor = self._connection.cursor()
        pass

    def close(self):
        self._cursor.close()
        self._cursor = None
        self._connection.commit()
        pass

    def createTable(self,table_name,*primary_key_names,**fields):
        """
        Create a table in the database
        primary_key_name: field that will be the primary key
        fields: the values most be a list of string and the first value of each list most be the type
        of the field, else an exception will raised
        """
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        
        query = f'CREATE TABLE \"{table_name}\" (\n'
        
        for field in fields.keys():
            query += f'\"{field}\"'
            type_given = False
            for Type in SQLiteType:
                if fields[field][0] == Type:
                    type_given = True
                    break
                pass
            
            if not type_given:
                raise Exception(f'No type given for the field {field}')
            
            for val in fields[field]:
                query += f' {val.value}'
                pass
            
            query += ',\n'
            pass
        
        query += f'PRIMARY KEY(\"{primary_key_names[0]}\"'
        for i in range(1,len(primary_key_names),1):
            query += f',"{primary_key_names[i]}"'
            pass
        
        query += f')\n);'
        
        self._cursor.execute(query)
        
        pass
    
    def deleteTable(self,table_name):
        """
        Delete the given table from the database
        """
        
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        self._cursor.execute(f'DROP TABLE {table_name}')
        pass
    
    def insertInto(self,table_name,data):
        """
        insert the given data into the given table of the database
        data most be a tuple
        """
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be open')
        if not type(data) == tuple:
            if type(data) == str:
                self._cursor.execute(f'insert or ignore into {table_name} values ("{data}")')
                pass
            else:
                self._cursor.execute(f'insert or ignore into {table_name} values ({data})')
                pass
            pass
        else:
            self._cursor.execute(f'insert or ignore into {table_name} values {data}')
            pass
        pass
    
    def deleteFrom(self,table_name,**conditions):
        """
        Delete data from the given table having the given conditions
        """
        
        if self._connection == None:
            raise Exception('The connection most be open')
        
        if self._cursor == None:
            raise Exception('The database most be opened')
        
        if len(conditions.keys()) == 0:
            self._cursor.execute(f'DELETE FROM {table_name}')
            pass
        else:
            query = f'DELETE FROM {table_name} WHERE'

            first_condition = list(conditions.keys())[0]
            
            if type(conditions[first_condition]) == str:
                query += f' {first_condition} = "{conditions[first_condition]}"'
                pass
            else:
                query += f' {first_condition} = {conditions[first_condition]}'
                pass
            
            for condition in conditions.keys():
                if not condition == first_condition:
                    if type(conditions[condition]) == str:
                        query += f' AND {condition} =  "{conditions[condition]}"'
                        pass
                    else:
                        query += f' AND {condition} =  {conditions[condition]}'
                        pass
                    pass
                pass
            query += ';'
            self._cursor.execute(query)
            pass
        pass
    
    def selectFrom(self,table_name,**conditions):
        """
        Returns a list with the data selected from the given table
        """
        
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        
        if len(conditions.keys()) == 0:
            self._cursor.execute(f'SELECT * FROM {table_name}')
            return self._cursor.fetchall()
        
        query = f'SELECT * FROM {table_name} WHERE'
        first_condition = list(conditions.keys())[0]
        
        if type(conditions[first_condition]) == str:
            query += f' {first_condition} = "{conditions[first_condition]}"'
            pass
        else:
            query += f' {first_condition} = {conditions[first_condition]}'
            pass
        
        for condition in conditions.keys():
            if not condition == first_condition:
                if type(conditions[condition]) == str:
                    query += f' AND {condition} = "{conditions[condition]}"'
                    pass
                else:
                    query += f' AND {condition} = {conditions[condition]}'
                    pass
                pass
            pass
        self._cursor.execute(query)
        
        return self._cursor.fetchall()
    
    def selectFieldsFrom(self,table_name,*fields,**conditions):
        """
        select the given fields from the given table
        """
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        if len(fields) == 0:
            raise Exception('There most be a subfield to select')
        
        query = f'SELECT {fields[0]}'
        
        for i in range(1,len(fields),1):
            query += f',{fields[i]}'
            pass
        
        query += f' FROM {table_name}'
        
        if len(conditions.keys()) == 0:
            self._cursor.execute(query)
            return self._cursor.fetchall()
        
        first_condition = list(conditions.keys())[0]
        if type(conditions[first_condition]) == str:
            query += f' WHERE {first_condition} = "{conditions[first_condition]}"'
            pass
        else:
            query += f' WHERE {first_condition} = {conditions[first_condition]}'
            pass
        
        for condition in conditions.keys():
            if not condition == first_condition:
                if type(conditions[condition]) == str:
                    query += f' AND {condition} = "{conditions[condition]}"'
                    pass
                else:
                    query += f' AND {condition} = {conditions[condition]}'
                    pass
                pass
            pass
        self._cursor.execute(query)
        return self._cursor.fetchall()
             
    def deleteFromVerbose(self,table_name,condition):
        """
        Delete data from the given table having the verbose condition given
        condition most be given in SQL format
        """
        
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        
        self._cursor.execute(f'DELETE FROM {table_name} WHERE {condition}')
        pass
    
    def selectFromVerbose(self,table_name,condition):
        """
        Select data from the given database having the verbose condition
        condition most be given in SQL format
        """
        
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        
        self._cursor.execute(f'SELECT * FROM {table_name} WHERE {condition}')
        return self._cursor.fetchall()
    
    def updateTable(self,table_name,*values,**conditions):
        """
        updates the database with the given values
        values's items most be a tuple field_name,new_value
        """
        
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        if len(values) == 0:
            raise Exception('There most be a fields subselection')
        if len(conditions.keys()) == 0:
            raise Exception('Ther most be a condition to update the database')
        
        if type(values[0][1]) == str:
            query = f'UPDATE {table_name} SET {values[0][0]}="{values[0][1]}"'
            pass
        else:
            query = f'UPDATE {table_name} SET {values[0][0]}={values[0][1]}'
            pass
        for i in range(1,len(values),1):
            if type(values[i][1]) == str:
                query += f',{values[i][0]}="{values[i][1]}"'
                pass
            else:
                query += f',{values[i][0]}={values[i][1]}'
                pass
                
            pass
        first_condition = list(conditions.keys())[0]
        if type(conditions[first_condition]) == str:
            query += f' WHERE {first_condition}="{conditions[first_condition]}"'
            pass
        else:
            query += f' WHERE {first_condition}={conditions[first_condition]}'
            pass
        for condition in conditions.keys():
            if not condition == first_condition:
                if type(conditions[condition]) == str:
                    query += f' AND {condition} = "{conditions[condition]}"'
                    pass
                else:
                    query += f' AND {condition} = {conditions[condition]}'
                    pass
                pass
            pass
        self._cursor.execute(query)
        pass
    
    def updateTableVerbose(self,table_name,condition,**values):
        """
        Update the given table on rows where the condition eval true with the new values
        condition most be given in SQL format
        """
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        if len(values.keys()) == 0:
            raise Exception('There most be a fields subselection to update')
        
        query = f'UPDATE {table_name} SET '
        first_field = list(values.keys())[0]
        if type(values[first_field]) == str:
            query += f'{first_field}="{values[first_field]}"'
            pass
        else:
            query += f'{first_field}={values[first_field]}'
            pass
        
        for field in values.keys():
            if not field == first_field:
                if type(values[field]) == str:
                    query += f',{field}="{values[field]}"'
                    pass
                else:
                    query += f',{field}={values[field]}'
                    pass
                pass
            pass
        
        query += f' WHERE {condition}'
        self._cursor.execute(query)
        
        pass
    
    def count(self,table_name,field,**conditions):
        """
        returns the count of rows with different values of the given field
        """
        
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        
        if len(conditions.keys()) == 0:
            self._cursor.execute(f'SELECT COUNT({field}) FROM {table_name}')
            pass
        else:
            query = f'SELECT COUNT({field}) FROM {table_name}'
            first_condition = list(conditions.keys())[0]
            if type(conditions[first_condition]) == str:
                query += f' WHERE {first_condition}="{conditions[first_condition]}"'
                pass
            else:
                query += f' WHERE {first_condition}={conditions[first_condition]}'
                pass
            
            for condition in conditions.keys():
                if not condition == first_condition:
                    if type(conditions[condition]) == str:
                        query += f' AND {condition}="{conditions[condition]}"'
                        pass
                    else:
                        query += f' AND {condition}={conditions[condition]}'
                        pass
                    pass
                pass
            self._cursor.execute(query)
            pass
        return self._cursor.fetchall()[0][f'COUNT({field})']
    
    def countVerbose(self,table_name,field,condition):
        """
        return the count of rows with differents values of field with the given condition
        """
        
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        
        query = f'SELECT COUNT({field}) FROM {table_name} WHERE {condition}'
        self._cursor.execute(query)
        return self._cursor.fetchall()[0][f'COUNT({field})']
        
        pass
    
    def max(self,table_name,field,**conditions):
        """
        return the max value of a given field in the especified table
        """
        
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        
        if len(conditions.keys()) == 0:
            self._cursor.execute(f'SELECT MAX({field}) FROM {table_name}')
            pass
        else:
            query = f'SELECT MAX({field}) FROM {table_name}'
            first_condition = list(conditions.keys())[0]
            if type(conditions[first_condition]) == str:
                query += f' WHERE {first_condition}="{conditions[first_condition]}"'
                pass
            else:
                query += f' WHERE {first_condition}={conditions[first_condition]}'
                pass
            
            for condition in conditions.keys():
                if not condition == first_condition:
                    if type(conditions[condition]) == str:
                        query += f' AND {condition}="{conditions[condition]}"'
                        pass
                    else:
                        query += f' AND {condition}={conditions[condition]}'
                        pass
                    pass
                pass
            self._cursor.execute(query)
            pass
        return self._cursor.fetchall()[0][f'MAX({field})']
    
    def maxVerbose(self,table_name,field,condition):
        """
        return s the max value of the given field in th especified database with the given condition
        """
        
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        
        query = f'SELECT MAX({field}) FROM {table_name} WHERE {condition}'
        self._cursor.execute(query)
        return self._cursor.fetchall()[0][f'MAX({field})']
        
    def min(self,table_name,field,**conditions):
        """
        return the max value of a given field in the especified table
        """
        
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        
        if len(conditions.keys()) == 0:
            self._cursor.execute(f'SELECT MIN({field}) FROM {table_name}')
            pass
        else:
            query = f'SELECT MIN({field}) FROM {table_name}'
            first_condition = list(conditions.keys())[0]
            if type(conditions[first_condition]) == str:
                query += f' WHERE {first_condition}="{conditions[first_condition]}"'
                pass
            else:
                query += f' WHERE {first_condition}={conditions[first_condition]}'
                pass
            
            for condition in conditions.keys():
                if not condition == first_condition:
                    if type(conditions[condition]) == str:
                        query += f' AND {condition}="{conditions[condition]}"'
                        pass
                    else:
                        query += f' AND {condition}={conditions[condition]}'
                        pass
                    pass
                pass
            self._cursor.execute(query)
            pass
        return self._cursor.fetchall()[0][f'MIN({field})']
        
    def minVerbose(self,table_name,field,condition):
        """
        return s the max value of the given field in th especified database with the given condition
        """
        
        if self._connection == None:
            raise Exception('The connection most be open')
        if self._cursor == None:
            raise Exception('The database most be opened')
        
        query = f'SELECT MIN({field}) FROM {table_name} WHERE {condition}'
        self._cursor.execute(query)
        return self._cursor.fetchall()[0][f'MIN({field})']
    pass