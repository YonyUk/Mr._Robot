"""
This module defines all the necessary to manipulate querys
"""

class BooleanParser:
    """
    Class that parse a query in natural language and trasnform it to a boolean query
    """
    
    def parse_query(self):
        raise NotImplementedError()
    
    pass

class QueryParser(BooleanParser):
    
    
    _operators = ['and','or','not']
    
    def __init__(self):
        
        pass
    
    def _tokenize_query(self,query):
        """
        tokenize the given query and returns the respective boolean query
        """
        query = query.split(' ')
        i = 0
        while i < len(query):
            if len(query[i]) == 0:
                query.pop(i)
                continue
            i += 1
            pass
    
        return query
    
    def parse_query(self,query):
        
        query = self._tokenize_query(query)
        
        query_result = []
        last_token = None
        
        for token in query:
            
            if self._operators.count(token) == 0 and not last_token == None:
            
                if self._operators.count(last_token) == 0:
                    query_result.append('and')
                    query_result.append(token)
                    pass
                else:
                    query_result.append(token)
                    pass
            
                pass
            else:
                query_result.append(token)
                pass
            last_token = token
            pass
            
        return query_result
    
    pass