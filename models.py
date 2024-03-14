from datasets import TfIdfDataSet
from structured_information_storer import TfIdfDataSetStorer
from parser import QueryParser
from utils import setQuery

"""
This module defines a set of models for information retrieval
"""

class InformationRetrievalModel:
    
    def Search(self,query):
        """
        return a list with the rank values for every document in the internal dataset of this model
        """
        
        raise NotImplementedError()
    
    pass

class ExtendedBooleanModel(InformationRetrievalModel):
    """
    This class represents the Extended Boolean Model for information retrieval
    """
    def __init__(self,norm=0,root_path=None,database_path=None,database_name=None):
        """
        norm is the norm degree that will be used to compute the similarity of one document
        with a given query
        """
        
        if root_path == None:
            self._root_path = './dataset'
            pass
        else:
            self._root_path = self._root_path
            pass
        
        if database_path == None:
            self._database_path = './data'
            pass
        else:
            self._database_path = database_path
            pass
        
        if database_name == None:
            self._database_name = 'database'
            pass
        else:
            self._database_name = database_name
            pass
        
        self._parser = QueryParser()
        self._dataset_structure = TfIdfDataSetStorer(self._database_path,self._root_path)
        self._dataset_structure.init(self._database_name)
        self._dataset_structure.LoadData()
        self._dataset = TfIdfDataSet(self._dataset_structure)
        self._norm = norm
        pass
    
    def _documents_similarity(self,word1,word2,operator,negateds=[]):
        """
        returns a dict with the similarity bettwen documents and query
        """
        documents_similarity = {}
        doc = 0
        w1_v = self._dataset.GetWeightVector(word1.lower())
        w2_v = self._dataset.GetWeightVector(word2.lower())
            
            
        for i in range(len(w1_v)):
            if not self._norm == 0:
                
                if operator == 'and':
                    if negateds.count(word1) > 0:
                        if negateds.count(word2) > 0:
                            documents_similarity[doc] = 1 - ((w1_v[i]**self._norm + w2_v[i]**self._norm)/2)**(1/self._norm)
                            pass
                        else:
                            documents_similarity[doc] = 1 - ((w1_v[i]**self._norm + (1 - w2_v[i])**self._norm)/2)**(1/self._norm)
                        pass
                    elif negateds.count(word2) > 0:
                        documents_similarity[doc] = 1 - (((1 - w1_v[i])**self._norm + w2_v[i]**self._norm)/2)**(1/self._norm)
                        pass
                    else:
                        documents_similarity[doc] = 1 - (((1 - w1_v[i])**self._norm + (1 - w2_v[i])**self._norm)/2)**(1/self._norm)
                        pass
                    pass
                elif operator == 'or':
                    if negateds.count(word1) > 0:
                        if negateds.count(word2) > 0:
                            documents_similarity[doc] = (((1 - w1_v[i])**self._norm + (1 - w2_v[i])**self._norm)/2)**(1/self._norm)
                            pass
                        else:
                            documents_similarity[doc] = (((1 - w1_v[i])**self._norm + w2_v[i]**self._norm)/2)**(1/self._norm)
                            pass
                        pass
                    elif negateds.count(word2) > 0:
                        documents_similarity[doc] = ((w1_v[i]**self._norm + (1 - w2_v[i])**self._norm)/2)**(1/self._norm)
                        pass
                    else:
                        documents_similarity[doc] = ((w1_v[i]**self._norm + w2_v[i]**self._norm)/2)**(1/self._norm)
                        pass
                    pass
                else:
                    raise NotImplementedError()
                pass
            else:
                raise NotImplementedError()
            
            doc += 1
            pass
         
        return documents_similarity
    
    def _documents_similarity_extended(self,word,operator,documents_similarty_values,negated=0):
        """
        actualiza el valor de similitud de un documento dado los valores actuales y una expansion a la consulta
        negated = 0 is that none of both members are negated
        negated = 1 is that the first member is negated
        negated = 2 is that the seconde member is negated
        negated = 3 is that both memebrs are negated
        """
        w_v = self._dataset.GetWeightVector(word.lower())
        
        for doc in documents_similarty_values.keys():
            
            if operator == 'and':
                if negated == 0:
                    documents_similarty_values[doc] = 1 - (((1 - documents_similarty_values[doc])**self._norm + (1 - w_v[doc]**self._norm))/2)**(1/self._norm)
                    pass
                elif negated == 1:
                    documents_similarty_values[doc] = 1 - ((documents_similarty_values[doc]**self._norm + (1 - w_v[doc])**self._norm)/2)**(self._norm)
                    pass
                elif negated == 2:
                    documents_similarty_values[doc] = 1 - (((1 - documents_similarty_values[doc])**self._norm + w_v[doc]**self._norm)/2)**(1/self._norm)
                    pass
                else:
                    documents_similarty_values[doc] = 1 -((documents_similarty_values[doc]**self._norm + w_v[doc]**self._norm)/2)**(1/self._norm)
                    pass
                pass
            elif operator == 'or':
                if negated == 0:
                    documents_similarty_values[doc] = ((documents_similarty_values[doc]**self._norm + w_v[doc]**self._norm)/2)**(1/self._norm)
                    pass
                elif negated == 1:
                    documents_similarty_values[doc] = (((1 - documents_similarty_values[doc])**self._norm + w_v[doc]**self._norm)/2)**(1/self._norm)
                    pass
                elif negated == 2:
                    documents_similarty_values[doc] = ((documents_similarty_values[doc]**self._norm + (1 - w_v[doc])**self._norm)/2)**(1/self._norm)
                    pass
                else:
                    documents_similarty_values[doc] = (((1 -documents_similarty_values[doc])**self._norm + (1 - w_v[doc])**self._norm)/2)**(1/self._norm)
                    pass
                pass
            else:
                raise NotImplementedError()
            
            pass
        return documents_similarty_values
    
    def _documents_similarity_extended_2(self,operator,d_s_v_1,d_s_v_2,negated=0):
        """
        same functionality that '_documents_similarity_extended' but take two query_values computated
        d_s_v_#: documents_similarity_values
        returns a dict with the similarity values computated
        """
        
        result = {}
        for doc in d_s_v_1.keys():
            if operator == 'and':
                
                if negated == 0:
                    result[doc] = 1 - (((1 - d_s_v_1[doc])**self._norm + (1 - d_s_v_2[doc])**self._norm)/2)**(1/self._norm)
                    pass
                elif negated == 1:
                    result[doc] = 1 - ((d_s_v_1[doc]**self._norm + (1 - d_s_v_2[doc])**self._norm)/2)**(1/self._norm)
                    pass
                elif negated == 2:
                    result[doc] = 1 - (((1 - d_s_v_1[doc])**self._norm + d_s_v_2[doc]**self._norm)/2)**(1/self._norm)
                    pass
                else:
                    result[doc] = 1 - ((d_s_v_1[doc]**self._norm + d_s_v_2[doc]**self._norm)/2)**(1/self._norm)
                    pass
            
                pass
            elif operator == 'or':
                
                if negated == 0:
                    result[doc] = ((d_s_v_1[doc]**self._norm + d_s_v_2[doc]**self._norm)/2)**(1/self._norm)
                    pass
                elif negated == 1:
                    result[doc] = (((1 - d_s_v_1[doc])**self._norm + d_s_v_2[doc]**self._norm)/2)**(1/self._norm)
                    pass
                elif negated == 2:
                    result[doc] = ((d_s_v_1[doc]**self._norm + (1 - d_s_v_2[doc])**self._norm)/2)**(1/self._norm)
                    pass
                else:
                    result[doc] = (((1 - d_s_v_1[doc])**self._norm + (1 - d_s_v_2[doc])**self._norm)/2)**(1/self._norm)
                    pass
                pass
            else:
                raise NotImplementedError()
            pass
        return result
    
    def _search(self,query_vector):
        documents_similarities = None
        start = 0
        if query_vector[0] == 'not' and len(query_vector) == 2:
            query_vector.append('and')
            query_vector.append(query_vector[0])
            query_vector.append(query_vector[1])
            pass
        if query_vector[0] == 'not':
            if query_vector[3] == 'not':
                documents_similarities = self._documents_similarity(query_vector[1],query_vector[4],query_vector[2],[query_vector[1],query_vector[4]])
                start = 5
                pass
            else:
                documents_similarities = self._documents_similarity(query_vector[1],query_vector[3],query_vector[2],[query_vector[1]])
                start = 4
                pass
            pass
        else:
            if query_vector[2] == 'not':
                documents_similarities = self._documents_similarity(query_vector[0],query_vector[3],query_vector[1],[query_vector[3]])
                start = 4
                pass
            else:
                documents_similarities = self._documents_similarity(query_vector[0],query_vector[2],query_vector[1])
                start = 3
                pass
            pass
        
        operator = None
        negated = 0
        last_token = None
        i = start
        while i < len(query_vector):
            if last_token == 'and' or last_token == 'or' or last_token == 'not':           
                if not query_vector[i] == 'not':
                    documents_similarities = self._documents_similarity_extended(query_vector[i],operator,documents_similarities,negated)
                    negated = 0
                    operator = None
                    pass
                else:
                    negated = 2
                    pass
                pass
            elif query_vector[i] == 'and' or query_vector[i] == 'or':
                operator = query_vector[i]
                pass
            elif query_vector[i] == 'not':
                negated = 2
                pass
            elif query_vector[i] == '(':
                end = query_vector.index(')',i)
                q = query_vector[i:end]
                value = self._search(q)
                documents_similarities = self._documents_similarity_extended_2(operator,documents_similarities,value,negated)
                i = end
                pass
            # Resta terminar la implementacion
            last_token = query_vector[i]
            i += 1
            pass
        
        return documents_similarities
    
    def _rank(self,documents):
        results = []
        for document in documents.keys():
            if not documents[document] == 0:
                results.append((self._dataset.GetDocumentTitleById(document),documents[document]))
                pass
            pass
        
        for i in range(len(results)):
            for j in range(i,len(results),1):
                if results[j][1] > results[i][1]:
                    temp = results[i]
                    results[i] = results[j]
                    results[j] = temp
                    pass
                pass
            pass
        return results
    
    def Search(self,query):
        """
        realiza la busqueda y devuelve un diccionario con los resultados
        """
        query = setQuery(query)
        query = query.replace('(',' ( ')
        query = query.replace(')', ' ) ')
        q = self._parser.parse_query(query.lower())
        similarities = self._search(q)
        ranking = self._rank(similarities)
        return ranking
    
    pass