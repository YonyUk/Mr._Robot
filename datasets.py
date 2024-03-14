import json
import PyPDF2 as pdf
from os import listdir,getcwd,system
from pathlib import Path
from math import log10
from utils import splitBy
import fitz

"""
This module contains diferent retrieval information models for documents
"""

# globals variables definitions
global CONFIG

CONFIG = {}

class DataSet:
    """
    This class defines a dataset of documents
    """
    _files = None
    _path = None
    _path_worker = Path()
    def __init__(self,path=None):
        self._path = path
        self._path_worker = Path(path)
        if not path == None:
            self._files = listdir(path)
            pass
        else:
            self._files = listdir(getcwd())
            pass
        
        temp = []
        for i in range(len(self._files)):
            if CONFIG['files_to_omit'].count(self._files[i]) == 0:
                temp.append(self._files[i])
                pass
            pass
        
        self._files = temp
        pass
    
    pass

class TfIdfDataSet(DataSet):
    
    """
    This clas defines a dataset which's documents are tokenized and stored in a matrix M, the components are the
    tf-idf normalized values for each token of a document, the tokens are the words contained in that document
    """
    
    _dataset = None
    
    def __init__(self,dataset_structure):
        """
        dataset_structure most be an instance of TfIdfDataSetStorer
        """
        self._dataset = dataset_structure
        self._document_table = 'document'
        self._word_idf_table = 'word_idf'
        self._word_tf_table = 'word_tf'
        pass
    
    def GetWeight(self,word,document):
        """
        returns the tf-idf value normalized for the given value in this dataset in the given document
        document is the title of the document
        """
        self._dataset._database.open()
        
        # extraemos el id del documento
        doc = self._dataset._database.selectFieldsFrom(self._document_table,'DOCUMENT',TITLE=document)[0]['DOCUMENT']
        # extraemos el maximo tf del documento
        max_tf = self._dataset._database.max(self._word_tf_table,'TF',DOCUMENT=doc)
        # calculamos el tf normalizado
        result = self._dataset._database.selectFieldsFrom(self._word_tf_table,'TF',WORD=word,DOCUMENT=doc)
        
        if len(result) == 0:
            return 0
        
        tf = result[0]['TF']
        tf_normalized = tf / max_tf
        # extraemos el maximo idf del dataset
        max_idf = self._dataset._database.max(self._word_idf_table,'IDF',WORD=word)
        # extraemos el idf
        result = self._dataset._database.selectFieldsFrom(self._word_idf_table,'IDF',WORD=word)
        if len(result) == 0:
            return 0
        idf = result[0]['IDF']
        idf_normalized = idf / max_idf
        
        self._dataset._database.close()
        return tf_normalized * idf_normalized
    
    def GetWeightVector(self,word):
        """
        returns the tf x idf vector for the given word
        """
        
        self._dataset._database.open()
        
        files = self._dataset._database.selectFieldsFrom(self._document_table,'TITLE')
        
        vector = []
        for document in files:
            vector.append(self.GetWeight(word,document['TITLE']))
            pass
        
        
        return vector
    
    def GetDocumentTitleById(self,id_doc):
        self._dataset._database.open()
        title = self._dataset._database.selectFieldsFrom(self._document_table,'TITLE',DOCUMENT=id_doc)[0]['TITLE']
        self._dataset._database.close()
        return title
    
    pass

# load the configuration file
with open('config.json','r') as f:
    config = json.load(f)
    for key in config.keys():
        CONFIG[key] = config[key]
        pass
    pass
