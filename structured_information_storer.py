import json
from pathlib import Path
from os import getcwd
from math import log10
from database import DataBase,SQLiteType,SQLiteModifiers
from utils import splitBy
from document_reader import ReadPlainText,ReadPdfText

"""
This module manage the cache of the documents in the dataset
"""
global CONFIG
CONFIG = {}

class DataStorer:
    
    """
    base class for all datastorer class
    """
    _path = None
    _database = None
    _created = False
    _dataset_path = None
    _files_existents = []
    _changed = False

    def __init__(self,database_path=None,dataset_path=None):
        if database_path == None:
            self._path = Path(getcwd())
            pass
        else:
            self._path = Path(database_path)
            if not self._path.exists():
                raise Exception(f'{database_path} is not a path in this filesystem')
            pass
        if dataset_path == None:
            self._dataset_path = Path(getcwd())
            pass
        else:
            self._dataset_path = Path(dataset_path)
            if not self._dataset_path.exists():
                raise Exception(f'{dataset_path} is not a path in this filesystem')
            pass
        pass
    
    @property
    def Path(self):
        return str(self._path.resolve())
    
    def changeCurrentPath(self,path):
        new_path = Path(path)
        if not new_path.exists():
            raise Exception('The given path does not exists')
        self._path = new_path
        pass
    
    def init(self,name):
        """
        Initialize the cache storer
        """
        self._files_existents = []
        local_path = self._path.joinpath(f'{name}.db')
        
        self._database = DataBase(name,str(self._path))
        if local_path.exists():
            print(f'WARNING: Already exists a database with that name in this location: {self._path.resolve()}')
            self._created = False
            pass
        else:
            self._database.create()
            self._created = True
            pass
        
        self._database.connect()
        pass
    
    def close(self):
        if not self._database == None:
            self._database.disconnect()
            pass
        self._database = None
        pass
    
    def open(self,name):
        """
        open the database in the current path with the given name
        """
        if not self._database == None:
            raise Exception('Already there is a connection with a database, close it first')
        location = self._path.joinpath(f'{name}.db')
        if not location.exists():
            raise Exception('There is not exists a database with the name {name}')
        
        self._database = DataBase(name,str(self._path))
        self._database.connect()
        pass
    
    def _load_document(self,file,path):
        """
        load the data from the given file
        path is an instance of pathlib.Path
        return True if the file cache already exists
        """
        raise NotImplementedError()
    
    def _load_documents(self,path):
        """
        load recursively all the document's data inside the given path
        path most be a pathlib.Path object
        """
        raise NotImplementedError()
    
    def _update_database(self):
        """
        update the database, if any document has been moved from the given path, them is deleted from the database
        and every data relationed with it is updated
        """
        raise NotImplementedError()
         
    def LoadData(self):
        """
        Load all the data from the given path an store it in the internal database
        path most be a pathlib.Path object
        """
        self._load_documents(self._dataset_path)
        self._update_database()
        pass
    
    pass

class TfIdfDataSetStorer(DataStorer):
    
    def __init__(self,database_path=None,dataset_path=None):
        super().__init__(database_path,dataset_path)
        self._document_table = 'document'
        self._word_tf_table = 'word_tf'
        self._word_idf = 'word_idf'
        pass

    def init(self,name):
        super().init(name)
        
        if self._created:
            self._database.open()
            # definicion del campo 'WORD'
            w_m = [SQLiteType.TEXT,SQLiteModifiers.NN]
            # definicion del campo 'TF'
            tf_m = [SQLiteType.INTEGER,SQLiteModifiers.NN]
            # definicion del campo 'DOCUMENT'
            doc_m = [SQLiteType.INTEGER,SQLiteModifiers.NN]
            # definicion del campo 'IDF'
            idf_m = [SQLiteType.REAL,SQLiteModifiers.NN]
            # definicion del campo 'DOCUMENT' de la tabla document
            document_m = [SQLiteType.INTEGER,SQLiteModifiers.NN,SQLiteModifiers.UT]
            # definicion del campo 'TITLE' de la tabla document
            title_m = [SQLiteType.TEXT,SQLiteModifiers.NN,SQLiteModifiers.UT]
            # definicion del campo 'LOCATION'
            location_m = [SQLiteType.TEXT,SQLiteModifiers.NN]

            # creamos las tablas
            self._database.createTable(self._word_tf_table,'WORD','DOCUMENT',WORD=w_m,TF=tf_m,DOCUMENT=doc_m)
            self._database.createTable(self._word_idf,'WORD',WORD=w_m,IDF=idf_m)
            self._database.createTable(self._document_table,'DOCUMENT',DOCUMENT=document_m,LOCATION=location_m,TITLE=title_m)

            self._database.close()
            pass
        pass
    
    def _read_document(self,document,doc_number,extension):
        """
        Read the given document with the given extension
        """
        self._database.open()
        
        if extension == 'pdf':
            
            ReadPdfText(document,self._database,doc_number,CONFIG)
            
            pass
        elif extension == 'txt':
            
            ReadPlainText(document,self._database,doc_number,CONFIG)
            
            pass
        else:
            self._database.close()
            raise Exception('Format not supported')
        
        self._database.close()
        
        pass
    
    def _load_document(self,file,path):
        
        pos = file.rindex('.')
        title,extension = file[:pos],file[pos + 1:]
        
        doc = path.joinpath(file)
        
        doc_number = None

        self._database.open()

        if len(self._database.selectFrom(self._document_table,TITLE=title)) > 0:
            doc_number = self._database.selectFieldsFrom(self._document_table,'DOCUMENT',TITLE=title)[0]['DOCUMENT']
            return True
        else:
            doc_number = len(self._database.selectFrom(self._document_table))
            self._database.insertInto(self._document_table,(doc_number,str(path.resolve()),title))
            pass
        self._database.close()
        
        self._read_document(doc,doc_number,extension)
        
        return False
    
    def _load_documents(self,path):
        
        if not path.exists():
            raise Exception(f'{path} is not a valid path in this filesystem')
        
        if path.is_file():
            
            file = path.name
            pos = file.rindex('.')
            title = file[:pos]
            
            if not title == '':
                self._files_existents.append(title)
                pass
            
            if CONFIG['files_to_omit'].count(file) == 0:
                print(f'Loading file {file}')
                cached = self._load_document(file,path.parent)
                if not cached:
                    print(file,'loaded')
                    pass
                else:
                    print(file,'cached')
                    pass
                pass
            pass
        else:
            for sub_path in path.iterdir():
                self._load_documents(sub_path)
                pass
            pass
        pass
    
    def _update_database(self):

        self._database.open()
        files_stored = self._database.selectFrom('document')
        
        if not len(self._files_existents) == len(files_stored):
            self._changed = True
            pass
        
        for file in self._files_existents:
            fi = None
            for f in files_stored:
                if f['TITLE'] == file:
                    fi = f
                    break
                pass
            if not fi == None:
                files_stored.remove(fi)
                pass
            pass
        
        for file in files_stored:
            doc = self._database.selectFieldsFrom('document','DOCUMENT',TITLE=file['TITLE'])[0]['DOCUMENT']
            self._database.deleteFrom('document',DOCUMENT=doc)
            self._database.deleteFrom('word_tf',DOCUMENT=doc)
            print(f'{file["TITLE"]} deleted')
            pass
        self._database.close()
        pass
    
    def _calculate_idf(self):
        """
        Calculate the idf for each word in the dataset
        """
        if self._changed:
            self._database.open()
            
            words_stored = self._database.selectFrom(self._word_idf)
            for word in words_stored:
                if self._database.count(self._word_tf_table,'WORD',WORD=word['WORD']) == 0:
                    self._database.deleteFrom(self._word_idf,WORD=word['WORD'])
                    pass
                pass
            
            documents_count = self._database.count('document','DOCUMENT')
            words = self._database.selectFrom(self._word_tf_table)
            for result in words:
                
                docs_with_word = self._database.count(self._word_tf_table,'WORD',WORD=result['WORD'])
                count = self._database.count(self._word_idf,'WORD',WORD=result['WORD'])
                
                if count == 0:
                    self._database.insertInto(self._word_idf,(result['WORD'],log10(documents_count/docs_with_word)))
                    pass
                elif not self._database.selectFieldsFrom(self._word_idf,'IDF',WORD=result['WORD'])[0]['IDF'] == log10(documents_count/docs_with_word):
                    self._database.updateTable(self._word_idf,('IDF',log10(documents_count/docs_with_word)))
                    pass
                
                pass
            
            self._database.close()
            self._changed = False
            pass
        
        pass

    def LoadData(self):
        super().LoadData()
        self._calculate_idf()
        pass
    
    pass

# load the configuration file
with open('config.json','r') as f:
    config = json.load(f)
    for key in config.keys():
        CONFIG[key] = config[key]
        pass
    pass