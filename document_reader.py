from utils import splitBy
import fitz
from os import system

animation_bar = ['|','/','-','\\']

def ReadPlainText(document,database,doc_number,CONFIG):
    """
    Read a document in plain text and store the tf of each word in that document in the given database
    document -> the full path to the file
    database -> an DataBase like object
    doc_number -> the id of the file
    CONFIG -> the configuration to load the file
    """
    
    reader = open(str(document),'r')
            
    text = reader.read().lower()
    content = splitBy(text,CONFIG['characters_to_omit'])
    counter = 0
    for word in content:
        
        counter += 1
        points = ''
        
        for i in range(counter%100):
            points += '.'
            pass
        
        system('clear')
        print(f'Loading file {document} {animation_bar[counter%len(animation_bar)]} {points}')
        
        word_count = database.count('word_tf','WORD',DOCUMENT=doc_number,WORD=word)
        if word_count == 0:
            database.insertInto('word_tf',(word,1,doc_number))
            pass
        else:
            database.updateTable('word_tf',('TF',word_count + 1),DOCUMENT=doc_number,WORD=word)
            pass
        pass
    reader.close()
    pass

def ReadPdfText(document,database,doc_number,CONFIG):
    """
    Read a pdf document and store the tf of each word in that document in the given database
    document -> the full path to the file
    database -> an DataBase like object
    doc_number -> the id of the file
    CONFIG -> the configuration to load the file
    """
    
    document = fitz.open(str(document))
    
        
    for i in range(document.page_count):
        
        points = ''
        for j in range(i%100):
            points += '.'
            pass
        
        system('clear')
        print(f'Loading file {document} {animation_bar[i%len(animation_bar)]} {points}')    
        
        text = document.get_page_text(i).lower()
        content = splitBy(text,CONFIG['characters_to_omit'])
        for word in content:
            word_count = database.count('word_tf','WORD',DOCUMENT=doc_number,WORD=word)
            if word_count == 0:
                database.insertInto('word_tf',(word,1,doc_number))
                pass
            else:
                tf = database.selectFieldsFrom('word_tf','TF',WORD=word,DOCUMENT=doc_number)[0]['TF']
                database.updateTable('word_tf',('TF',tf + 1),DOCUMENT=doc_number,WORD=word)
                pass
            pass
        pass
    document.close()
    pass