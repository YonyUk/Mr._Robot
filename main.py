from datetime import datetime
from datasets import TfIdfDataSet
from models import ExtendedBooleanModel
from os import system

model = ExtendedBooleanModel(2)

system('clear')
while True:
    results = model.Search(input('>>> '))
    system('clear')
    for result in results:
        print(result[0],"\t\t\t\t",result[1])
        pass
    pass