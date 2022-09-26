import json
import requests
import itertools

url='http://ec2-3-88-109-135.compute-1.amazonaws.com:8501/v1/models/retail:predict'

with open('test.json', 'r') as testjson:
    data = json.load(testjson)

response = requests.post(url=url, json=data)
dictresp = response.json()
output_2 = dictresp['outputs']['output_2']
predictions = list(itertools.chain.from_iterable(output_2[0]))
print(predictions)