import json
import requests

url='http://ec2-3-94-210-245.compute-1.amazonaws.com:8501/v1/models/retail:predict'

with open('test.json', 'r') as testjson:
    data = json.load(testjson)

response = requests.post(url=url, json=data)

print(response)
