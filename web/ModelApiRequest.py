import requests
import itertools

class ModelApiRequest:
    def __init__(self):
        self.url='http://ec2-107-23-121-67.compute-1.amazonaws.com:8501/v1/models/retail:predict'
        self.querydict = {
            'signature_name': 'serving_default',
            'inputs': {
                "context_item_id": [[]],
                "context_item_price": [[]],
                "context_item_department_id": [[]]
            }
        }

    def Predict(self, item_ids, item_prices, item_departments):
        for x in item_ids:
            self.querydict['inputs']['context_item_id'][0].append([x])
        for x in item_prices:
            self.querydict['inputs']['context_item_price'][0].append([x])
        for x in item_departments:
            self.querydict['inputs']['context_item_department_id'][0].append([x])

        response = requests.post(url=self.url, json=self.querydict)
        if response.status_code == 200:
            dictresp = response.json()
            output_2 = dictresp['outputs']['output_2']
            predictions = list(itertools.chain.from_iterable(output_2[0]))
            return predictions
        else:
            return None
