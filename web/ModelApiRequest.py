import requests
import itertools

class ModelApiRequest:
    def __init__(self):
        self.url='http://ec2-3-88-109-135.compute-1.amazonaws.com:8501/v1/models/retail:predict'
        self.querydict = {
            'signature_name': 'serving_default',
            'inputs': {
                "context_item_id": [[]],
                "context_item_department_id": [[]],
                "context_item_brand_code" : [[]]
            }
        }

    def Predict(self, item_ids, item_departments, item_brands):
        for x in item_ids:
            self.querydict['inputs']['context_item_id'][0].append([x])
        for x in item_departments:
            self.querydict['inputs']['context_item_department_id'][0].append([x])
        for x in item_brands:
            self.querydict['inputs']['context_item_brand_code'][0].append([x])

        response = requests.post(url=self.url, json=self.querydict)
        if response.status_code == 200:
            dictresp = response.json()
            output_2 = dictresp['outputs']['output_2']
            predictions = list(itertools.chain.from_iterable(output_2[0]))
            return predictions
        else:
            print(response.status_code)
            print(response.json())
            return []
