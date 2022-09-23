from asyncio import tasks
from pyexpat import model
from typing import Dict, Text
import numpy as np
import tensorflow as tf
import tensorflow_recommenders as tfrs
import pandas as pd

tranfile = '../data/csv/transactions2500.csv'
itemsfile = '../data/csv/items2500.csv'
modelfile = '../data/test_model'
customerfile = '../data/csv/anon_customers2500.csv'

class CustomerPurchaseModel(tfrs.Model):
  # We derive from a custom base class to help reduce boilerplate. Under the hood,
  # these are still plain Keras Models.

  def __init__(
      self,
      customer_model: tf.keras.Model,
      item_model: tf.keras.Model,
      task: tfrs.tasks.Retrieval):
    super().__init__()

    # Set up user and movie representations.
    self.customer_model = customer_model
    self.item_model = item_model

    # Set up a retrieval task.
    self.task = task

  def compute_loss(self, features: Dict[int, tf.Tensor], training=False) -> tf.Tensor:
    # Define how the loss is computed.
    customer_embeddings = self.customer_model(features['CustomerID'])
    item_embeddings = self.item_model(features['ItemID'])

    return self.task(customer_embeddings, item_embeddings)

print('Read Purchases CSV file...')
col_types  = [tf.int32, tf.int32, tf.string, tf.string, tf.float32, tf.float32, tf.float32, tf.string, tf.string, tf.string]
purchases = tf.data.experimental.make_csv_dataset(tranfile, batch_size=1024, column_defaults=col_types)
print('Read items CSV file...')
items = tf.data.experimental.make_csv_dataset(itemsfile, batch_size=1024)
print('Get CustomerID Vocabulary')
customerids = pd.read_csv(customerfile)
customerids = customerids['ID']
print('Get ItemIDs')
itemids = pd.read_csv(itemsfile)
itemids = itemids['ID']

# Select the basic features.
purchases = purchases.map(lambda x: {
    "ItemID": x["ItemID"],
    "CustomerID": x["CustomerID"]
})
items = items.map(lambda x: x["ID"])

customer_ids_vocabulary = tf.keras.layers.IntegerLookup(vocabulary=customerids, output_mode='int')
items_vocabulary = tf.keras.layers.IntegerLookup(vocabulary=itemids, output_mode='int')

customer_model = tf.keras.Sequential([
    customer_ids_vocabulary,
    tf.keras.layers.Embedding(customer_ids_vocabulary.vocabulary_size(), 512, input_length=512)
])
item_model = tf.keras.Sequential([
    items_vocabulary,
    tf.keras.layers.Embedding(items_vocabulary.vocabulary_size(), 512, input_length=512)
])
print("Define Retrieval Task")
# Define your objectives.
task = tfrs.tasks.Retrieval(metrics=tfrs.metrics.FactorizedTopK(
    items.batch(128).map(item_model)
  )
)

# Create a retrieval model.
model = CustomerPurchaseModel(customer_model, item_model, task)
model.compile(optimizer=tf.keras.optimizers.Adagrad(0.5))

# Train for 3 epochs.
model.fit(purchases.batch(512), epochs=3)

# Use brute-force search to set up retrieval using the trained representations.
index = tfrs.layers.factorized_top_k.BruteForce(model.customer_model)
index.index_from_dataset(
    items.batch(128).map(lambda itemid: (itemid, model.item_model(itemid))))

_,items = index(np.array([583625]))
print(f"Top 3 recommendations for user 583625: {items[0, :3]}")
index.save(modelfile)

