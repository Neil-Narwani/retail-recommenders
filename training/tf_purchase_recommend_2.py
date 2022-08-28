import numpy as np
import tensorflow as tf
import tensorflow_recommenders as tfrs
from typing import Dict, Text
import pandas as pd

tranfile = './transactions2000.csv'
itemsfile = './items2000.csv'
customersfile = './anon_customers2000.csv'

print('Read Purchases CSV file...')
col_types  = [tf.int32, tf.string, tf.string, tf.string, tf.string, tf.float32, tf.float32, tf.float32, tf.string, tf.string, tf.string]
purchases = tf.data.experimental.make_csv_dataset(tranfile, batch_size=4096, column_defaults=col_types)
print('Read items CSV file...')
items =tf.data.experimental.make_csv_dataset(itemsfile, batch_size=4096)
items_list = pd.read_csv(itemsfile, dtype=str, header=0)
items_list = pd.unique(items_list['Description'])

customers_list = pd.read_csv(customersfile, dtype=str, header=0)
customers_list = customers_list['ID']

# Select the basic features.
print('Select Basic Features')
purchases = purchases.map(lambda x: {
    "Description": x["Description"],
    "CustomerID": x["CustomerID"]
})
items = items.map(lambda x: x["Description"])

print('Create Customer Vocabularies')
customer_ids_vocabulary = tf.keras.layers.StringLookup(vocabulary=customers_list, mask_token=None)
# customer_ids_vocabulary.adapt(purchases.map(lambda x: x["CustomerID"]))

print('Create Item Vocabularies')
items_vocabulary = tf.keras.layers.StringLookup(vocabulary=items_list, mask_token=None)
# items_vocabulary.adapt(items)

class MovieLensModel(tfrs.Model):
  # We derive from a custom base class to help reduce boilerplate. Under the hood,
  # these are still plain Keras Models.

  def __init__(
      self,
      user_model: tf.keras.Model,
      movie_model: tf.keras.Model,
      task: tfrs.tasks.Retrieval):
    super().__init__()

    # Set up user and movie representations.
    self.user_model = user_model
    self.movie_model = movie_model

    # Set up a retrieval task.
    self.task = task

  def compute_loss(self, feats: Dict[Text, tf.Tensor], training=False) -> tf.Tensor:
    # Define how the loss is computed.

    user_embeddings = self.user_model(feats['CustomerID'])
    movie_embeddings = self.movie_model(feats['Description'])

    return self.task(user_embeddings, movie_embeddings)

print("Create Models...")

user_model = tf.keras.Sequential([
    customer_ids_vocabulary,
    tf.keras.layers.Embedding(customer_ids_vocabulary.vocab_size(), 64)
])
movie_model = tf.keras.Sequential([
    items_vocabulary,
    tf.keras.layers.Embedding(items_vocabulary.vocab_size(), 64)
])

print("Define Retrieval Task")
# Define your objectives.
task = tfrs.tasks.Retrieval(metrics=tfrs.metrics.FactorizedTopK(items.batch(128).map(movie_model)))

# Create a retrieval model.
model = MovieLensModel(user_model, movie_model, task)
model.compile(optimizer=tf.keras.optimizers.Adagrad(0.5))

# Train for 3 epochs.
model.fit(purchases.batch(4096), epochs=3)

# Use brute-force search to set up retrieval using the trained representations.
index = tfrs.layers.factorized_top_k.BruteForce(model.user_model)
index.index_from_dataset(
    items.batch(100).map(lambda title: (title, model.movie_model(title))))

_,items = index(np.array([583625]))
print(f"Top 3 recommendations for user 583625: {items[0, :3]}")
model.save('./recommenders_model')