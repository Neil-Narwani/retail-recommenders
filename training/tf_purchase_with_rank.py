import numpy as np
import tensorflow as tf
import tensorflow_recommenders as tfrs
import pandas as pd
from datetime import datetime
from tf_convenience_models import CustomerPurchaseModel

tranfile = './trans3500.csv'
itemsfile = './items3500.csv'
modelfile = './recommenders_model'
customerfile = './anon_customers3500.csv'

print('Read Purchases CSV file...')
transactions = pd.read_csv(tranfile, dtype=str)
# transactions['Timestamp'] = transactions['TransactionTime'].map(lambda x: datetime.strptime(x,'%Y-%m-%d %H:%M:%S').timestamp())
# timestamps = transactions.pop['Timestamp']
labels = transactions.pop('Description')
samples = transactions.pop('CustomerID')
purchases = tf.data.Dataset.from_tensor_slices((samples,labels))
print('Read items CSV file...')
itemraw = pd.read_csv(itemsfile, dtype=str)
descriptions = itemraw.pop('Description')
items = tf.data.Dataset.from_tensor_slices(descriptions)

tf.random.set_seed(42)
shuffled = purchases.shuffle(1000_000, seed=42, reshuffle_each_iteration=False)

train = shuffled.take(800_000)
test = shuffled.skip(800_000).take(200_000)

item_descriptions = purchases.batch(1_000_000).map(lambda x: x["Description"])
customer_ids = purchases.batch(1_000_000).map(lambda x: x["CustomerID"])

unique_items = np.unique(np.concatenate(list(item_descriptions)))
unique_customer_ids = np.unique(np.concatenate(list(customer_ids)))


class RankingModel(tf.keras.Model):

  def __init__(self):
    super().__init__()
    embedding_dimension = 32

    # Compute embeddings for users.
    self.user_embeddings = tf.keras.Sequential([
      tf.keras.layers.StringLookup(
        vocabulary=unique_customer_ids, mask_token=None),
      tf.keras.layers.Embedding(len(unique_customer_ids) + 1, embedding_dimension)
    ])

    # Compute embeddings for movies.
    self.movie_embeddings = tf.keras.Sequential([
      tf.keras.layers.StringLookup(
        vocabulary=unique_items, mask_token=None),
      tf.keras.layers.Embedding(len(unique_items) + 1, embedding_dimension)
    ])

    # Compute predictions.
    self.ratings = tf.keras.Sequential([
      # Learn multiple dense layers.
      tf.keras.layers.Dense(256, activation="relu"),
      tf.keras.layers.Dense(64, activation="relu"),
      # Make rating predictions in the final layer.
      tf.keras.layers.Dense(1)
  ])

  def call(self, inputs):

    user_id, movie_title = inputs

    user_embedding = self.user_embeddings(user_id)
    movie_embedding = self.movie_embeddings(movie_title)

    return self.ratings(tf.concat([user_embedding, movie_embedding], axis=1))



print('Create Customer Vocabularies')
customer_ids_vocabulary = tf.keras.layers.StringLookup(mask_token=None)
customer_ids_vocabulary.adapt(train.map(lambda x,y: x))

print('Create Item Vocabularies')
items_vocabulary = tf.keras.layers.StringLookup(mask_token=None, vocabulary=pd.unique(descriptions))
items_vocabulary.adapt(items)

print("Create Models...")
customer_model = tf.keras.Sequential([
    customer_ids_vocabulary,
    tf.keras.layers.Embedding(customer_ids_vocabulary.vocabulary_size(), 64)
])
item_model = tf.keras.Sequential([
    items_vocabulary,
    tf.keras.layers.Embedding(items_vocabulary.vocabulary_size(), 64),
])
print("Define Retrieval Task")
# Define your objectives.
task = tfrs.tasks.Retrieval(metrics=tfrs.metrics.FactorizedTopK(items.batch(128).map(item_model)))

print('Compile and fit our model')
model = CustomerPurchaseModel(customer_model, item_model, task)
model.compile(optimizer=tf.keras.optimizers.Adagrad(0.5))

# Train for 3 epochs.
model.fit(purchases.batch(512), epochs=3)

# Use brute-force search to set up retrieval using the trained representations.
index = tfrs.layers.factorized_top_k.BruteForce(model.customer_model)
index.index_from_dataset(
    items.batch(128).map(lambda itemid: (itemid, model.item_model(itemid))))

_,items = index(np.array(['583625']))
print(f"Top 5 recommendations for user 583625: {items[0, :5]}")

# model.save(modelfile)