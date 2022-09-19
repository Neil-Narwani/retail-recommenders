import os
import datetime
from typing import Dict, Text

import numpy as np
import tensorflow as tf
import tensorflow_datasets as tfds
import tensorflow_recommenders as tfrs
import yaml

#load training and test data
train_filename = "../data/samples/train_transactions.tfrecord"
train = tf.data.TFRecordDataset(train_filename)

test_filename = "../data/samples/test_transactions.tfrecord"
test = tf.data.TFRecordDataset(test_filename)

validation_filename = "../data/samples/validation_transactions.tfrecord"
validation = tf.data.TFRecordDataset(validation_filename)

items_filename = "../data/samples/items.tfrecord"
items_tf = tf.data.TFRecordDataset(items_filename)

dept_filename = "../data/samples/departments.tfrecord"
departments_tf = tf.data.TFRecordDataset(dept_filename)

feature_description = {
    'context_item_id': tf.io.FixedLenFeature([10], tf.int64, default_value=np.repeat(0, 10)),    
    'context_item_quantity': tf.io.FixedLenFeature([10], tf.float32, default_value=np.repeat(0, 10)),
    'context_item_price': tf.io.FixedLenFeature([10], tf.float32, default_value=np.repeat(0, 10)),    
    'context_item_department_id': tf.io.FixedLenFeature([10], tf.int64, default_value=np.repeat(0, 10)),
    'label_item_id': tf.io.FixedLenFeature([1], tf.int64, default_value=0),
}

def _parse_function(example_proto):
  return tf.io.parse_single_example(example_proto, feature_description)
def _map_function(x):
  return {
    "context_item_id": tf.strings.as_string(x["context_item_id"]),
    "context_item_price": x["context_item_price"],
    "context_item_department_id": x["context_item_department_id"],
    "label_item_id": tf.strings.as_string(x["label_item_id"])}
    
train_ds = train.map(_parse_function).map(_map_function)
test_ds = test.map(_parse_function).map(_map_function)
cached_train = train_ds.shuffle(10_000).batch(12800).cache()
cached_test = test_ds.batch(2560).cache()
cached_validation = validation.map(_parse_function).map(_map_function).batch(1280).cache()

item_feature_description = {
    'item_id': tf.io.FixedLenFeature([1], tf.int64, default_value=0),
    'item_fullprice': tf.io.FixedLenFeature([1], tf.float32, default_value=0.0 )
}

def item_parse_function(example_proto):
  return tf.io.parse_single_example(example_proto, item_feature_description)

items_ds = items_tf.map(item_parse_function).map(lambda x: {
    "item_id": tf.strings.as_string(x["item_id"]),
    "item_fullprice": x["item_fullprice"]
})
item_ids = items_ds.map(lambda x: x["item_id"]).batch(1_000)
unique_item_ids = np.unique(np.concatenate(list(item_ids)))
item_prices = np.concatenate(list(items_ds.map(lambda x: x["item_fullprice"]).batch(1000)))

dept_feature_description = {
  'dept_id' : tf.io.FixedLenFeature([1], tf.int64, default_value=0)
}
def dept_parse_function(example_proto):
  return tf.io.parse_single_example(example_proto, dept_feature_description)
department_ids = np.concatenate(list(departments_tf.map(dept_parse_function).map(lambda x: x['dept_id'])))

class ItemEmbeddingModel(tf.keras.Model):
  embedding_dimension = 32
  def __init__(self):
    super().__init__()

    self.item_embedding = tf.keras.Sequential([
      tf.keras.layers.StringLookup(vocabulary=unique_item_ids, mask_token=None, name='item_id_string_lookup'),
      tf.keras.layers.Embedding(len(unique_item_ids) + 1, self.embedding_dimension, name='item_id_embedding'),
      tf.keras.layers.GRU(self.embedding_dimension, name='item_id_rnn'),
    ], name='item_embedding')

    self.price_normalization = tf.keras.layers.Normalization(axis=-1)

    self.price_embedding = tf.keras.Sequential([
      self.price_normalization,
      tf.keras.layers.Embedding(len(item_prices)+1,output_dim=self.embedding_dimension, mask_zero=True),
      tf.keras.layers.GRU(self.embedding_dimension, name='item_price_rnn')
    ], name='price_embedding')    

    self.price_normalization.adapt(item_prices)

    self.dept_categorical = tf.keras.layers.IntegerLookup(max_tokens=len(department_ids)+1, vocabulary=department_ids)

    self.dept_embedding = tf.keras.Sequential([
      self.dept_categorical,
      tf.keras.layers.Embedding(len(department_ids)+1, output_dim=self.embedding_dimension, mask_zero=True),
      tf.keras.layers.GRU(self.embedding_dimension, name='dept_rnn')
    ])

  def call(self, features):
    return tf.concat([
        self.item_embedding(features["context_item_id"]),
        self.price_embedding(features["context_item_price"]),
        self.dept_embedding(features['context_item_department_id'])
    ], axis=1)

class DeepLayerModel(tf.keras.Model):
  """Model for encoding user queries."""

  def __init__(self, layer_sizes, embedding_model):
    """Model for encoding user queries.

    Args:
      layer_sizes:
        A list of integers where the i-th entry represents the number of units
        the i-th layer contains.
    """
    super().__init__()

    # We first use the user model for generating embeddings.
    self.embedding_model = embedding_model

    # Then construct the layers.
    self.dense_layers = tf.keras.Sequential()

    # Use the ReLU activation for all but the last layer.
    for layer_size in layer_sizes[:-1]:
      self.dense_layers.add(tf.keras.layers.Dense(layer_size, activation="relu"))

    # No activation for the last layer.
    for layer_size in layer_sizes[-1:]:
      self.dense_layers.add(tf.keras.layers.Dense(layer_size))

  def call(self, inputs):
    feature_embedding = self.embedding_model(inputs)
    return self.dense_layers(feature_embedding)

class RetrievalModel(tfrs.Model):
    def __init__(self, layer_sizes):
        super().__init__()
        if layer_sizes is not None:
            self._query_model = DeepLayerModel(layer_sizes, ItemEmbeddingModel())
        else:
            self._query_model = DeepLayerModel([32], embedding_model=ItemEmbeddingModel())
        self._candidate_model = tf.keras.Sequential([
            tf.keras.layers.StringLookup(vocabulary=unique_item_ids, mask_token=None, name='candidate_itemid_lookup'),
            tf.keras.layers.Embedding(len(unique_item_ids) + 1, 32, name='candidate_embedding_lookup'),
            ], name='candidate_model')
        metrics = tfrs.metrics.FactorizedTopK(candidates=items_tf.batch(128).map(self._candidate_model))
        self._task = tfrs.tasks.Retrieval(metrics=metrics)

    def compute_loss(self, features, training=False):
        item_history = {
            "context_item_id": features["context_item_id"],
            "context_item_price": features["context_item_price"],
            "context_item_department_id" : features["context_item_department_id"]}   
        next_item_label = features["label_item_id"]

        query_embedding = self._query_model(item_history)       
        candidate_embedding = self._candidate_model(next_item_label)

        return self._task(query_embedding, candidate_embedding, compute_metrics=not training)

if __name__ == "__main__":
    with open('experiment_config.yaml', 'r') as stream:
        config = yaml.safe_load(stream)

    run_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    log_base = f"../logs/{run_time}"

    for experiment in config['experiments']:
        log_dir = os.path.join(log_base, experiment['name'])
        # tensorboard_callback = tf.keras.callbacks.TensorBoard(log_dir=log_dir, histogram_freq=1)
        model = RetrievalModel(experiment['deep_layers'])
        model.compile(optimizer=tf.keras.optimizers.Adagrad(learning_rate=experiment['learning_rate']))
        # history = model.fit(cached_train, epochs=experiment['epochs'], callbacks=[tensorboard_callback], validation_data=cached_validation)
        history = model.fit(cached_train, epochs=experiment['epochs'], validation_data=cached_validation)
        experiment['history'] = history.history
        experiment['evaluation'] = model.evaluate(cached_test, return_dict=True)


    output_file = os.path.join(log_base, 'results.yaml')
    with open('../logs/initial_tests_results.yaml', 'w') as stream:
      yaml.dump(config, stream)