import tensorflow as tf
import tensorflow_recommenders as tfrs
from typing import Dict, Text

class CustomerPurchaseModel(tfrs.Model):
  # We derive from a custom base class to help reduce boilerplate. Under the hood,
  # these are still plain Keras Models.

  def __init__(
      self,
      customer_model: tf.keras.Model,
      item_model: tf.keras.Model,
      timestamp_model: tf.keras.Model,
      task: tfrs.tasks.Retrieval):
    super().__init__()

    # Set up user and movie representations.
    self.customer_model = customer_model
    self.item_model = item_model
    self.timestamp_model = timestamp_model
    # Set up a retrieval task.
    self.task = task

  def compute_loss(self, features: Dict[int, tf.Tensor], training=False) -> tf.Tensor:
    # Define how the loss is computed.
    customer_embeddings = self.customer_model(features[0])
    item_embeddings = self.item_model(features[1])
    return self.task(customer_embeddings, item_embeddings)

class UserModel(tf.keras.Model):

  def __init__(self):
    super().__init__()

    self.user_embedding = tf.keras.Sequential([
        user_id_lookup,
        tf.keras.layers.Embedding(user_id_lookup.vocab_size(), 32),
    ])
    self.timestamp_embedding = tf.keras.Sequential([
      tf.keras.layers.Discretization(timestamp_buckets.tolist()),
      tf.keras.layers.Embedding(len(timestamp_buckets) + 2, 32)
    ])
    self.normalized_timestamp = tf.keras.layers.Normalization(
        axis=None
    )

  def call(self, inputs):

    # Take the input dictionary, pass it through each input layer,
    # and concatenate the result.
    return tf.concat([
        self.user_embedding(inputs["user_id"]),
        self.timestamp_embedding(inputs["timestamp"]),
        tf.reshape(self.normalized_timestamp(inputs["timestamp"]), (-1, 1))
    ], axis=1)


class MovieModel(tf.keras.Model):

  def __init__(self):
    super().__init__()

    max_tokens = 10_000

    self.title_embedding = tf.keras.Sequential([
      movie_title_lookup,
      tf.keras.layers.Embedding(movie_title_lookup.vocab_size(), 32)
    ])
    self.title_text_embedding = tf.keras.Sequential([
      tf.keras.layers.TextVectorization(max_tokens=max_tokens),
      tf.keras.layers.Embedding(max_tokens, 32, mask_zero=True),
      # We average the embedding of individual words to get one embedding vector
      # per title.
      tf.keras.layers.GlobalAveragePooling1D(),
    ])

  def call(self, inputs):
    return tf.concat([
        self.title_embedding(inputs["movie_title"]),
        self.title_text_embedding(inputs["movie_title"]),
    ], axis=1)