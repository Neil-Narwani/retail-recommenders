import numpy as np
import pandas as pd
import tensorflow as tf
import tensorflow_recommenders as tfrs

transactions = pd.read_csv('./transactions2000.csv', dtype=str)
labels = transactions.pop('Description')
samples = transactions.pop('CustomerID')
dataset = tf.data.Dataset.from_tensor_slices((samples,labels))