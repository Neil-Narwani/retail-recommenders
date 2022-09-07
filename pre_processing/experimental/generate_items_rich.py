from ast import Mult
import collections
from enum import unique
import pandas as pd
import tensorflow as tf
from sqlalchemy import create_engine
import argparse
from datetime import datetime
import random
import os
from fixstring import fix_string

output_dir = '../data/samples'
min_timeline_length = 3
max_context_length = 10
train_data_fraction = 0.8
OUTPUT_TRAINING_DATA_FILENAME = "train_transactions_rich.tfrecord"
OUTPUT_TESTING_DATA_FILENAME = "test_transactions_rich.tfrecord"
OUTPUT_ITEMDATA_FILENAME = "items.tfrecord"

class TransactionInfo(
    collections.namedtuple("TransactionInfo", 
    ["item_id", "timestamp", "description" , "price", "fullprice"])):
  """Data holder of basic information of an transaction."""
  __slots__ = ()

  def __new__(cls,
              item_id=0,
              timestamp=0,
              description = '', 
              price = 0.0,
              fullprice = 0.0):
    return super(TransactionInfo, cls).__new__(cls, item_id, timestamp, description, price, fullprice)

class ItemInfo(collections.namedtuple("ItemInfo", ["item_id", "description", "fullprice"])):
  """Data holder of basic item information"""
  __slots__ = ()

  def __new__(cls, item_id=0, description='', fullprice=0.0):
    return super(ItemInfo, cls).__new__(cls, item_id, description, fullprice)

def read_data(sqlconn): 
    print('Load duplicate customer map')
    entity_map_query = r"""
      select id as CustomerID, canon_id as CanonID from entity_map where cluster_score > 0.97;"""
    entity_map_df = pd.read_sql(entity_map_query, sqlconn)
    print('query transactions...')
    transactions_df = pd.read_sql(r"""
      select 
        transactionentry.ID,CustomerID, item.ID as ItemID, item.Description as Description, 
        TransactionTime, Quantity, Price, transactionentry.FullPrice as FullPrice
      from transactionentry
      inner join item on Item.ID = transactionentry.ItemID 
      where TransactionTime > '2018-01-01 00:00:00';
      """, sqlconn)
    
    print('Add timestamps')
    transactions_df['Timestamp'] = transactions_df['TransactionTime'].map(lambda x: x.timestamp())
    transactions_df.drop('TransactionTime', inplace=True, axis=1)
    print('Update duplicate entries')
    transactions_df = transactions_df.merge(entity_map_df, on='CustomerID', how='left')
    transactions_df['CustomerID'] = transactions_df['CanonID'].fillna(transactions_df['CustomerID'])
    transactions_df.drop('CanonID', inplace=True, axis=1)
    print('Query items')
    items_df = pd.read_sql("""
      select item.ID as ID, Description, FullPrice 
        from Item 
        where item.ID in (select distinct ItemID from transactionentry where TransactionTime > '2018-01-01 00:00:00') limit 25000;""", sqlconn)

    return transactions_df, items_df

def remove_last_item(alist, id):
  found = None
  for ndx in reversed(range(len(alist))):
    if alist[ndx].item_id == id:
      found = alist[ndx]
      break
  if found is not None:
    alist.remove(found)


def convert_to_timelines(transactions_df):
  """Covert to purchase histories."""
  timelines = collections.defaultdict(list)
  item_counts = collections.Counter()
  for ID,CustomerID, ItemID, Description, Quantity, Price, FullPrice, Timestamp in transactions_df.values:
    if Quantity < 0:
      remove_last_item(timelines[CustomerID], ItemID)
    else:
      timelines[CustomerID].append(
          TransactionInfo(item_id=ItemID, timestamp=Timestamp, price=Price, fullprice=FullPrice, description=Description))
    item_counts[ItemID] += 1
  # Sort per-user timeline by timestamp
  for (user_id, context) in timelines.items():
    context.sort(key=lambda x: x.timestamp)
    timelines[user_id] = context
  return timelines, item_counts

def generate_items_dict(items_df):
  """Generates movies dictionary from movies dataframe."""
  items_dict = {
      ID: ItemInfo(item_id=ID, description=Description, fullprice=FullPrice)
      for ID, Description, FullPrice in items_df.values
  }
  items_dict[0] = ItemInfo()
  return items_dict

def generate_examples_from_single_timeline(timeline,
                                           max_context_len=100):
  """Generate TF examples from a single user timeline.

  Generate TF examples from a single user timeline. Timeline with length less
  than minimum timeline length will be skipped. And if context user history
  length is shorter than max_context_len, features will be padded with default
  values.

  Args:
    timeline: The timeline to generate TF examples from.
    movies_dict: Dictionary of all MovieInfos.
    max_context_len: The maximum length of the context. If the context history
      length is less than max_context_length, features will be padded with
      default values.
    max_context_movie_genre_len: The length of movie genre feature.

  Returns:
    examples: Generated examples from this single timeline.
  """
  examples = []
  for label_idx in range(1, len(timeline)):
    start_idx = max(0, label_idx - max_context_len)
    context = timeline[start_idx:label_idx]
    # Pad context with out-of-vocab movie id 0.
    while len(context) < max_context_len:
      context.append(TransactionInfo())
    label_item_id = int(timeline[label_idx].item_id)
    context_item_id = [int(item.item_id) for item in context]
    context_item_price = [item.price for item in context]
    context_item_description = [tf.compat.as_bytes(item.description) for item in context]
    context_item_discount=[max(0, item.fullprice - item.price) for item in context]
    feature = {
        "context_item_id":
            tf.train.Feature(
                int64_list=tf.train.Int64List(value=context_item_id)),
        "context_item_description":
            tf.train.Feature(
                bytes_list = tf.train.BytesList(value=context_item_description)),
        "context_item_price":
            tf.train.Feature(
                float_list=tf.train.FloatList(value=context_item_price)),
         "context_item_discount":
            tf.train.Feature(
                float_list=tf.train.FloatList(value=context_item_discount)),                     
        "label_item_id":
            tf.train.Feature(
                int64_list=tf.train.Int64List(value=[label_item_id]))
    }
    tf_example = tf.train.Example(features=tf.train.Features(feature=feature))
    examples.append(tf_example)

  return examples

def generate_item_vocab(items_dict):
    items = []
    for itemid in items_dict.keys():
      description = tf.compat.as_bytes(fix_string(items_dict[itemid].description))
      fullprice = items_dict[itemid].fullprice
      feature = {
          "item_id":
              tf.train.Feature(
                  int64_list=tf.train.Int64List(value=[itemid])),
          "item_fullprice":
              tf.train.Feature(
                  float_list=tf.train.FloatList(value=[fullprice])),
          "item_description":
              tf.train.Feature(
                  bytes_list=tf.train.BytesList(value=[description])),
      }
      tf_example = tf.train.Example(features=tf.train.Features(feature=feature))
      items.append(tf_example)
    return items

def generate_examples_from_timelines(timelines,
                                     items_df,
                                     min_timeline_len=3,
                                     max_context_len=100,
                                     train_data_fraction=0.9,
                                     random_seed=None,
                                     shuffle=True):
  """Convert user timelines to tf examples.

  Convert user timelines to tf examples by adding all possible context-label
  pairs in the examples pool.

  Args:
    timelines: The user timelines to process.
    movies_df: The dataframe of all movies.
    min_timeline_len: The minimum length of timeline. If the timeline length is
      less than min_timeline_len, empty examples list will be returned.
    max_context_len: The maximum length of the context. If the context history
      length is less than max_context_length, features will be padded with
      default values.
    max_context_movie_genre_len: The length of movie genre feature.
    train_data_fraction: Fraction of training data.
    random_seed: Seed for randomization.
    shuffle: Whether to shuffle the examples before splitting train and test
      data.

  Returns:
    train_examples: TF example list for training.
    test_examples: TF example list for testing.
  """
  examples = []
  progress_bar = tf.keras.utils.Progbar(len(timelines))
  for timeline in timelines.values():
    if len(timeline) < min_timeline_len:
      progress_bar.add(1)
      continue
    single_timeline_examples = generate_examples_from_single_timeline(
        timeline=timeline,
        max_context_len=max_context_len)
    examples.extend(single_timeline_examples)
    progress_bar.add(1) 
  # Split the examples into train, test sets.
  if shuffle:
    random.seed(random_seed)
    random.shuffle(examples)
  last_train_index = round(len(examples) * train_data_fraction)

  train_examples = examples[:last_train_index]
  test_examples = examples[last_train_index:]
  return train_examples, test_examples

def write_tfrecords(tf_examples, filename):
  """Writes tf examples to tfrecord file, and returns the count."""
  with tf.io.TFRecordWriter(filename) as file_writer:
    length = len(tf_examples)
    progress_bar = tf.keras.utils.Progbar(length)
    for example in tf_examples:
      file_writer.write(example.SerializeToString())
      progress_bar.add(1)
    return length


def generate_datasets(sqlconn,
                      output_dir,
                      min_timeline_length,
                      max_context_length,
                      train_data_fraction=0.9,
                      train_filename=OUTPUT_TRAINING_DATA_FILENAME,
                      test_filename=OUTPUT_TESTING_DATA_FILENAME,
                      items_filename=OUTPUT_ITEMDATA_FILENAME):
  """Generates train and test datasets as TFRecord, and returns stats."""
  print("Reading data to dataframes.")
  transactions_df, items_df = read_data(sqlconn)
  print("Generating customer purchase histories.")
  timelines, item_counts = convert_to_timelines(transactions_df)
  print("Generating train and test examples.")
  train_examples, test_examples = generate_examples_from_timelines(
      timelines=timelines,
      items_df=items_df,
      min_timeline_len=min_timeline_length,
      max_context_len=max_context_length,
      train_data_fraction=train_data_fraction)
  print('Generating Item data')
  items_dict = generate_items_dict(items_df)
  items_set = generate_item_vocab(items_dict=items_dict)

  if not tf.io.gfile.exists(output_dir):
    tf.io.gfile.makedirs(output_dir)
  print("Writing generated training examples.")
  train_file = os.path.join(output_dir, train_filename)
  train_size = write_tfrecords(tf_examples=train_examples, filename=train_file)
  print("Writing generated testing examples.")
  test_file = os.path.join(output_dir, test_filename)
  test_size = write_tfrecords(tf_examples=test_examples, filename=test_file)
  print("Writing Item Data")
  item_file = os.path.join(output_dir, items_filename)
  items_size = write_tfrecords(tf_examples=items_set, filename=item_file)

  stats = {
      "train_size": train_size,
      "test_size": test_size,
      "train_file": train_file,
      "test_file": test_file,
      "item_size": items_size,
      "item_file": item_file
  }
  return stats



if __name__ == "__main__":

  parser = argparse.ArgumentParser(description='Extract customer and transaction data from mySQL db')
  parser.add_argument('--server', help='Address of mySQL server (default=localhost)')
  parser.add_argument('--username', help='Username to access server (default=sa)')
  parser.add_argument('--password', help='Password to access server (required)', required=True)
  parser.add_argument('--database', help='Database to access (default=RetailDB)')

  args = parser.parse_args()

  if args.database is None:
    database = 'RetailDB'
  else:
    database = args.database
    
  if args.username is None:
    username = 'sa'
  else:
    username = args.username

  if args.server is None:
    server = 'localhost'
  else:
    server = args.server

  print('Connect to Database...')
  connect_str = 'mysql+mysqlconnector://' + username + ':' + args.password + '@' + server +'/' + database
  alchemy_engine = create_engine(connect_str)
  sqlconn = alchemy_engine.connect()
  print('Generating Datasets...')
  stats = generate_datasets(sqlconn,
      output_dir=output_dir,
      min_timeline_length=min_timeline_length,
      max_context_length=max_context_length,
      train_data_fraction=train_data_fraction,
  )
  print(f"Generated dataset: {stats}")

