from ast import Mult
import collections
from enum import unique
from re import T
import pandas as pd
import tensorflow as tf
from sqlalchemy import create_engine
import argparse
from datetime import datetime
import random
import os
from fixstring import fix_string

output_dir = '../../data/samples'
min_timeline_length = 3
max_context_length = 10
data_fractions = [ 0.7, 0.2, 0.1 ] 
OUTPUT_TRAINING_DATA_FILENAME = "train_transactions.tfrecord"
OUTPUT_TESTING_DATA_FILENAME = "test_transactions.tfrecord"
OUTPUT_ITEMDATA_FILENAME = "items.tfrecord"
OUTPUT_DEPT_FILENAME= "departments.tfrecord"
OUTPUT_VALIDATION_FILENAME = "validation_transactions.tfrecord"

class TransactionInfo(
    collections.namedtuple("TransactionInfo", 
    ["item_id", "timestamp", "quantity", "price", "department_id", "discount_id", "return_id"])):
  """Data holder of basic information of an transaction."""
  __slots__ = ()

  def __new__(cls,
              item_id=0,
              timestamp=0, 
              quantity=0.0,
              price = 0.0,
              department_id = 0, discount_id=0, return_id=0):
    return super(TransactionInfo, cls).__new__(cls, item_id, timestamp, quantity,
                                         price, department_id,discount_id,return_id)

class ItemInfo(collections.namedtuple("ItemInfo", ["item_id", "description", "fullprice"])):
  """Data holder of basic item information"""
  __slots__ = ()

  def __new__(cls, item_id=0, description='', fullprice=0.0):
    return super(ItemInfo, cls).__new__(cls, item_id, description, fullprice)

class DepartmentInfo(collections.namedtuple("DepartmentInfo", ["dept_id", "dept_name"])):
  """Data holder of basic department information"""
  __slots__ = ()

  def __new__(cls, dept_id=0, dept_name=''):
    return super(DepartmentInfo, cls).__new__(cls, dept_id, dept_name)

def read_data(sqlconn, startdate): 
    print('Load duplicate customer map')
    entity_map_query = r"""
      select id as CustomerID, canon_id as CanonID from entity_map where cluster_score > 0.97;"""
    entity_map_df = pd.read_sql(entity_map_query, sqlconn)
    print('query transactions...')
    query = f"""
      select 
        transactionentry.ID,CustomerID, ItemID, TransactionTime, Quantity, Price, 
        FullPrice, DepartmentID, DiscountID, ReturnID
      from transactionentry 
      where TransactionTime > '{startdate}';
      """
    transactions_df = pd.read_sql(query, sqlconn)
    print('Add timestamps')
    transactions_df['Timestamp'] = transactions_df['TransactionTime'].map(lambda x: x.timestamp())
    transactions_df.drop('TransactionTime', inplace=True, axis=1)
    print('Update duplicate entries')
    transactions_df = transactions_df.merge(entity_map_df, on='CustomerID', how='left')
    transactions_df['CustomerID'] = transactions_df['CanonID'].fillna(transactions_df['CustomerID'])
    transactions_df.drop('CanonID', inplace=True, axis=1)
    print('Query items')
    items_df = pd.read_sql(f"""
      select item.ID as ID, Description, FullPrice 
        from Item 
        where item.ID in (select distinct ItemID from transactionentry where TransactionTime > '{startdate}') limit 25000;""", sqlconn)
    department_df = pd.read_sql("""
      select id, departmentname from department;
    """, sqlconn)
    return transactions_df, items_df, department_df


def convert_to_timelines(transactions_df):
  """Covert to purchase histories."""
  timelines = collections.defaultdict(list)
  item_counts = collections.Counter()
  for ID,CustomerID, ItemID, Quantity, Price, FullPrice, DepartmentID, DiscountID, ReturnID, Timestamp in transactions_df.values:
    timelines[CustomerID].append(
        TransactionInfo(item_id=ItemID, timestamp=Timestamp, quantity=Quantity, price=Price, department_id=int(DepartmentID), discount_id=int(DiscountID), return_id=int(ReturnID)))
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
    context_item_department = [item.department_id for item in context]
    context_discount_id = [item.discount_id for item in context]
    context_return_id=[item.return_id for item in context]
    context_item_quantity=[item.quantity for item in context]
    feature = {
        "context_item_id":
            tf.train.Feature(
                int64_list=tf.train.Int64List(value=context_item_id)),
         "context_item_quantity":
            tf.train.Feature(
                float_list=tf.train.FloatList(value=context_item_quantity)),             
        "context_item_price":
            tf.train.Feature(
                float_list=tf.train.FloatList(value=context_item_price)),
        "context_item_department_id":
            tf.train.Feature(
                int64_list=tf.train.Int64List(value=context_item_department)),
        "context_discount_id":
            tf.train.Feature(
                int64_list=tf.train.Int64List(value=context_discount_id)),
        "context_return_id":
            tf.train.Feature(
                int64_list=tf.train.Int64List(value=context_return_id)),
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

def generate_dept_vocab(department_df):
  departments = []
  for _, dept in department_df.iterrows():
    dept_name = tf.compat.as_bytes(fix_string(dept['departmentname']))
    feature = {
      "dept_id" : tf.train.Feature(int64_list=tf.train.Int64List(value=[dept['id']])),
      "dept_name": tf.train.Feature(bytes_list=tf.train.BytesList(value=[dept_name]))
    }
    tf_example = tf.train.Example(features=tf.train.Features(feature=feature))
    departments.append(tf_example)
  return departments

def generate_examples_from_timelines(timelines,
                                     min_timeline_len=3,
                                     max_context_len=100,
                                     train_data_fractions=[0.7, 0.2, 0.1],
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
  last_train_index = round(len(examples) * train_data_fractions[0])
  next_train_index = round(len(examples) * (train_data_fractions[0]+train_data_fractions[1]))

  train_examples = examples[:last_train_index]
  test_examples = examples[last_train_index:next_train_index]
  validation_examples = examples[next_train_index:]
  return train_examples, test_examples, validation_examples

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
                      start_date,
                      genauxdata,
                      train_data_fractions=[0.7, 0.2, 0.1],                      
                      train_filename=OUTPUT_TRAINING_DATA_FILENAME,
                      test_filename=OUTPUT_TESTING_DATA_FILENAME,
                      items_filename=OUTPUT_ITEMDATA_FILENAME,
                      validation_filename=OUTPUT_VALIDATION_FILENAME,
                      dept_filename=OUTPUT_DEPT_FILENAME):
  """Generates train and test datasets as TFRecord, and returns stats."""
  print("Reading data to dataframes.")
  transactions_df, items_df, department_df = read_data(sqlconn,start_date)
  print("Generating customer purchase histories.")
  timelines, item_counts = convert_to_timelines(transactions_df)
  print("Generating train and test examples.")
  train_examples, test_examples, validation_examples = generate_examples_from_timelines(
      timelines=timelines,
      min_timeline_len=min_timeline_length,
      max_context_len=max_context_length,
      train_data_fractions=train_data_fractions)

  if not tf.io.gfile.exists(output_dir):
    tf.io.gfile.makedirs(output_dir)
  print("Writing generated training examples.")
  train_file = os.path.join(output_dir, train_filename)
  train_size = write_tfrecords(tf_examples=train_examples, filename=train_file)
  print("Writing generated testing examples.")
  test_file = os.path.join(output_dir, test_filename)
  test_size = write_tfrecords(tf_examples=test_examples, filename=test_file)
  print("Writing generated validation examples.")
  validation_file = os.path.join(output_dir, validation_filename)
  validation_size = write_tfrecords(tf_examples=validation_examples, filename=validation_file)

  items_file = None
  items_size = None
  dept_file = None
  dept_size = None

  if genauxdata:
    print('Generating Item data')
    items_dict = generate_items_dict(items_df)
    items_set = generate_item_vocab(items_dict=items_dict)
    print('Genrating Department data')
    dept_examples = generate_dept_vocab(department_df=department_df)
    print("Writing Item Data")
    items_file = os.path.join(output_dir, items_filename)
    items_size = write_tfrecords(tf_examples=items_set, filename=items_file)
    print("Writing Department Data")
    dept_file = os.path.join(output_dir, dept_filename)
    dept_size = write_tfrecords(tf_examples=dept_examples,filename=dept_file)

  stats = {
      "train_size": train_size,
      "test_size": test_size,
      "train_file": train_file,
      "test_file": test_file,
      "validation_file": validation_file,
      "validation_size": validation_size,
      "item_size": items_size,
      "item_file": items_file,
      "dept_file": dept_file,
      "dept_size": dept_size
  }
  return stats



if __name__ == "__main__":

  parser = argparse.ArgumentParser(description='Extract customer and transaction data from mySQL db')
  parser.add_argument('--server', help='Address of mySQL server (default=localhost)', default='localhost')
  parser.add_argument('--username', help='Username to access server (default=sa)', default='sa')
  parser.add_argument('--password', help='Password to access server (required)', required=True)
  parser.add_argument('--database', help='Database to access (default=RetailDB)', default='RetailDB')
  parser.add_argument('--startdate',help='Start Date to pull transactions from. format: 2018-01-01 00:00:00', required=True)
  parser.add_argument('--outputdir', help='Directory to output to (default=../data/samples)', default='../data/samples')
  parser.add_argument('--filepostfix', help='String to add to filenames generated (default=None)', default=None)
  parser.add_argument('--nogenauxdata', help='Skip Generating item and dept data', action='store_false', default=True)
  args = parser.parse_args()
 
  if args.filepostfix is not None:
    OUTPUT_TRAINING_DATA_FILENAME = "train_transactions" + args.filepostfix + ".tfrecord"
    OUTPUT_TESTING_DATA_FILENAME = "test_transactions" + args.filepostfix + ".tfrecord"
    OUTPUT_VALIDATION_FILENAME = "validation_transactions" + args.filepostfix + ".tfrecord"
  

  print('Connect to Database...')
  connect_str = 'mysql+mysqlconnector://' + args.username + ':' + args.password + '@' + args.server +'/' + args.database
  alchemy_engine = create_engine(connect_str)
  sqlconn = alchemy_engine.connect()
  print('Generating Datasets...')
  stats = generate_datasets(sqlconn,
      output_dir=output_dir,
      min_timeline_length=min_timeline_length,
      max_context_length=max_context_length,
      train_data_fractions=data_fractions,
      start_date=str(args.startdate),
      genauxdata=args.nogenauxdata
  )
  print(f"Generated dataset: {stats}")

