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

def read_data(sqlconn, startdate): 
    print('Load duplicate customer map')
    entity_map_query = r"""
      select id as CustomerID, canon_id as CanonID from entity_map where cluster_score > 0.97;"""
    entity_map_df = pd.read_sql(entity_map_query, sqlconn)
    print('query transactions...')
    query = f"""
      select 
        transactionentry.ID,CustomerID, ItemID, TransactionTime, Price, FullPrice, Quantity, DepartmentID, ReturnID, DiscountID
      from transactionentry
      where TransactionTime > '{startdate}';
      """
    transactions_df = pd.read_sql(query, sqlconn)
    print('Update duplicate entries')
    transactions_df = transactions_df.merge(entity_map_df, on='CustomerID', how='left')
    transactions_df['CustomerID'] = transactions_df['CanonID'].fillna(transactions_df['CustomerID'])
    transactions_df.drop('CanonID', inplace=True, axis=1)
    return transactions_df

def write_tfrecords(tf_examples, filename):
  """Writes tf examples to tfrecord file, and returns the count."""
  with tf.io.TFRecordWriter(filename) as file_writer:
    length = len(tf_examples)
    progress_bar = tf.keras.utils.Progbar(length)
    for example in tf_examples:
      file_writer.write(example.SerializeToString())
      progress_bar.add(1)
    return length

def generate_user_item_examples(transactions_df):
    examples = []
    train_data_fraction = 0.8
    print('Generate Examples')
    progress_bar = tf.keras.utils.Progbar(len(transactions_df))
    for _, transaction in transactions_df.iterrows():
        feature = {
            "user_id":
                tf.train.Feature(
                    int64_list=tf.train.Int64List(value=[int(transaction.CustomerID)])),
            "item_id":
                tf.train.Feature(
                    int64_list=tf.train.Int64List(value=[int(transaction.ItemID)])),
            "item_trantime" : tf.train.Feature(
                    int64_list=tf.train.Int64List(value=[int(transaction.TransactionTime.timestamp())])),
            "item_price":
                tf.train.Feature(
                    float_list=tf.train.FloatList(value=[transaction.Price])),
            "item_fullprice":
                tf.train.Feature(
                    float_list=tf.train.FloatList(value=[transaction.FullPrice])),
            "item_quantity":
                tf.train.Feature(
                    float_list=tf.train.FloatList(value=[transaction.Quantity])),
            "department_id":
                tf.train.Feature(
                    int64_list=tf.train.Int64List(value=[int(transaction.DepartmentID)])),
            "return_id":
                tf.train.Feature(
                    int64_list=tf.train.Int64List(value=[int(transaction.ReturnID)])),                  
            "discount_id":
                tf.train.Feature(
                    int64_list=tf.train.Int64List(value=[int(transaction.ReturnID)]))       
        }
        tf_example = tf.train.Example(features=tf.train.Features(feature=feature))
        examples.append(tf_example)
        progress_bar.add(1)
    # Split the examples into train, test sets.
    random.seed(None)
    random.shuffle(examples)
    last_train_index = round(len(examples) * train_data_fraction)

    train_examples = examples[:last_train_index]
    test_examples = examples[last_train_index:]
    return train_examples, test_examples

def read__customer_data(sqlconn): 
    print('query non-duplicate customers...')
    query = f"""
      select 
        ID, Zip, TotalSales, TotalSavings, TotalVisits
      from
        Customer
      where
        ID not in (select ID from entity_map where cluster_score > 0.97);
      """
    customers_df = pd.read_sql(query, sqlconn)
    print('query duplicate entries')
    query = f"""
      select
        ID, Zip, TotalSales, TotalSavings, TotalVisits
      from
        Customer
      where
        ID in (select distinct canon_id from entity_map);"""

    customers_df = pd.concat([customers_df, pd.read_sql(query, sqlconn)])
    return customers_df

def generate_customer_examples(customers_df):
    examples = []
    print('Generate Customer Examples')
    progress_bar = tf.keras.utils.Progbar(len(customers_df))
    for _, customer in customers_df.iterrows():
        feature = {
            "user_id":
                tf.train.Feature(
                    int64_list=tf.train.Int64List(value=[int(customer.ID)])),
           "zip_code":
                tf.train.Feature(
                    bytes_list=tf.train.BytesList(value=[tf.compat.as_bytes(str(customer.Zip))])),
            "total_visits":
                tf.train.Feature(
                    int64_list=tf.train.Int64List(value=[int(customer.TotalVisits)])),
            "total_sales":
                tf.train.Feature(
                    float_list=tf.train.FloatList(value=[customer.TotalSales])),
            "total_savings":
                tf.train.Feature(
                    float_list=tf.train.FloatList(value=[customer.TotalSavings]))
        }
        tf_example = tf.train.Example(features=tf.train.Features(feature=feature))
        examples.append(tf_example)
        progress_bar.add(1)
    return examples

def generate_datasets(connection,output_dir, startdate):
    train_filename = "train_user_item_transactions.tfrecord"
    test_filename = "test_user_item_transactions.tfrecord"
    customers_filename = "customers.tfrecord"

    transactions_df = read_data(connection, startdate)
    train_examples, test_examples = generate_user_item_examples(transactions_df=transactions_df)
    
    customers_df = read__customer_data(connection)
    customer_examples = generate_customer_examples(customers_df)
    
    if not tf.io.gfile.exists(output_dir):
        tf.io.gfile.makedirs(output_dir)  
    print("Writing generated training examples.")
    train_file = os.path.join(output_dir, train_filename)
    train_size = write_tfrecords(tf_examples=train_examples, filename=train_file)
    print("Writing generated testing examples.")
    test_file = os.path.join(output_dir, test_filename)
    test_size = write_tfrecords(tf_examples=test_examples, filename=test_file)
    print ("Writing customer records")
    customers_file = os.path.join(output_dir, customers_filename)
    customer_size = write_tfrecords(customer_examples, customers_file)
    stats = {
      "train_size": train_size,
      "test_size": test_size,
      "train_file": train_file,
      "test_file": test_file,
      "customers_size" : customer_size,
      "customers_file": customers_file
    }
    return stats

if __name__ == "__main__":
  
  parser = argparse.ArgumentParser(description='Extract customer and transaction data from mySQL db')
  parser.add_argument('--server', help='Address of mySQL server (default=localhost)', default='localhost')
  parser.add_argument('--username', help='Username to access server (default=sa)', default='sa')
  parser.add_argument('--password', help='Password to access server (required)', required=True)
  parser.add_argument('--database', help='Database to access (default=RetailDB)', default='RetailDB')
  parser.add_argument('--startdate',help='Start Date to pull transactions from. format: 2018-01-01 00:00:00', required=True)
  parser.add_argument('--outputdir', help='Directory to output to (default=../../data/samples)', default='../../data/samples')

  args = parser.parse_args()

  print('Connect to Database...')
  connect_str = 'mysql+mysqlconnector://' + args.username + ':' + args.password + '@' + args.server +'/' + args.database
  alchemy_engine = create_engine(connect_str)
  sqlconn = alchemy_engine.connect()
  print('Generating Datasets...')
  stats = generate_datasets(connection=sqlconn, 
      output_dir=args.outputdir,
      startdate=args.startdate
  )
  print(f"Generated dataset: {stats}")


  print('Generating Datasets...')

    
