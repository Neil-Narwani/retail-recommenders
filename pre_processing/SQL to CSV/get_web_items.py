from random import sample
import argparse
from fixstring import fix_string
import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description='Extract customer and transaction data from mySQL db')
parser.add_argument('--server', help='Address of mySQL server (default=localhost)', default='localhost')
parser.add_argument('--username', help='Username to access server (default=sa)', default='sa')
parser.add_argument('--password', help='Password to access server (required)', required=True)
parser.add_argument('--database', help='Database to access (default=RetailDB)', default='RetailDB')
args = parser.parse_args()

connect_str = 'mysql+mysqlconnector://' + args.username + ':' + args.password + '@' + args.server +'/' + args.database
alchemy_engine = create_engine(connect_str)
read_con = alchemy_engine.connect()

print('Read Items and Descriptions')
query = r"""
        select id, department_id, description, price
        from items_web;
        """
items = pd.read_sql(query,read_con)
items['description'] = items['description'].map(lambda x: fix_string(x))

print('Save Items')
itemsfile = f'../../data/csv/web_items.csv'
items.to_csv(itemsfile, header=True, index=False)

departmentsfile = f'../../data/csv/web_departments.csv'
query = r"""select id, departmentname from department;"""
departments = pd.read_sql(query, read_con)
departments['departmentname'] = departments['departmentname'].map(lambda x: fix_string(x))
departments.to_csv(departmentsfile, header=True, index=False)