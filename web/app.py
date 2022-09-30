from flask import Flask, render_template, request, jsonify, url_for
from os import environ
from flask_sqlalchemy import SQLAlchemy
import numpy as np
import pandas as pd
import gunicorn
from ModelApiRequest import ModelApiRequest

app = Flask(__name__)
# app.config.from_object('config')
model = ModelApiRequest()

@app.route('/')
def index():
    return render_template('index.html')

items_df = pd.read_csv('./data/web_items.csv', header=0).sort_values(['department_id','description'], axis=0)
items_df = items_df[items_df['department_id']>600]
depts_df = pd.read_csv('./data/web_departments.csv', header=0)
depts_df = depts_df.rename(columns={'id':'department_id'})
items_df = items_df.merge(depts_df, on='department_id', how='left')

DESCRIPTION_TOTAL = len(items_df.index)

USERS_PER_PAGE = 10

@app.route('/order', defaults={ 'offset' : 0, 'limit' : USERS_PER_PAGE }, methods=['GET'] )
@app.route('/order/<offset>', defaults={ 'limit' : USERS_PER_PAGE }, methods=['GET'] )
@app.route('/order/<offset>/<limit>', methods=['GET'] )
def order(offset, limit):
    return render_template('order.html', items=items_df)
    
@app.route('/recommend', methods=['POST'])
def recommend():
    results = "<p>No Recommendations Generated</p>"
    if request.method == "POST":
        products=request.json['products']
        idlist = [str(x['id']) for index,x in enumerate(products) if index < 10]
        pricelist = [float(items_df.loc[items_df['id'] == x['id']]['price'].values[0]) for index,x in enumerate(products) if index < 10]
        departmentlist = [int(items_df.loc[items_df['id'] == x['id']]['department_id'].values[0]) for index,x in enumerate(products) if index < 10]
        recommends_list = model.Predict(idlist,pricelist,departmentlist)

        results = '''<h2> Recommendations :  </h2>
        <table class="table" id="recommendationTable">
          <thead>
            <tr>
              <th scope="col">Item ID</th>
              <th scope="col">Description</th>
              <th scope="col">Price</th>
            </tr>
          </thead>
          <tbody>'''

        for recommendation in recommends_list:
            currentItem = items_df.loc[items_df['id']==int(recommendation)]
            if len(currentItem.price.values) > 0:
              price = float(currentItem.price.values[0])
            else:
              price = 0.0
            price_str =  "${:,.2f}".format(price)
            results = results + f'''    <tr>
              <td>{ currentItem.id.values[0] }</td>
              <td>{ currentItem.description.values[0] }</td>
              <td>{ price_str }</td>
              <td><button class="btn btn-danger my-cart-btn" data-id="{currentItem.id.values[0]}" data-name="{currentItem.description.values[0]}" data-summary="summary 2" 
              data-price="{currentItem.price.values[0]}" data-quantity="1" data-image="{url_for('static', filename='img/add.png')}">Add to Cart</button></td>
            </tr>'''
        results = results + '</tbody> </table>'

        return jsonify(results)

@app.route('/checkout', methods=['POST'])
def checkout():
    if request.method == "POST":
        products=request.json['products']
        print(products)
    foo = {'redirect' : '/success'}
    return jsonify(foo)
    
@app.route('/success', methods=['Get'])
def success():
    return render_template('success.html')