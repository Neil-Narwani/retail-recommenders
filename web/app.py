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
depts_df = pd.read_csv('./data/web_departments.csv', header=0)
depts_df = depts_df.rename(columns={'id':'department_id'})
items_df = items_df.merge(depts_df, on='department_id', how='left')

DESCRIPTION_TOTAL = len(items_df.index)

USERS_PER_PAGE = 10

@app.route('/order', defaults={ 'offset' : 0, 'limit' : USERS_PER_PAGE }, methods=['GET'] )
@app.route('/order/<offset>', defaults={ 'limit' : USERS_PER_PAGE }, methods=['GET'] )
@app.route('/order/<offset>/<limit>', methods=['GET'] )
def order(offset, limit):
    try:
        offset = int( offset )
    except ValueError:
        offset = 0
    
    try:
        limit = int( limit )
    except:
        limit = USERS_PER_PAGE
    
    # ensure offset & limit aren't negative
    offset = offset if offset > 0 else 1 
    limit = limit if limit > 0 else USERS_PER_PAGE
        
    if DESCRIPTION_TOTAL <= offset:
       return redirect( f'/order/{DESCRIPTION_TOTAL - limit}/{limit}' )
    
    pages = { 'begin' : 0 };
    pages[ 'prev' ] = max( offset - limit, 0 ) # don't want a negative index!
    pages[ 'current' ] = min( max( 0, offset ), DESCRIPTION_TOTAL ) # gotta be in range!
    pages[ 'next' ] = min( offset + limit, DESCRIPTION_TOTAL - limit ) # don't want to go over
    pages[ 'end' ] = DESCRIPTION_TOTAL - limit # just get the end.    
    items = items_df[offset:offset+limit]
    return render_template('order.html', items=items, pages=pages, limit=limit)
    
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
            print(currentItem.id.values[0])
            print(currentItem.description.values[0])

            results = results + f'''    <tr>
              <td>{ currentItem.id.values[0] }</td>
              <td>{ currentItem.description.values[0] }</td>
              <td>{ currentItem.price.values[0] }</td>
              <td><button class="btn btn-danger my-cart-btn" data-id="{currentItem.id.values}" data-name="{currentItem.description.values}" data-summary="summary 2" 
              data-price="{currentItem.price.values}" data-quantity="1" data-image="{url_for('static', filename='img/add.png')}">Add to Cart</button></td>
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