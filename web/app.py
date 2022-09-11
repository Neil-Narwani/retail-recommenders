from flask import Flask, render_template, request, jsonify, url_for
from os import environ
from flask_sqlalchemy import SQLAlchemy
import numpy as np
import pandas as pd
import tensorflow as tf
import gunicorn

app = Flask(__name__)
# app.config.from_object('config')

@app.route('/')
def index():
    return render_template('index.html')

items_df = pd.read_csv('./data/csv/web_items.csv', header=0)
recommender_model = tf.saved_model.load('./data/saved_model')

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

        idlist = idlist + ['']*(10 - len(idlist))

        idlist_tf = tf.constant(idlist,shape=(1,10,1))

        _, recommends = recommender_model(idlist_tf)

        recommends_list = recommends.numpy().flatten().tolist()

        recommends_list = [int(x.decode('utf-8')) for x in recommends_list]

        print(recommends_list)

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
            currentItem = items_df.loc[items_df['id']==recommendation]
            print(currentItem.id.values)
            print(currentItem.description.values)

            results = results + f'''    <tr>
              <td>{ currentItem.id.values }</td>
              <td>{ currentItem.description.values }</td>
              <td>{ currentItem.price.values }</td>
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