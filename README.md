# Retail Recommenders
Machine Learning Project used on Specialty Retailer data for deduplication of customer records as well as using Tensorflow Recommenders to build a recommendation engine for this data.

Demo Website - http://retail-recommenders.herokuapp.com/

## Phase 1
### Deduplication of Customer Data (pre_processing folder)
Customer Data was presented in several SQL databases.  Seeing that there were many duplicates in the customer records, I would need to deduplicate this data before using transaction data to create a recommendation engine. In order to train the model, the data must be converted to tensorflow_datasets, which is the datatype required to train the model, as well as CSV files, for easier local access.

#### Merging SQL Tables folder
Initially, customer data was stored in several SQL databases, seperated by store. Through several join SQL commands, each database was merged into the RetailDB SQL database.

#### SQL to CSV folder
After converging all customer data into the RetailDB SQL database, I then converted RetailDB into several CSV files for easier local access. I queried RetailDB by relevant columns and converted them into the 3 CSV files as listed below:


## Phase 2
### Item-Item Dataset generation
Genenerating item-item datasets to allow for a sequential model.

## Phase 3
### Tensor Flow Recommenders to create and save a Machine Learning Model on the item-item data.
Created a two - tower model using the Tensor Flow Recommenders library and training data created in Phase 1 and 2. Model's inputs include the ItemIDs of a customer's purchase history and output is a list of ten items that the customer is most likely to purchase next. I then created a demo website (http://retail-recommenders.herokuapp.com/) which allows real-time testing and a proof of concept application of the model. 

## Sources
https://github.com/Asraf-Uddin-Ahmed/jquery.mycart

https://www.tensorflow.org/recommenders/examples/sequential_retrieval
https://www.tensorflow.org/recommenders/
https://www.tensorflow.org/recommenders/examples/efficient_serving

### Data Generation Utility used to create Item-Item Test Data:
https://github.com/tensorflow/examples/blob/master/lite/examples/recommendation/ml/data/example_generation_movielens.py
