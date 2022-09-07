import os
import itertools
import time
import locale
import json
import dedupe
import dedupe.backport
import mysql.connector
import argparse

##returns a generator for two rows given a set of rows 
def record_pairs(result_set):
    print('Inside record_pairs')
    for i,row in enumerate(result_set):
        a_record_id = row['a_record_id']
        a_record = row['a_record']
        b_record_id = row['b_record_id']
        b_record = row['b_record']
        record_a = (a_record_id, json.loads(a_record))
        record_b = (b_record_id, json.loads(b_record))

        yield record_a, record_b       
        if i % 1000 == 0:
            print('.', end='')
    print('Done')

## returns a generator for each 
def cluster_ids(clustered_dupes):

    for cluster, scores in clustered_dupes:
        cluster_id = cluster[0]
        for ID, score in zip(cluster, scores):
            yield ID, cluster_id, score
			
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract customer and transaction data from mySQL db')
    parser.add_argument('--server', help='Address of mySQL server (default=localhost)')
    parser.add_argument('--username', help='Username to access server (default=sa)')
    parser.add_argument('--password', help='Password to access server (required)', required=True)
    parser.add_argument('--database', help='Database to access (default=RetailDB)')
    parser.add_argument('--table', help='Use Custom Table Name')
    
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
    
    if args.table is None:
        Customer_Table = 'customer'
    else:  
        Customer_Table = args.table

    settings_file = 'dedupe_dataframe_learned_settings'
    training_file = 'dedupe_dataframe_training.json'

    start_time = time.time()

    # You'll need to copy `examples/mysql_example/mysql.cnf_LOCAL` to
    # `examples/mysql_example/mysql.cnf` and fill in your mysql database
    # information in `examples/mysql_example/mysql.cnf`

    # We use Server Side cursors (SSDictCursor and SSCursor) to [avoid
    # having to have enormous result sets in
    # memory](http://stackoverflow.com/questions/1808150/how-to-efficiently-use-mysqldb-sscursor).
    read_con = mysql.connector.connect(
        host = server,
        database  = database,
        user = username,
        password = args.password
    )

    write_con = mysql.connector.connect(
        host = server,
        database  = database,
        user = username,
        password = args.password
    )

    CUSTOMER_SELECT = "SELECT ID, Company, FirstName, LastName, Address, Address2, City, State, Zip, PhoneNumber, EmailAddress " \
                   "from {table}".format(table=Customer_Table)

    # ## Training

    if os.path.exists(settings_file):
        print('reading from ', settings_file)
        with open(settings_file, 'rb') as sf:
            deduper = dedupe.StaticDedupe(sf, num_cores=6)
    else:
        print('Settings file not found')
        # Define the fields dedupe will pay attention to
        #
        # The address, city, and zip fields are often missing, so we'll
        # tell dedupe that, and we'll learn a model that take that into
        # account
        fields = [{'field': 'Company', 'type': 'String', 'has missing' : True},
                  {'field': 'FirstName', 'type': 'String', 'has missing': True},
                  {'field': 'LastName', 'type': 'String', 'has missing': True},
                  {'field': 'Address', 'type': 'String', 'has missing': True},
                  {'field': 'Address2', 'type': 'String', 'has missing': True},
                  {'field': 'City', 'type': 'String', 'has missing': True},
                  {'field': 'State', 'type': 'String', 'has missing': True},
                  {'field': 'Zip', 'type': 'String', 'has missing': True},
                  {'field': 'PhoneNumber', 'type': 'String', 'has missing': True},
                  {'field': 'EmailAddress', 'type': 'String', 'has missing': True},
                  ]

        # Create a new deduper object and pass our data model to it.
        deduper = dedupe.Dedupe(fields, num_cores=6)

        # We will sample pairs from the entire customer table for training
        with read_con.cursor(dictionary = True, buffered = False) as cur:
            cur.execute(CUSTOMER_SELECT)
            temp_d = {i: row for i, row in enumerate(cur)}

        # If we have training data saved from a previous run of dedupe,
        # look for it an load it in.
        #
        # __Note:__ if you want to train from
        # scratch, delete the training_file
        if os.path.exists(training_file):
            print('reading labeled examples from ', training_file)
            with open(training_file) as tf:
                deduper.prepare_training(temp_d, training_file=tf)
        else:
            print('training file not found')
            deduper.prepare_training(temp_d)

        del temp_d

        # ## Active learning

        print('starting active labeling...')
        # Starts the training loop. Dedupe will find the next pair of records
        # it is least certain about and ask you to label them as duplicates
        # or not.

        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        dedupe.convenience.console_label(deduper)
        # When finished, save our labeled, training pairs to disk
        with open(training_file, 'w') as tf:
            deduper.write_training(tf)

        # Notice our the argument here
        #
        # `recall` is the proportion of true dupes pairs that the learned
        # rules must cover. You may want to reduce this if your are making
        # too many blocks and too many comparisons.
        deduper.train(recall=0.75)

        with open(settings_file, 'wb') as sf:
            deduper.write_settings(sf)

        # We can now remove some of the memory hobbing objects we used
        # for training
        deduper.cleanup_training()

   
    print('blocking...')


    # To run blocking on such a large set of data, we create a separate table
    # that contains blocking keys and record ids
    print('creating blocking_map database')
    with write_con.cursor(buffered = True) as cur:
        cur.execute("DROP TABLE IF EXISTS blocking_map")
        cur.execute("CREATE TABLE blocking_map "
                    "(block_key VARCHAR(200), ID INTEGER) "
                    "CHARACTER SET utf8 COLLATE utf8_unicode_ci")

    write_con.commit()
    
    # If dedupe learned a Index Predicate, we have to take a pass
    # through the data and create indices.
    print('creating inverted index')

    for column in deduper.fingerprinter.index_fields:
        with read_con.cursor(dictionary = True, buffered = False) as cur:
            cur.execute("SELECT DISTINCT {field} FROM {table} "
                        "WHERE {field} IS NOT NULL".format(field=column, table=Customer_Table))
            field_data = (row[column] for row in cur)
            deduper.fingerprinter.index(field_data, column)
    # Now we are ready to write our blocking map table by creating a
    # generator that yields unique `(block_key, ID)` tuples.
    print('writing blocking map')

    with read_con.cursor(dictionary = True, buffered = False) as read_cur:
        read_cur.execute(CUSTOMER_SELECT)
        full_data = ((row['ID'], row) for row in read_cur)
        b_data = deduper.fingerprinter(full_data)

        with write_con.cursor(buffered = True) as write_cur:
            for tup in b_data:
                write_cur.execute("INSERT INTO blocking_map VALUES (%s, %s)",  tup)

    write_con.commit()
    
    # Free up memory by removing indices we don't need anymore
    deduper.fingerprinter.reset_indices()

    # indexing blocking_map
    print('creating index')
    with write_con.cursor(buffered = True) as cur:
        cur.execute("CREATE UNIQUE INDEX bm_idx ON blocking_map (block_key, ID)")

    write_con.commit()
    read_con.commit()
    
    
    # select unique pairs to compare
    with read_con.cursor(dictionary = True, buffered = False) as read_cur:

        read_cur.execute("""
               select a.ID as a_record_id,
                      json_object('Company', a.Company,
                                  'FirstName', a.FirstName,
                                  'LastName', a.LastName,                                  
                                  'Address', a.Address,
                                  'Address2', a.Address2,
                                  'City', a.City,                                  
                                  'State', a.State,                                  
                                  'Zip', a.Zip,                                  
                                  'PhoneNumber', a.PhoneNumber,                                                                    
                                  'EmailAddress', a.EmailAddress) as a_record,
                      b.ID as b_record_id,
                      json_object('Company', b.Company,
                                  'FirstName', b.FirstName,
                                  'LastName', b.LastName,                                                                    
                                  'Address', b.Address,
                                  'Address2', b.Address2,
                                  'City', b.City,                                  
                                  'State', b.State,                                  
                                  'Zip', b.Zip,                                  
                                  'PhoneNumber', b.PhoneNumber,                                     
                                  'EmailAddress', b.EmailAddress) as b_record
               from (select DISTINCT l.ID as east, r.ID as west
                     from blocking_map as l
                     INNER JOIN blocking_map as r
                     using (block_key)
                     where l.ID < r.ID) ids
               INNER JOIN customer a on ids.east=a.ID
               INNER JOIN customer b on ids.west=b.ID
               """)

        # ## Clustering

        print('clustering...')
  
        clustered_dupes = deduper.cluster(deduper.score(record_pairs(read_cur)), threshold=0.5)

        with write_con.cursor(buffered = True) as write_cur:
            # ## Writing out results

            # We now have a sequence of tuples of donor ids that dedupe believes
            # all refer to the same entity. We write this out onto an entity map
            # table
            write_cur.execute("DROP TABLE IF EXISTS entity_map")

            print('creating entity_map database')

            write_cur.execute("CREATE TABLE entity_map "
                              "(ID INTEGER, canon_id INTEGER, "
                              " cluster_score FLOAT, PRIMARY KEY(ID))")
            for cluster in cluster_ids(clustered_dupes) :
                write_cur.execute('INSERT INTO entity_map VALUES (%s, %s, %s)', [x.item() for x in cluster])

    write_con.commit()

    with write_con.cursor(buffered = True) as cur:
        cur.execute("CREATE INDEX head_index ON entity_map (canon_id)")

    write_con.commit()
    read_con.commit()

    # Print out the number of duplicates found
    print('# duplicate sets')
    
    locale.setlocale(locale.LC_ALL, '')  # for pretty printing numbers

    with read_con.cursor() as cur:
        cur.execute(r'Select Count(*) as cnt from entity_map where id <> canon_id')
        for row in cur:
            print (row[0])
    # Close our database connection
    read_con.close()
    write_con.close()

    print('ran in', time.time() - start_time, 'seconds')

