'''
Loads data from the data/raw folder into PGSQL. Runs a deduplication step based on job posting ID for deduplication. 
Contains deduplication so duplicate roles are not upserted into the PG DB 
'''

