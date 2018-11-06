from collections import namedtuple

"""
The purpose of using a namedtuple rather than regular tuples or classes is to easily make inserts into Postgres database.
    A namedtuple will automatically convert to a composite type in Postgres when using Psycopg2.
"""


Announcement_tup = namedtuple('Announcement','prefix_origin_hash path_len_and_rec_from')
What_if_tup = namedtuple('what_if_rates','rejection_rate false_positive_rate')
Relationship = namedtuple('Relationship', 'primary_key cone_as customer_as provider_as peer_as_1 peer_as_2 source')
