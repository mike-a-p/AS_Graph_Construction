from collections import namedtuple

Announcement_tup = namedtuple('Announcement','prefix_origin_hash path_len_and_rec_from')
What_if_tup = namedtuple('what_if_rates','rejection_rate false_positive_rate')
Relationship = namedtuple('Relationship', 'primary_key cone_as customer_as provider_as peer_as_1 peer_as_2 source')
