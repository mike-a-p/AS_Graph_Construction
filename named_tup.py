from collections import namedtuple

Announcement = namedtuple('Announcement','origin prefix sent_to rec_from hop as_path')

Relationship = namedtuple('Relationship', 'primary_key cone_as customer_as provider_as peer_as_1 peer_as_2 source')
