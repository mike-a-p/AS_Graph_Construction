import uuid
import psycopg2
from named_tup import Announcement_tup
from Announcement import Announcement

class AS:
    def __init__(self,asn,
            customers=None,
            peers=None,
            providers=None,
            scc_id=None,
            graph_id=None):
        if(customers is not None):
            self.customers = customers
        else: self.customers = list()
        if(peers is not None):
            self.peers = peers
        else: self.peers = list()
        if(providers is not None):
            self.providers = providers
        else: self.providers = list()
        if(scc_id is not None):
            self.scc_id = scc_id
        if(graph_id is not None):
            self.graph_id = graph_id
        self.asn = asn
        self.rank = None
        self.anns_from_self = dict()
        self.anns_from_customers = dict()
        self.anns_from_providers = dict()
        self.anns_from_peers = dict()
        self.anns_sent_to_peers_providers = dict()
        self.all_anns = dict()
        self.seen_anns = dict()
        self.incoming_announcements = dict()
        #variables for Tarjan's Alg
        self.index = None
        self.lowlink = None
        self.onstack = False
        self.SCC_id = None
        #component validation
        self.visited = False
    
    def __str__(self):
        return ('ASN: ' + str(self.asn) + ', Rank: ' + str(self.rank)
            + ', Providers: ' + str(self.providers)
            + ', Peers: ' + str(self.peers)
            + ', Customerss: ' + str(self.customers)
            + ', Index: ' + str(self.index)
            + ', Lowlink: ' + str(self.lowlink)
            + ', Onstack: ' + str(self.onstack)
            + ', SCC_id: ' + str(self.SCC_id))

    def add_neighbor(self,asn,relationship):
        """Adds neighbor ASN to to appropriate list based on relationship
        
        Args:
            asn (:obj:`int`): ASN of neighbor to append
            relationship (:obj:`int`): Type of AS 'asn' is with respect to this
                 AS object. 0/1/2 : customer/peer/provider

        """
        if(relationship == 0):  
            self.providers.append(asn)
        if(relationship == 1):  
            self.peers.append(asn)
        if(relationship == 2):  
            self.customers.append(asn)
        return
    
    def update_rank(self,rank):
        """Updates rank of this AS to provided rank only if current rank is
            lower.
        
        Args:
            rank (:obj:`int`): Rank used to describe height of AS in 
                provider->customer tree like graph. Used for simple propagation
                up and down the graph.
    
        Returns:
            (:obj:`boolean`): True if rank was changed, False if not.

        """

        old = self.rank
        self.rank = max(self.rank, rank)
        if(old!=self.rank):
            return True
        else:
            return False

    #TODO rename plural
    def receive_announcements(self,announcements):
        """Appends announcement to appropriate dictionary (from customer or 
            peer/provider).
    
        Args:
            announcement (:obj:`Announcement`): Announcement to append.
        """
        for ann in announcements:
            if(ann.prefix not in self.incoming_announcements):
                self.incoming_announcements[ann.prefix] = list()
#            ann_copy = Announcement(ann.prefix, ann.origin, ann.next_as, ann.received_from,
#                                    None,ann.priority,ann.as_path_length, ann.as_path.copy())
#            ann_copy.as_path.appendleft(self.asn)
#            ann_copy.as_path_length = ann_copy.as_path_length + 1
            self.incoming_announcements[ann.prefix].append(ann)
        return
            
    def process_announcements(self):
        for prefix in self.incoming_announcements:
            anns = self.incoming_announcements[prefix]
            best = anns[0]
            for ann in anns:
                if ann.priority > best.priority:
                    best = ann
            self.all_anns[prefix] = best

    def sent_to_peer_or_provider(self,announcement):
        self.anns_sent_to_peers_providers[announcement.prefix + str(announcement.origin)] = announcement
    
    def already_received(self,ann):
        if((ann.prefix + str(ann.origin)) in self.anns_from_providers or
              (ann.prefix + str(ann.origin)) in self.anns_from_peers or
              (ann.prefix + str(ann.origin)) in self.anns_from_customers):
            return True
        else:
            return False

    def anns_to_sql(self):
        """Converts all announcements received by this AS to a list of
            named tuples expected by Postgres.

        Returns:
            (:obj:`list` of :obj:`Announcement_tup`): List of all announcements
                received by this AS in format accepted by Postgres table.


        """
        data = list()
        psycopg2.extras.register_uuid()

        #TODO iterate through all announcements between two dictionaries
        #without repeating code
        for ann in self.anns_from_customers:
            ann = self.anns_from_customers[ann]
            
            #path_len an rec_from are given 3 digits each
            #padding ensures e.g. '33' + '0' is not mistaken for '3' + '30"
            path_len = str(ann.as_path_length).zfill(3)
            if(ann.received_from is None):
               rec_from = str(3).zfill(3) 
            else:
               rec_from = str(ann.received_from).zfill(3)
            # '-' serves similar purpose to padding, seperating parts
            po = (str(ann.prefix) + '-' + str(ann.origin))
            po = uuid.uuid3(uuid.NAMESPACE_DNS,po)
            path_len_rec_from = int(path_len + rec_from)
            #Named Tuples are adaptable to Postgres composite types
            ann_tup = Announcement_tup(*(po,path_len_rec_from))
            data.append(ann_tup)
        for ann in self.anns_from_peers_providers:
            ann = self.anns_from_peers_providers[ann]
            
            #path_len an rec_from are given 3 digits each
            #padding ensures e.g. '33' + '0' is not mistaken for '3' + '30"
            path_len = str(ann.as_path_length).zfill(3)
            if(ann.received_from is None):
               rec_from = str(3).zfill(3) 
            else:
               rec_from = str(ann.received_from).zfill(3)
            # '-' serves similar purpose to padding, seperating parts
            po = (str(ann.prefix) + '-' + str(ann.origin))
            po = uuid.uuid3(uuid.NAMESPACE_DNS,po)
            path_len_rec_from = int(path_len + rec_from)
            #Named Tuples are adaptable to Postgres composite types
            ann_tup = Announcement_tup(*(po,path_len_rec_from))
            data.append(ann_tup)

        return data

    def append_no_dup(self, this_list, asn, l = None, r = None):
        """Maintains order of provided list. Inserts given asn to this_list if 
            binary search doesn't find asn.


        Returns: None, but uses return statement for recursion.
    
        """

        #initialize l and r on first call
        if(l is None and r is None):
            l = 0
            r = len(this_list)-1
        #if r is greater than l, continue binary search
        if(r>=l):
            half = int(l + (r-l)/2)
            #if asn is found in the list, return without inserting
            if(this_list[half] == asn):
                return
            elif(this_list[half] > asn):
                return self.append_no_dup(this_list, asn, l, half-1)
            else:
                return self.append_no_dup(this_list, asn, half+1, r)
        #if r is less than l, insert asn
        else:
            this_list[r+1:r+1] = [asn]
        return
