'''
Created on Aug 23, 2018

@author: Mike P

May Require 'psycopg2'
"pip3 install psycopg2"
'''
import sys
import time
import named_tup
from SQL_querier import SQL_querier
import json
from AS_Graph_iterative import AS_Graph
from AS import AS
from Announcement import Announcement
from Recipient_List import Recipient_List
from progress_bar import progress_bar

class extrapolator:
    def __init__(self, ann_table_name = None, graph_table_name = None, save_to_db = None,):
        #extrapolator can be given a graph to use
        self.ann_table_name = ann_table_name
        self.save_to_db = save_to_db
        #if test_data is anything but True, real data is used
        self.graph = AS_Graph(graph_table_name)
        self.ases_with_anns = list()
        self.querier = SQL_querier()
        return

    def update_graph(self,peers_table_name = None, customer_provider_table_name = None,
                        no_ranks = None):
        #Graph is updated using one-to-one relationships stored in DB.
        #Newly constructed graph is assigned ranks (which includes combining
        #strongly connected components).
        if(peers_table_name and customer_provider_table_name):
            self.graph.read_seperate_relationships_from_db(peers_table_name,
                                                customer_provider_table_name)
        else:
            self.graph.read_seperate_relationships_from_db()
        if(not no_ranks):
            self.graph.assign_ranks()
        if(self.save_to_db):
            self.graph.save_graph_to_db()
        return
        
    def perform_propagation(self,num_announcements = None):
        """Performs announcement propagation and uploads results to database.
            :meth:`~insert_announcements()`\n 
            :meth:`~prop_anns_sent_to_peers_providers()`\n
            :meth:`~propagate_up()`\n
            :meth:`~propagate_down()`\n
            :meth:`~upload_to_db()`\n

        Args:
            use_db (:obj:`int` or `boolean`): Signifies using database to gather data.
            num_announcements (:obj:`int`, optional): Number of announcements to propagate.

        """
        self.insert_announcements(num_announcements)
        self.prop_anns_sent_to_peers_providers()
        self.propagate_up()
        self.propagate_down()
        if(self.save_to_db):
            self.save_anns_to_db()
        return

    def append_announcement(self, some_dict,key, announcement):
        """Adds an announcement to a dictionary
            
        Appends announcement to list at some_dict[key]
            
        Args:
            some_dict (:obj:`dict`): dictionary to append to.
            key (:obj:`int`/:obj:`str`): key to use in dictionary
            announcement (:obj:`Announcement`): Announcement to give append

        """
        #append if this key has an entry already, otherwise make new entry
        if(key in some_dict):
            some_dict[key].append(announcement)
        else:
            some_dict[key] = [announcement]
        return

    def best_from_multiple_prefixes(self, announcements,one_type = None):
        """Finds the best announcements from a list containing multiple prefixes
        
        Args:
            announcements (:obj:`list` of :obj:`Announcement`): Announcements to choose from.
            one_type (:obj:`int`, optional): The presence of this variable 
                signifies that announcements all come from the same priority AS 
                e.g. provider, peer, or customer.
        Returns:
            (:obj:`list` of :obj:`Announcement`): A list of announcements, only one per prefix.

        """

        #initialize list of best announcements
        best = list()
        #Create and fill dictionary that organizes announcements by prefix
        #by using the prefix as the key
        anns_by_prefix = dict()
        for ann in announcements:
            self.append_announcement(anns_by_prefix, ann.prefix, ann)
        
        #If all announcements have same relationship priority 
        #get the shortest announcement for each prefix and append to "best"
        #else get shortest and best relationship for each prefix
        if(one_type):
            for prefix in anns_by_prefix:
                best.append(self.best_by_length(anns_by_prefix[prefix]))
        else:
            for prefix in anns_by_prefix:
                best.append(self.best_by_relationship(anns_by_prefix[prefix]))
        return best

    def best_by_relationship(self, announcements):
        """Finds the best announcement for a single prefix from a list with varying
            relationship types e.g. provider, peer, or customer and varying as_path
            lengths
        
        Args:
            announcements (:obj:`list` of :obj:`Announcement`): Announcements to choose from.
                
        Returns:
            (:obj:`Announcement`): Single best announcement.
        """

        #announcements = announcements of same prefix but different priorities.
        #Returns single announcement of best relationship and length in NON LIST
        
        bestType = 0
        best_anns = list()
        
        #If announcement originated from this AS send it
        #If it's of equal priority to bestType, keep it
        #If it's of higher priority to bestType, clear best_anns and keep
        #Return the announcement with shortest as_path of best_anns
        for ann in announcements:
            if(ann.received_from == None):
                return ann
            elif(ann.received_from==bestType):
                best_anns.append(ann)
            elif(ann.received_from > bestType):
                bestType = ann.received_from
                best_anns[:] = []
                best_anns.append(ann)
        return self.best_by_length(best_anns)

    def best_by_length(self, announcements):
        """Finds the announcement with the shortest as_path
        
        Args:
            announcements (:obj:`list` of :obj:`Announcement`): Announcements to choose from.
            
        Returns:
            (:obj:`Announcement`): Single best announcement by length of AS_PATH.
        
        """

        bestLength = sys.maxsize
        best = None
        for ann in announcements:
            if(ann.as_path_length<bestLength):
                bestLength = ann.as_path_length
                best = ann
        return best

    def prop_one_announcement(self, asn,ann,to_peer_provider = None, 
                                            to_customer = None):
        """Send a single announcement to neighbors depending on arguments

        Args:
            asn(:obj:`int`): ASN of AS sending announcement.
            ann(:obj:`Announcement`): Announcement to send.
            to_peer_provider(:obj:`int` or :obj: 'bool', optional):Identification of whether or
                not the announcement is going to peers and providers.
            to_customer(:obj:`int` or :obj: 'bool', optional):Identification of whether or 
                not the announcement is going to customers.

        """
        #TODO make this code shorter
        send_to = Recipient_List()
        #start appending neighbors to "send_to" if they haven't already received it
        send_to_filtered = Recipient_List()
        source_as = self.graph.ases[asn]
        if(to_peer_provider is not None):
            for peer in source_as.peers:
                send_to.peers.append(peer)
            for provider in source_as.providers:
                send_to.providers.append(provider)
        if(to_customer is not None):
            for customer in source_as.customers:
                send_to.customers.append(customer)

        for provider in send_to.providers:
            already_received = 0
            all_neighbor_anns =(self.graph.ases[provider].anns_from_customers +
                            self.graph.ases[provider].anns_from_peers_providers)
            for neighbor_ann in all_neighbor_anns:
                if((neighbor_ann.origin == ann.origin) and
                    (neighbor_ann.prefix == ann.prefix)):
                    already_received = 1
                    break
            if(not already_received):
                send_to_filtered.providers.append(provider)

        for peer in send_to.peers:
            already_received = 0
            all_neighbor_anns =(self.graph.ases[peer].anns_from_customers +
                            self.graph.ases[peer].anns_from_peers_providers)
            for neighbor_ann in all_neighbor_anns:
                if((neighbor_ann.origin == ann.origin) and 
                    (neighbor_ann.prefix == ann.prefix)):
                    already_received = 1
                    break
            if(not already_received):
                send_to_filtered.peers.append(peer)

        for customer in send_to.customers:
            already_received = 0
            all_neighbor_anns =(self.graph.ases[customer].anns_from_customers +
                            self.graph.ases[customer].anns_from_peers_providers)
            for neighbor_ann in all_neighbor_anns:
                if((neighbor_ann.origin == ann.origin) and 
                    (neighbor_ann.prefix == ann.prefix)):
                    already_received = 1
                    break
            if(not already_received):
                send_to_filtered.customers.append(customer)

        #Integer arguments 2/1/0 are for "received from" customer/peer/provider
        for provider in send_to_filtered.providers:
            this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 2 , None, ann.as_path_length + 1)
            self.graph.ases[provider].give_announcement(this_ann)
        for peer in send_to_filtered.peers:
            this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 1, None, ann.as_path_length + 1)
            self.graph.ases[peer].give_announcement(this_ann)
        for customer in send_to_filtered.customers:
            this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 0, None, ann.as_path_length + 1)
            self.graph.ases[customer].give_announcement(this_ann)
        return
                       
    def propagate_up(self):
        """Propagate announcements that came from customers to peers and providers
        
        """
        
        print("Propagating Announcements From Customers")
        graph = self.graph
        progress = progress_bar(len(self.graph.ases_by_rank))
        for level in range(len(self.graph.ases_by_rank)):
            for asn in self.graph.ases_by_rank[level]:
                #filter out best announcements from customers
                cust_anns = self.graph.ases[asn].anns_from_customers
                best_anns_from_customers = self.best_from_multiple_prefixes(cust_anns,1)
                #"non best" announcements are discarded
                self.graph.ases[asn].anns_from_customers = best_anns_from_customers
                #send out best announcements
                for ann in best_anns_from_customers:
                    self.prop_one_announcement(asn, ann, 1, None)
            progress.update()
        progress.finish()
        return

    def give_ann_to_as_path(self, as_path, prefix, hop):
        """Record announcement to all ASes on as_path
        
        Args:
            as_path(:obj:`list` of :obj:`int`): ASNs showing the path taken by
                an announcement. Leftmost being the most recent.
            prefix(:obj:`string`): An IP address subset e.g. 123.456.789/10 
                where 10 depicts the scope of the IP set.
            hop(:obj:`str`):An IP address used to record the next system to hop to.

        """
     
        #avoids error for anomaly announcement
        if(as_path is None):
                return
        #i used to traverse as_path
        i = 0
        #as_path is ordered right to left, so rev_path is the reverse
        rev_path = as_path[::-1]

        for asn in rev_path:
        #TODO order ases_with_anns
            if(asn not in self.graph.ases):
                continue
            comp_id = self.graph.ases[asn].SCC_id
            if(comp_id in self.ases_with_anns):
                #If AS has already recorded origin/prefix pair, stop
                for ann2 in self.graph.ases[comp_id].all_announcements():
                    #compare the origin to the first AS in rev_path and prefix to prefix
                    if(ann2.origin==rev_path[0] and ann2.prefix==prefix):
                        return
            sent_to = None
            #If not at the most recent AS (rightmost in rev_path), record the AS it is sent to next
            if(i<len(as_path)-1):
                #similar to rec_from() function, could get own function
                found_sent_to = 0
                asn_sent_to = rev_path[i+1]
                if(asn_sent_to in self.graph.ases[comp_id].providers):
                    sent_to = 0
                    found_sent_to = 1
                if(not found_sent_to):
                    if(asn_sent_to in self.graph.ases[comp_id].peers):
                        sent_to = 1
                        found_sent_to = 1
                if(not found_sent_to):
                    if(asn_sent_to in self.graph.ases[comp_id].customers):
                        sent_to = 2
                        found_sent_to = 1

            this_path_len = i + 1

            #assign 'received_from' if AS isn't first in as_path
            if(i > 1):
                if(rev_path[i-1] in self.graph.ases[comp_id].providers):
                    received_from = 0
                if(rev_path[i-1] in self.graph.ases[comp_id].peers):
                    received_from = 1
                if(rev_path[i-1] in self.graph.ases[comp_id].customers):
                    received_from = 2
            else: received_from = None

            announcement = Announcement(prefix,rev_path[0],hop,received_from,sent_to,this_path_len)
            #append new announcement to ann_dict
            self.graph.ases[asn].give_announcement(announcement)
            #TODO check if this 'if' ammendment works
            if(sent_to == 1 or sent_to == 0):
                self.graph.ases[asn].anns_sent_to_peers_providers.append(announcement)
            #ases_with_anns
            self.ases_with_anns.append(asn)
            #increment i for path traversal
            i = i + 1
        return

    def prop_anns_sent_to_peers_providers(self):
        """Send announcements known to be sent to a peer or provider of each AS to
            the other peers and providers of each AS
       

        """
        print("\tPropagating Announcements Sent to Peers/Providers...")
        
        #For all ASes with announcements ( list made in give_anns_to_as_path() )
        for asn in self.ases_with_anns:
            #For all announcements received by an AS
            for ann in self.graph.ases[asn].anns_sent_to_peers_providers:
                    self.prop_one_announcement(asn,ann,1, None)
            return

    def propagate_down(self):
        """From "top" to "bottom"  send the best announcements at every AS to customers

        """
        print("Propagating Announcements To Customers")
        progress = progress_bar(len(self.graph.ases_by_rank))
        for level in reversed(range(len(self.graph.ases_by_rank))):
            for asn in self.graph.ases_by_rank[level]:
                this_as = self.graph.ases[asn]
                
                best_anns_from_peers_providers = self.best_from_multiple_prefixes(this_as.anns_from_peers_providers)
                #"non best" announcements from peers and providers are discarded
                this_as.anns_from_peers_providers = best_anns_from_peers_providers
                best_announcements = self.best_from_multiple_prefixes(this_as.anns_from_customers +
                                                              this_as.anns_from_peers_providers)
                for ann in best_announcements:
                    self.prop_one_announcement(asn,ann,None, 1)
            progress.update()
        progress.finish()
        return

    def insert_announcements(self,num_announcements = None):
        """Begins announcement propagation
            
        Args:
            num_announcements (:obj:`int`, optional): Number of announcements to propagate
      
        """

        print("Inserting Announcements...")

        start_time = time.time()

        if(self.ann_table_name):
            records = self.querier.select_table(ann_table_name, num_announcements)
        else:
            records = self.querier.select_table('elements', num_announcements)

        for ann in records:
            self.give_ann_to_as_path(ann.as_path, ann.prefix, ann.next_hop)
        return

    def count_asn_conflicts(self):
        if(self.ann_table_name):
            records = self.querier.select_table(ann_table_name, num_announcements)
        else:
            records = self.querier.select_table('elements', num_announcements)
        conflict_asns = dict()
        for ann in records:
            for asn in ann.as_path:
                if(asn not in self.graph.ases):
                    conflict_asns[asn] = True
        return len(conflict_asns)

    def save_anns_to_db(self):
        print("Saving Propagation results to DB")
        progress = progress_bar(len(self.graph.ases))
        start_time = time.time()

        for asn in self.graph.ases:
            AS = self.graph.ases[asn]
            if asn == AS.SCC_id:
                sql_anns_arg = AS.anns_to_sql()
                self.querier.insert_to_as_announcements(asn,sql_anns_arg)
            progress.update()
        progress.finish()
        end_time = time.time()
        print("Time To Save Announcements: " + str(end_time - start_time) + "s")
        return
