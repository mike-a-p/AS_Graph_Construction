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
from collections import deque

class extrapolator:
    def __init__(self):
        """Per
        

        """
        self.querier = SQL_querier()
        self.graph = AS_Graph()
        self.ases_with_anns = list()
        return

    def set_ann_input_table(self,table_name): 
        exists = self.querier.exists_table(table_name)
        if(exists):
            self.ann_input_table_name = table_name
        else:
            print("Table name \"" + table_name + "\" not found in database. Check config file.")
            sys.exit()
        return 

    def set_results_table(self,table_name):
        self.querier.set_results_table(table_name)
        return
    
    def set_peers_table(self,table_name):
        self.graph.set_peers_table(table_name)
        return
    
    def set_customer_provider_table(self,table_name):
        self.graph.set_customer_provider_table(table_name)
        return
        
    def set_graph_table(self,table_name):
        self.graph.set_graph_table(table_name)
        return

    def perform_propagation(self,num_announcements = None,):
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
        start_time = time.time()
        self.insert_announcements(num_announcements)
        self.prop_anns_sent_to_peers_providers()
        self.propagate_up()
        start_down = time.time()
        self.propagate_down()
        end_time = time.time()
        print("Time To Propagate Down: " + str(end_time - start_down) + "s")
        print("Total Time To Propagate: " + str(end_time - start_time) + "s")
        return
   
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
        source_as = self.graph.ases[asn]
        new_path_length = ann.as_path_length + 1
        #Integer arguments 2/1/0 are for "received from" customer/peer/provider
        if(to_peer_provider):
            prov_priority = 2 + new_path_length/100
            for provider in source_as.providers:
                updated_path = ann.as_path
                updated_path.append(provider)
                this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 2 , None, prov_priority,new_path_length, updated_path)
                self.graph.ases[provider].give_announcement(this_ann) 
            peer_priority = 1 + new_path_length/100
            for peer in source_as.peers:
                updated_path = ann.as_path
                updated_path.append(peer)
                this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 1, None, peer_priority, new_path_length, updated_path)
                self.graph.ases[peer].give_announcement(this_ann)

        if(to_customer):
            cust_priority = new_path_length/100
            for customer in source_as.customers:
                updated_path = ann.as_path
                updated_path.append(customer)
                this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 0, None, cust_priority,new_path_length, updated_path)
                self.graph.ases[customer].give_announcement(this_ann)
        return

    def send_all_announcements(self,asn,to_peers_providers = None, to_customers = None):
        source_as = self.graph.ases[asn]
        if(to_peers_providers):
            anns_to_providers = list()
            anns_to_peers = list()
            for ann in source_as.all_anns:
                ann = source_as.all_anns[ann]
                new_path_length = ann.as_path_length + 1
                path_length_weighted = 1 - new_path_length/100
                
                prov_priority = 2 + path_length_weighted
                this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 2 , None, prov_priority,new_path_length, ann.as_path)
                anns_to_providers.append(this_ann)
                
                peer_priority = 1 + path_length_weighted
                this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 1 , None, peer_priority,new_path_length, ann.as_path)
                anns_to_peers.append(this_ann)

            for provider in source_as.providers:
                self.graph.ases[provider].receive_announcements(anns_to_providers)
            for peer in source_as.peers:
                self.graph.ases[peer].receive_announcements(anns_to_peers)

        if(to_customers):
            anns_to_customers = list()
            for ann in source_as.all_anns: 
                ann = source_as.all_anns[ann]
                new_path_length = ann.as_path_length + 1
                path_length_weighted = 1 - new_path_length/100

                cust_priority = 0 + path_length_weighted 
                this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 0 , None, cust_priority,new_path_length, ann.as_path)
                anns_to_customers.append(this_ann)
            for customer in source_as.customers:
                self.graph.ases[customer].receive_announcements(anns_to_customers)

        return

    def propagate_up(self):
        """Propagate announcements that came from customers to peers and providers
        
        """
        
        print("Propagating Announcements From Customers...")
        graph = self.graph
        progress = progress_bar(len(graph.ases_by_rank))
        for level in range(len(graph.ases_by_rank)):
            for asn in graph.ases_by_rank[level]:
                graph.ases[asn].process_announcements()
                if(graph.ases[asn].all_anns):
                    self.send_all_announcements(asn, to_peers_providers = True, 
                                               to_customers = False)
            progress.update()
        progress.finish()
        return

    def propagate_down(self):
        """From "top" to "bottom"  send the best announcements at every AS to customers

        """
        print("Propagating Announcements To Customers...")
        graph = self.graph 
        progress = progress_bar(len(graph.ases_by_rank))
        for level in reversed(range(len(graph.ases_by_rank))):
            for asn in graph.ases_by_rank[level]:
                graph.ases[asn].process_announcements()
                if(graph.ases[asn].all_anns):
                    self.send_all_announcements(asn, to_peers_providers = False,
                                                to_customers = True)
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

        #as_path is ordered right to left, so rev_path is the reverse
        rev_path = as_path[::-1]

        ann_to_check_for = Announcement(prefix,rev_path[0],hop,None,None,None,None)

        #i used to traverse as_path
        i = 0 
        for asn in rev_path: 
            i = i + 1
            if(asn not in self.graph.ases):
                continue
            comp_id = self.graph.ases[asn].SCC_id
            if(comp_id in self.ases_with_anns):
                #If AS has already recorded origin/prefix pair, stop
                if(self.graph.ases[comp_id].already_received(ann_to_check_for)):
                    continue
            sent_to = None
            #If not at the most recent AS (rightmost in rev_path), record the AS it is sent to next
            if(i<len(as_path)-1):
                #similar to rec_from() function, could get own function
                found_sent_to = 0
                asn_sent_to = rev_path[i+1]
                if(asn_sent_to not in self.graph.strongly_connected_components[comp_id]):
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
            
	    #Used to identify when ASes in path aren't neighbors
            broken_path = False
            #assign 'received_from' if AS isn't first in as_path
            received_from = 3
            if(i > 1):
                if(rev_path[i-1] in self.graph.ases[comp_id].providers):
                    received_from = 0
                elif(rev_path[i-1] in self.graph.ases[comp_id].peers):
                    received_from = 1
                elif(rev_path[i-1] in self.graph.ases[comp_id].customers):
                    received_from = 2
                elif(rev_path[i-1] == asn):
                    received_from = 3
                else:
                    broken_path = True
                    
            this_path_len = i-1
            path_length_weighted = 1 - this_path_len/100

            priority = received_from + path_length_weighted
            if(not broken_path):
                left_appendable_path = deque(as_path[-this_path_len:])
                announcement = Announcement(prefix,rev_path[0],hop,received_from,sent_to,priority,this_path_len,left_appendable_path)
                if(sent_to == 1 or sent_to == 0):
                    self.graph.ases[asn].sent_to_peer_or_provider(announcement)
                
                announcement = [announcement,]
                self.graph.ases[asn].receive_announcements(announcement)
                #ases_with_anns
                self.ases_with_anns.append(comp_id)

        self.ases_with_anns = list(set(self.ases_with_anns))
        return

    def prop_anns_sent_to_peers_providers(self):
        """Send announcements known to be sent to a peer or provider of each AS to
            the other peers and providers of each AS
       

        """
        print("Propagating Announcements Sent to Peers/Providers...")
        
        #For all ASes with announcements ( list made in give_anns_to_as_path() )
        for asn in self.ases_with_anns:
            #For all announcements received by an AS
            for ann in self.graph.ases[asn].anns_sent_to_peers_providers:
                ann = self.graph.ases[asn].anns_sent_to_peers_providers[ann]
                self.prop_one_announcement(asn,ann,1, None)
            return

    def insert_announcements(self,num_announcements = None):
        """Begins announcement propagation
            
        Args:
            num_announcements (:obj:`int`, optional): Number of announcements to propagate
      
        """

        print("\nInserting Announcements...")

        start_time = time.time()

        if(self.ann_input_table_name):
            records = self.querier.select_table(self.ann_input_table_name, num_announcements)
        else:
            records = self.querier.select_table('elements', num_announcements)

        for ann in records:
            if(ann.element_type == 'A'):
                self.give_ann_to_as_path(ann.as_path, ann.prefix, ann.next_hop)
        return

    def save_anns_to_db(self):
        print("Saving Propagation results to DB")
        progress = progress_bar(len(self.graph.ases))
        start_time = time.time()

        for asn in self.graph.ases:
            AS = self.graph.ases[asn]
        #    if asn == AS.SCC_id:
            sql_anns_arg = AS.anns_to_sql()
            self.querier.insert_results(asn,sql_anns_arg)
            progress.update()
        progress.finish()
        end_time = time.time()
        print("Time To Save Announcements: " + str(end_time - start_time) + "s")
        return
