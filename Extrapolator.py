'''
Created on Aug 23, 2018

@author: Mike P

May Require 'psycopg2'
"pip3 install psycopg2"
'''
import sys
import time
import math
import named_tup
from SQL_querier import SQL_querier
import json
from AS_Graph import AS_Graph
from AS import AS
from Announcement import Announcement
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
            print("Table name \"" + table_name + "\" not found in database. Check config file or database.")
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

    def perform_propagation(self, max_total_anns = None,max_memory = None, test = False):
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
    
        progress = progress_bar(len(list(self.graph.ases_by_rank.keys())))
        for rank in list(self.graph.ases_by_rank.keys()):
            time.sleep(1)
            progress.update()
    
        if(max_memory is None):
            #MB
            max_memory = 20000
        max_group_anns = math.floor(max_memory/2.9)
       
        print("Ordering prefixes by frequency...") 
        prefix_counts = self.querier.count_prefix_amounts(self.ann_input_table_name)
        i = len(prefix_counts)-1
        j = len(prefix_counts)-1

        total_anns = 0
        stop = False
        while(i >=0 and ((not max_total_anns) or total_anns + prefix_counts[i].count <= max_total_anns)):
            anns_in_group = 0
            while(i>=0 and anns_in_group + prefix_counts[i].count < max_group_anns):
                if(max_total_anns and total_anns + prefix_counts[i].count > max_total_anns):
                    break
                anns_in_group+= prefix_counts[i].count
                total_anns +=prefix_counts[i].count
                i-=1
            prefixes_to_use = prefix_counts[i+1:j+1]
            j=i

            self.insert_announcements(prefixes_to_use)

#            self.prop_anns_sent_to_peers_providers()
            self.propagate_up()
            start_down = time.time()
            self.propagate_down()
            if(not test):
                self.save_anns_to_db()

            self.graph.clear_announcements()
        
        if(not test):
            self.graph.save_graph_to_db()

        end_time = time.time()
        print("Total Time To Extrapolate: " + str(end_time - start_time) + "s")
        return
   
    def send_all_announcements(self,asn,to_peers_providers = None, to_customers = None):
        source_as = self.graph.ases[asn]
        if(to_peers_providers):
            anns_to_providers = list()
            anns_to_peers = list()
            for ann in source_as.all_anns:
                ann = source_as.all_anns[ann]
                #Peers/providers should not be send anns that came from peers/providers
                if(ann.priority < 2):
                    continue

                new_length_priority = ann.priority - int(ann.priority)
                new_length_priority -=  0.01
                new_priority = 2 + new_length_priority
                
                this_ann = Announcement(ann.prefix, ann.origin, new_priority, asn)
                anns_to_providers.append(this_ann)
                
                new_priority = 1 + new_length_priority
                this_ann = Announcement(ann.prefix, ann.origin, new_priority, asn)
                anns_to_peers.append(this_ann)

            for provider in source_as.providers:
                self.graph.ases[provider].receive_announcements(anns_to_providers)
            for peer in source_as.peers:
                self.graph.ases[peer].receive_announcements(anns_to_peers)

        if(to_customers):
            anns_to_customers = list()
            for ann in source_as.all_anns: 
                ann = source_as.all_anns[ann]
                new_length_priority = ann.priority - int(ann.priority)
                new_length_priority -=  0.01
                new_priority = 2 + new_length_priority
                this_ann = Announcement(ann.prefix, ann.origin, new_priority, asn)
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

        ann_to_check_for = Announcement(prefix,rev_path[0],None,None)

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
                announcement = Announcement(prefix,rev_path[0],priority,rev_path[i-1])
                if(sent_to == 1 or sent_to == 0):
                    self.graph.ases[asn].sent_to_peer_or_provider(announcement)
                
                announcement = [announcement,]
                self.graph.ases[asn].receive_announcements(announcement)
                #ases_with_anns
                self.ases_with_anns.append(comp_id)

        self.ases_with_anns = list(set(self.ases_with_anns))
        return

#TODO FIX this to not use prop_one
    def prop_anns_sent_to_peers_providers(self):
        """Send announcements known to be sent to a peer or provider of each AS to
            the other peers and providers of each AS
       

        """
        print("Propagating Announcements Sent to Peers/Providers...")
        
        #For all ASes with announcements ( list made in give_anns_to_as_path() )
        for asn in self.ases_with_anns:
            AS = self.graph.ases[asn]
            AS.process_announcements()
            #For all announcements received by an AS
            for ann in AS.anns_sent_to_peers_providers:
                ann = AS.anns_sent_to_peers_providers[ann]
                self.prop_one_announcement(asn,ann,1, None)
            return

    def insert_announcements(self,prefixes):
        """Begins announcement propagation
            
        Args:
            num_announcements (:obj:`int`, optional): Number of announcements to propagate
      
        """

        print("\nInserting Announcements...")

        start_time = time.time()

        records = list()
        for prefix in prefixes:
            records.append(self.querier.select_anns_by_prefix(self.ann_input_table_name,prefix.prefix))

        for prefix_anns in records:
            for ann in prefix_anns:
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
