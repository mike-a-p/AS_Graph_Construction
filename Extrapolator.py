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
        self.insert_announcements(num_announcements)
        self.prop_anns_sent_to_peers_providers()
        self.propagate_up()
        start_time = time.time()
        self.propagate_down()
        end_time = time.time()
        print("Time To Propagate Down: " + str(end_time - start_time) + "s")
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
        send_to = Recipient_List()
        source_as = self.graph.ases[asn]
        #Integer arguments 2/1/0 are for "received from" customer/peer/provider
        if(to_peer_provider is not None):
            for provider in source_as.providers:
                this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 2 , None, ann.as_path_length + 1)
                self.graph.ases[provider].give_announcement(this_ann) 

            for peer in send_to.peers:
                this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 1, None, ann.as_path_length + 1)
                self.graph.ases[peer].give_announcement(this_ann)

        if(to_customer is not None):
            for customer in send_to.customers:
                this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 0, None, ann.as_path_length + 1)
                self.graph.ases[customer].give_announcement(this_ann)
        return
                       
    def propagate_up(self):
        """Propagate announcements that came from customers to peers and providers
        
        """
        
        print("Propagating Announcements From Customers...")
        graph = self.graph
        progress = progress_bar(len(self.graph.ases_by_rank))
        for level in range(len(self.graph.ases_by_rank)):
            for asn in self.graph.ases_by_rank[level]:
                #filter out best announcements from customers
                cust_anns = self.graph.ases[asn].anns_from_customers
                #send out best announcements
                for ann in cust_anns:
                    ann = cust_anns[ann]
                    self.prop_one_announcement(asn, ann, 1, None)
                for ann in anns_from_self:
                    ann = anns_from_self[ann]
                    self.prop_one_announcement(asn, ann, 1, None)
            progress.update()
        progress.finish()
        return

    def propagate_down(self):
        """From "top" to "bottom"  send the best announcements at every AS to customers

        """
        print("Propagating Announcements To Customers...")
        progress = progress_bar(len(self.graph.ases_by_rank))
        for level in reversed(range(len(self.graph.ases_by_rank))):
            for asn in self.graph.ases_by_rank[level]:
                cust_anns = self.graph.ases[asn].anns_from_customers
                for ann in cust_anns:
                    ann = cust_anns[ann]
                    self.prop_one_announcement(asn,ann,None, 1)
                peer_prov_anns = self.graph.ases[asn].anns_from_peers_providers
                for ann in peer_prov_anns:
                    ann = peer_prov_anns[ann]
                    self.prop_one_announcement(asn,ann,None, 1)
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

        ann_to_check_for = Announcement(prefix,rev_path[0],hop,None,None,None)

        for asn in rev_path:
        #TODO order ases_with_anns
            if(asn not in self.graph.ases):
                continue
            comp_id = self.graph.ases[asn].SCC_id
            if(comp_id in self.ases_with_anns):
                #If AS has already recorded origin/prefix pair, stop
                if(self.graph.ases[comp_id].already_received(ann_to_check_for)):
                    return
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
                self.graph.ases[asn].sent_to_peer_or_provider(announcement)
            #ases_with_anns
            self.ases_with_anns.append(comp_id)
            #increment i for path traversal
            i = i + 1

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
