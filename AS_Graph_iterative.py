import sys
import os
import psutil
import psycopg2
import named_tup
import time
import gc
from collections import deque
from datetime import datetime
from progress_bar import progress_bar
from AS import AS
from SQL_querier import SQL_querier

class AS_Graph:

    def __init__(self):
        self.querier = SQL_querier()
        self.ases = dict()
        self.ases_with_anns = list()
        self.ases_by_rank = dict()
        self.strongly_connected_components = dict()
        self.date_time = datetime.now()
        
        return

    def __repr__(self):
        return str(self.ases)

    #TODO possibly add user prompt if table name is wrong 
    #TODO customize response (will say not found if lacking permissions).
    #Or just allow database to throw it's error and give stacktrace

    #ask to use default, change, or quit
    def set_peers_table(self,table_name):
        exists = self.querier.exists_table(table_name)
        if(exists):
            self.peers_table_name = table_name
        else:
            print("Table name \"" + table_name + "\" not found in database. Check config file.")
            sys.exit()
        return

    def set_customer_provider_table(self,table_name):
        exists = self.querier.exists_table(table_name)
        if(exists):
            self.customer_provider_table_name = table_name
        else: 
            print("Table name \"" + table_name + "\" not found in database. Check config file.")
            sys.exit()
        return

    def set_graph_table(self,table_name):
        exists = self.querier.exists_table(table_name)
        if(exists):
            self.graph_table_name = table_name
        else: 
            print("Table name \"" + table_name + "\" not found in database. Check config file.")
            sys.exit()
        return   

    def  add_relationship(self, asn,neighbor,relation):
        """Adds an AS relationship to the provided graph (dictionary)

        """
        
        #append if this key has an entry already, otherwise make new entry
        if(asn not in self.ases):
            self.ases[asn] = AS(asn)
        self.ases[asn].add_neighbor(neighbor,relation)
        return

    #TODO add param for graph date/version
    def load_pre_built(self):
        querier = SQL_querier()
        numLines = querier.count_entries(self.graph_table_name)
        records = querier.select_table(self.graph_table_name)

        print("\nReading from as_graph table")
        progress = progress_bar(numLines)
        for record in records:
            current_as = AS(record.asn)
            current_as.customers = record.customers
            current_as.peers = record.peers
            current_as.providers = record.providers
            current_as.SCC_id = record.asn
            current_as.rank = record.rank

            if(current_as.rank not in self.ases_by_rank):
                self.ases_by_rank[current_as.rank] = list()
            self.ases_by_rank[current_as.rank].append(current_as.asn)
            self.strongly_connected_components[current_as.SCC_id] = record.members

            for asn in record.members:
                current_as.asn = asn
                self.ases[asn] = current_as
            progress.update()
        progress.finish()
        return

    def update(self):
        #Graph is updated using one-to-one relationships stored in DB.
        #Newly constructed graph is assigned ranks (which includes combining
        #strongly connected components).
        self.read_seperate_relationships_from_db()
        self.assign_ranks()
        return

    def save_graph_to_db(self):
        querier = SQL_querier()
        querier.insert_as_graph_into_db(self,self.graph_table_name)
        return

    def read_seperate_relationships_from_db(self):
        print("Initializing Relationship Graph")
        querier = SQL_querier()
        numLines = querier.count_entries(self.peers_table_name)
        records = querier.select_table(self.peers_table_name)
        for record in records:
            self.add_relationship(record.peer_as_1,record.peer_as_2,1)
            self.add_relationship(record.peer_as_2,record.peer_as_1,1)

        numLines = querier.count_entries(self.customer_provider_table_name)
        records = querier.select_table(self.customer_provider_table_name)
        for record in records:
            self.add_relationship(record.customer_as,record.provider_as,0)
            self.add_relationship(record.provider_as,record.customer_as,2)
        return

    def assign_ranks(self):
        self.find_strong_conn_components()
        self.combine_components()
        self.decide_ranks()
        return

    def combine_components(self):
        """Takes the SCCs of this graph and exchanges them for "super nodes".
            These super nodes have the providers, peers and customers than all
            nodes in the SCC would have. These providers, peers, and customers
            point to the new super node.


        """

        print("\nCombining Components")
        large_components = list()
        for component in self.strongly_connected_components:
            comp = self.strongly_connected_components[component]
            if(len(comp)>1):
                large_components.append(comp)
        
        progress = progress_bar(0,len(large_components))
        for component in large_components: 
            progress.next_job(len(component))

            #Create an AS using an "inner" AS to avoid collision
            #TODO maybe change it to some known unique value, ideally integer
            #grab ASN of first AS in component
            new_asn = self.ases[component[0]].SCC_id
            combined_AS = AS(new_asn)
            combined_AS.SCC_id = new_asn
            combined_cust_anns = list()
            combined_peer_prov_anns = list()

            #get providers, peers, customers from "inner" ASes
            #only if they aren't also in "inner" ASes
            for asn in component:
                for provider in self.ases[asn].providers:
                    if(provider not in component):
                        combined_AS.append_no_dup(combined_AS.providers,provider)
              #          combined_AS.add_neighbor(provider,0)
                        #replace old customer reference from provider
                        prov_AS = self.ases[provider]
                        prov_AS.customers.remove(asn)
                        prov_AS.append_no_dup(prov_AS.customers,new_asn)
                for peer in self.ases[asn].peers:
                    if(peer not in component): 
                        combined_AS.append_no_dup(combined_AS.peers,peer)
               #         combined_AS.add_neighbor(peer,1)
                        peer_AS = self.ases[peer]
                        peer_AS.peers.remove(asn)
                        peer_AS.append_no_dup(peer_AS.peers,new_asn)
                for customer in self.ases[asn].customers:
                    if(customer not in component):
                        combined_AS.append_no_dup(combined_AS.customers,customer)
                #        combined_AS.add_neighbor(customer,2)
                        cust_AS = self.ases[customer]
                        cust_AS.providers.remove(asn)
                        cust_AS.append_no_dup(cust_AS.providers,new_asn)
                self.ases[asn] = combined_AS
                progress.update()

            self.ases[combined_AS.asn] = combined_AS
        progress.finish()
        return

    def find_strong_conn_components(self):

        #Begins Tarjan's Algorithm
        #index is node id in DFS from 
        index = 0
        stack = list()
        components = dict()
        
        print("\nFinding Strongly Connected Components")
        progress = progress_bar(len(self.ases))
        
        for asn in self.ases:
            index = 0
            AS = self.ases[asn]
            if(AS.index is None):
                self.strong_connect(AS,index,stack,components)
            progress.update()
        self.strongly_connected_components = components
        progress.finish()
        return components

    def strong_connect(self,AS,index,stack,components):
    #Generally follows Tarjan's Algorithm. Does not use real recursion

        iteration_stack = list()
        iteration_stack.append(AS)

        while(iteration_stack):
            node = iteration_stack.pop()
            #If node hasn't been visited yet
            #initialize Tarjan variables
            if(node.index is None):
                node.index = index
                node.lowlink = index
                index = index + 1
                stack.append(node)
                node.onstack = True
            recurse = False
            for provider in node.providers:
                prov_AS = self.ases[provider]
                if(prov_AS.index is None):
                    iteration_stack.append(node)
                    iteration_stack.append(prov_AS)
                    recurse = True
                    break
                elif(prov_AS.onstack == True):
                    node.lowlink = min(node.lowlink, prov_AS.index)
            #if recurse is true continue to top of "iteration_stack"
            if(recurse): continue
            if(node.lowlink == node.index):
                SCC_id = node.asn
                #DO pop node WHILE node != top
                #stack until "node" is ASes in current component
                component = list()
                while(True):
                    top = stack.pop()
                    top.onstack = False
                    top.SCC_id = SCC_id
                    component.append(top.asn)
                    if(node == top):
                        break
                components[SCC_id] = component
            
            #if "dead end" was hit and it's not part of component
            if(iteration_stack):
                prov = node
                node = iteration_stack[-1]
                node.lowlink = min(node.lowlink, prov.lowlink)
        return

    def decide_ranks(self):
        customer_ases = list()

        for asn in self.ases:
            if(not self.ases[asn].customers):
                customer_ases.append(asn) 
                self.ases[asn].rank = 0
        self.ases_by_rank[0] = customer_ases

        for i in range(1000):
            ases_at_rank_i_plus_one = list()

            if(i not in self.ases_by_rank):
                return self.ases_by_rank

            for asn in self.ases_by_rank[i]:
                for provider in self.ases[asn].providers:
                    prov_AS = self.ases[provider]
                    if(prov_AS.rank is None):
                        skip_provider = 0
                        
                        for prov_cust in prov_AS.customers:
                            prov_cust_AS = self.ases[prov_cust]
                            if ((prov_cust_AS.rank is None and
                                prov_cust_AS.SCC_id != prov_AS.SCC_id) or prov_cust_AS.rank > i ):
                                skip_provider = 1
                                break
                        if(skip_provider):
                                continue
                        else:
                            self.ases[provider].rank = i + 1
                            self.append_no_dup(ases_at_rank_i_plus_one,provider)
            if(ases_at_rank_i_plus_one):
                self.ases_by_rank[i+1] = ases_at_rank_i_plus_one
        return self.ases_by_rank

    def clear_announcements(self):
        #Removes all announcement references from ASes and garbage collects
        for asn in self.ases:
            AS = self.ases[asn]
            AS.clear_announcements()         
           # gc.collect()
        return
    def append_no_dup(self, this_list, asn, l = None, r = None):
   
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

