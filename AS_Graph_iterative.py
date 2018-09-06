import sys
import os
import psutil
from progress_bar import progress_bar
from collections import deque
from AS import AS

class AS_Graph:

    def __init__(self):
        self.ases = dict()
        self.ases_with_anns = list()
        self.ases_by_rank = list()
        self.strongly_connected_components = list()

    def __str__(self):
        return str(self.ases)

    def rank(self):
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

        print("\tCombining Components")
        #TODO allow for announcements with known paths to be given to large components
        large_components = list()
        for component in self.strongly_connected_components:
            if(len(component)>1):
                large_components.append(component)
        progress = progress_bar(len(large_components))

        for component in large_components:
            #Create an AS using an "inner" AS to avoid collision
            #TODO maybe change it to some known unique value, ideally integer
            #grab ASN of first AS in component
            new_asn = self.ases[component[0]].asn
            combined_AS = AS(new_asn)
            combined_cust_anns = list()
            combined_peer_prov_anns = list()

            #get providers, peers, customers from "inner" ASes
            #only if they aren't also in "inner" ASes
            for asn in component:

                if(self.ases[asn].anns_from_customers):
                    combined_cust_anns.extend(self.ases[asn].anns_from_customers)
                if(self.ases[asn].anns_from_peers_providers):
                    combined_peer_prov_anns.extend(self.ases[asn].anns_from_peers_providers)

                for provider in self.ases[asn].providers:
                    if(provider not in component):
                        combined_AS.add_neighbor(provider, 0)
                        #replace old customer reference from provider
                        #TODO maybe make faster removal function since list is sorted
                        prov_AS = self.ases[provider]
                        prov_AS.customers.remove(asn)
                        prov_AS.append_no_dup(prov_AS.customers,new_asn)
                for peer in self.ases[asn].peers:
                    if(peer not in component):
                        combined_AS.add_neighbor(peer, 1)
                        peer_AS = self.ases[peer]
                        peer_AS.peers.remove(asn)
                        peer_AS.append_no_dup(peer_AS.peers,new_asn)
                for customer in self.ases[asn].customers:
                    if(customer not in component):
                        combined_AS.add_neighbor(customer,2)
                        cust_AS = self.ases[customer]
                        cust_AS.providers.remove(asn)
                        cust_AS.append_no_dup(cust_AS.providers,new_asn)
                self.ases.pop(asn,None)
            combined_AS.anns_from_customers = combined_cust_anns
            combined_AS.anns_from_peers_providers = combined_peer_prov_anns
            self.ases[combined_AS.asn] = combined_AS
            progress.update()
        print()
        return

    def find_strong_conn_components(self):

        #Begins Tarjan's Algorithm
        #index is node id in DFS from 
        index = 0
        stack = list()
        components = list()
        
        print("\tFinding Strongly Connected Components")
        progress = progress_bar(len(self.ases))
        
        for asn in self.ases:
            index = 0
            AS = self.ases[asn]
            if(AS.index is None):
                self.strong_connect(AS,index,stack,components)
            progress.update()
        self.strongly_connected_components = components
        print()
        return components

    def strong_connect(self,AS,index,stack,components):
    #Generally follows Tarjan's Algorithm. Does not use real recursion

        iteration_stack = list()
        iteration_stack.append(AS)
        SCC_id = 0

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
                components.append(component)
                SCC_id = SCC_id + 1
            
            #if "dead end" was hit and it's not part of component
            if(iteration_stack):
                prov = node
                node = iteration_stack[-1]
                node.lowlink = min(node.lowlink, prov.lowlink)
        return

    def decide_ranks(self):
        customer_ases = list()
        ases_by_rank = dict()

        for asn in self.ases:
            if(not self.ases[asn].customers):
                customer_ases.append(asn) 
                self.ases[asn].rank = 0
        ases_by_rank[0] = customer_ases

        for i in range(1000):
            ases_at_rank_i_plus_one = list()

            if(i not in ases_by_rank):
                return ases_by_rank

            for asn in ases_by_rank[i]:
                for provider in self.ases[asn].providers:
                    prov_AS = self.ases[provider]
                    if(prov_AS.rank is None):
                        skip_provider = 0
                        
                        for prov_cust in prov_AS.customers:
                            prov_cust_AS = self.ases[prov_cust]
                            if (prov_cust_AS.rank is None and
                                prov_cust_AS.SCC_id != prov_AS.SCC_id):
                                skip_provider = 1
                                break
                        if(skip_provider):
                                continue
                        else:
                            self.ases[provider].rank = i + 1
                            self.append_no_dup(ases_at_rank_i_plus_one,provider)
            if(ases_at_rank_i_plus_one):
                ases_by_rank[i+1] = ases_at_rank_i_plus_one
        self.ases_by_rank = ases_by_rank
        return ases_by_rank
         

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

