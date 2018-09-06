'''
Created on Aug 23, 2018

@author: Mike P

May Require 'psycopg2'
"pip3 install psycopg2"
'''
import sys
import time
import named_tup
import SQL_functions
import json
from AS_Graph import AS_Graph
from AS import AS
from Announcement import Announcement
from Recipient_List import Recipient_List

def rec_from(as_path,as_graph):
    """Finds the relationship of the current AS in as_path to the previous AS
    
    Args:
        as_path (:obj:`list` of :obj:`int`): ASNs showing the path taken by
            an announcement. Leftmost being the most recent.
            
        as_graph (:obj:`dict` of :obj:`list` of :obj:`named_tup.Relationship`) 
            Dictionary using ASNs as keys and values being lists of
            tuples with relationship data.
        
    Returns: 
        (:obj:`int`,optional): 
            The relationship to the AS previously in as_path. 0/1/2 for
            provider/peer/customer of the current AS. Returns None if there 
            is no previous AS.

    """

    received_from = None
    #If path is 1 AS, return None
    if(len(as_path)==1):
        return received_from
    
    #For all neighbors in graph, check if they match the neighbor in path
    #When the correct neighbor is found, return the relationship
    #TODO sort neighbors to make this faster
    for provider in as_graph.ases[as_path[0]]:
        if(provider == as_path[1]):
            received_from = 0
            return received_from
    for peer in as_graph.ases[as_path[0]]:
        if(peer == as_path[1]):
            received_from = 1
            return received_from
    for customer in as_graph.ases[as_path[0]]:
        if(customer == as_path[1]):
            received_from = 2
            return received_from

def append_without_duplicates(this_list, asn, l = None, r = None):
    """Uses a binary search algorithm to insert an integer (asn) into a list 
        (this_list) with no duplicates.
    
    Args:
        this_list (:obj:`list` of :obj:`int`): Sorted list of integers
        asn (:obj:`int`): Integer to insert into the sorted list
        l (:obj:`int`, optional): Left index in binary search
        r (:obj:`int`, optional): Right index in binary search
    
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
                    return append_without_duplicates(this_list, asn, l, half-1)
            else:
                    return append_without_duplicates(this_list, asn, half+1, r)
    #if r is less than l, insert asn
    else:
            this_list[r+1:r+1] = [asn]
    return

#PROBABLY NOT USING ANYMORE
def append_announcement(some_dict,key, announcement):
    """Adds an announcement to a dictionary
        
    Appends announcement to list at some_dict[key]
        
    Args:
        some_dict (:obj:`dict`): dictionary to append to.

        key (:obj:`int`/:obj:`str`): key to use in dictionary
        announcement (:obj:`named_tup.Announcement`): namedtuple containing
            announcement data.

    """
    #append if this key has an entry already, otherwise make new entry
    if(key in some_dict):
        some_dict[key].append(announcement)
    else:
        some_dict[key] = [announcement]
    return

def best_from_multiple_prefixes(announcements,one_type = None):
    """Finds the best announcements from a list containing multiple prefixes
    
    Args:
        announcements (:obj:`list` of :obj:`named_tup.Announcement`): namedtuple            containing announcement data.
        one_type (:obj:`int`, optional): The presence of this variable 
            signifies that announcements all come from the same priority AS 
            e.g. provider, peer, or customer.
    Returns:
        (:obj:`list` of :obj:`named_tup.Announcement`): A list of namedtuple 
            containing announcement data.

    
    """

    #initialize list of best announcements
    best = list()
    #Create and fill dictionary that organizes announcements by prefix
    #by using the prefix as the key
    anns_by_prefix = dict()
    for ann in announcements:
        append_announcement(anns_by_prefix, ann.prefix, ann)
    
    #If all announcements have same relationship priority 
    #get the shortest announcement for each prefix and append to "best"
    #else get shortest and best relationship for each prefix
    if(one_type):
        for prefix in anns_by_prefix:
            best.append(best_by_length(anns_by_prefix[prefix]))
    else:
        for prefix in anns_by_prefix:
            best.append(best_by_relationship(anns_by_prefix[prefix]))
    return best

def best_by_relationship(announcements):
    """Finds the best announcement for a single prefix from a list with varying
        relationship types e.g. provider, peer, or customer and varying as_path
        lengths
    
    Args:
        announcements (:obj:`list` of :obj:`named_tup.Announcement`): 
            namedtuple containing announcement data.
            
    Returns:
        (:obj:`named_tup.Announcement`):namedtuple containing announcement data
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
    return best_by_length(best_anns)

def best_by_length(announcements):
    """Finds the announcement with the shortest as_path
    
    Args:
        announcements (:obj:`list` of :obj:`Announcement`): 
            namedtuple containing announcement information in format: 
            'origin prefix sent_to rec_from hop as_path'
        
    Returns:
        (:obj:`Announcement`): Announcement containing announcement data
    
    """

    bestLength = sys.maxsize
    best = None
    for ann in announcements:
        if(ann.as_path_length<bestLength):
            bestLength = ann.as_path_length
            best = ann
    return best

def prop_one_announcement(asn,as_graph,ann,ann_dict, to_peer_provider = None, 
        to_customer = None):
    """Send a single announcement to neighbors depending on arguments

    Args:
        asn(:obj:`int`): ASN of AS sending announcement.
        as_graph(:obj:`dict` of :obj:`list` of :obj:`named_tup.Relationship`) 
            a dictionary using ASNs as keys and values being lists of
            relationship information.
        ann(:obj:`named_tup.Announcement`): namedtuple containing announcement
            data.
        ann_dict(:obj:`dict` of :obj:`list` of :obj:`named_tup.Announcement`): 
            dictionary using ASNs as keys with values being lists of 
            announcements that AS has received.
        to_peer_provider(:obj:`int`, optional):Identification of whether or
            not the announcement is going to peers and providers.
        to_customer(:obj:`int`, optional):Identification of whether or 
            not the announcement is going to customers.
        ases_with_customer_announcements(:obj:`list`, optional):
            A list of ASNs for ASes that have announcements from customers.
        ases_with_provider_announcements:(:obj:`list`, optional):
            A list of ASNs for ASes that have announcements from providers.
    Todo:
        make arguments more readable on call

    """
    #TODO maybe replace 1s and 0s with Python booleans for readability
    #TODO make this code shorter
    send_to = Recipient_List()
    #start appending neighbors to "send_to" if they haven't already received it
    send_to_filtered = Recipient_List()

    if(to_peer_provider is not None):
        for peer in as_graph.ases[asn].peers:
            send_to.peers.append(peer)
       	for provider in as_graph.ases[asn].providers:
            send_to.providers.append(provider)
    if(to_customer is not None):
        for customer in as_graph.ases[asn].peers:
            send_to.customers.append(customer)

    for provider in send_to.providers:
        already_received = 0
        all_neighbor_anns =(as_graph.ases[provider].anns_from_customers +
                             as_graph.ases[provider].anns_from_peers_providers)
        for neighbor_ann in all_neighbor_anns:
            if((neighbor_ann.origin == ann.origin) and 
                (neighbor_ann.prefix == ann.prefix)):
                already_received = 1
                break
        if(not already_receieved):
            send_to_filtered.providers.append(provider)

    for peer in send_to.peers:
        already_received = 0
        all_neighbor_anns =(as_graph.ases[peer].anns_from_customers +
                             as_graph.ases[peer].anns_from_peers_providers)
        for neighbor_ann in all_neighbor_anns:
            if((neighbor_ann.origin == ann.origin) and 
                (neighbor_ann.prefix == ann.prefix)):
                already_received = 1
                break
        if(not already_receieved):
            send_to_filtered.peers.append(peer)

    for customer in send_to.customers:
        already_received = 0
        all_neighbor_anns =(as_graph.ases[customer].anns_from_customers +
                             as_graph.ases[customer].anns_from_peers_providers)
        for neighbor_ann in all_neighbor_anns:
            if((neighbor_ann.origin == ann.origin) and 
                (neighbor_ann.prefix == ann.prefix)):
                already_received = 1
                break
        if(not already_receieved):
            send_to_filtered.customers.append(customer)

    #Integer arguments 2/1/0 are for "received from" customer/peer/provider
    for provider in send_to_filtered.providers:
        this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 2 , None, ann.as_path_length + 1)
        append_announcement(graph.ases, provider, this_ann)
    for peer in send_to_filtered.peers:
        this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 1, None, ann.as_path_length + 1)
        append_announcement(graph.ases, provider, this_ann)
    for customer in send_to_filtered.peers:
        this_ann = Announcement(ann.prefix, ann.origin, ann.next_as, 0, None, ann.as_path_length + 1)
        append_announcement(graph.ases, provider, this_ann)

    return
  
  
                   
def prop_anns_from_customers_to_peers_providers(as_graph):
    """Propagate announcements that came from customers to peers and providers
    
    Args:
        ann_dict(:obj:`dict` of :obj:`list` of :obj:`named_tup.Announcement`): 
            dictionary using ASNs as keys with values being lists of 
            announcements that AS has received.
        as_graph(:obj:`dict` of :obj:`list` of :obj:`named_tup.Relationship`) 
            Dictionary using ASNs as keys and values being lists of
            relationship data.
        ases_with_customer_announcements(:obj:`list`, optional):
            a list of ASNs for ASes that have announcements from customers.
    
    Todo:
        Rank Ases to propagate lowest level to highest, to avoid repeating ASes
        Keep announcements of different priority in seperate lists
        Only keep best announcements of different prefix/origins/second_AS

    """
    
    print("\tPropagating Announcements From Customers")
    for level in ases_by_rank:
        for asn in level:
            #TODO throw out "bad" announcements when picking best
            #filter out best announcements from customers
            cust_anns = self.ases[asn].anns_from_customers
            best_anns_from_customers = best_from_multiple_prefixes(cust_anns,1)
            #if any announcements were collected, get the best for each prefix  
            #propagate the best customer sourced announcement for each prefix
            for ann in best_anns_from_customers:
                prop_one_announcement(asn, as_graph, ann, ann_dict, 1, None)
    return

def give_ann_to_as_path(as_path, prefix, hop, as_graph, 
                        ases_with_anns_sent_to_peers_providers):
    """Record announcement to all ASes on as_path
    
    Args:
        as_path(:obj:`list` of :obj:`int`): ASNs showing the path taken by
            an announcement. Leftmost being the most recent.
        prefix(:obj:`string`): An IP address subset e.g. 123.456.789/10 
            where 10 depicts the scope of the IP set.
        hop(:obj:`str`):An IP address used to record the next system to hop to.
        as_graph(:obj:`dict` of :obj:`list` of :obj:`named_tup.Relationship`) 
            Dictionary using ASNs as keys and values being lists of
            relationship data.
        ann_dict(:obj:`dict` of :obj:`list` of :obj:`named_tup.Announcement`): 
            dictionary using ASNs as keys with values being lists of 
            announcements that AS has received.
        ases_with_customer_announcements(:obj:`list`, optional):
            a list of ASNs for ASes that have announcements from customers.
    """
 
    print("\tPlacing Recorded Announcements...")
    #avoids error for anomaly announcement
    if(as_path is None):
            return
    #i used to traverse as_path
    i = 0
    #as_path is ordered right to left, so rev_path is the reverse
    rev_path = as_path[::-1]

    ases_with_anns = list()
    for asn in rev_path:
    #TODO order ases_with_anns
        if(asn in ases_with_anns):
            #If AS has already recorded origin/prefix pair, stop
            for ann2 in as_graph.ases[asn].all_announcements():
                #compare the origin to the first AS in rev_path and prefix to prefix
                if(ann2.origin==rev_path[0] and ann2.prefix==prefix):
                    return
        sent_to = None
        #If not at the most recent AS (rightmost in rev_path), record the AS it is sent to next
        if(i<len(as_path)-1):
            #similar to rec_from() function, could get own function
            found_sent_to = 0
            asn_sent_to = rev_path[i+1]
            if(asn_sent_to in as_graph.ases[asn].providers):
                sent_to = 0
                found_sent_to = 1
            if(not found_sent_to):
                if(asn_sent_to in as_graph.ases[asn].peers):
                    sent_to = 1
                    found_sent_to = 1
            if(found_sent_to == 0):
                if(asn_sent_to in as_graph.ases[asn].customers):
                    sent_to = 2
                    found_sent_to = 1

        #path for current AS removes "future" ASes
        this_path_len = i + 1

        if(i > 1):
            if(rev_path[i-1] in as_graph[asn].providers):
                received_from = 0
            if(rev_path[i-1] in as_graph[asn].peers):
                received_from = 1
            if(rev_path[i-1] in as_graph[asn].customers):
                received_from = 2
        else: received_from = None

        announcement = Announcement(prefix,rev_path[0],hop,received_from,sent_to,this_path_len)
        #append new announcement to ann_dict
        as_graph.ases[asn].give_announcement(announcement)
        ases_with_anns.append(asn)
        #increment i for path traversal
       	i = i + 1
    return

def prop_anns_sent_to_peers_providers(as_graph, ases_with_announcements):
    """Send announcements known to be sent to a peer or provider of each AS to
        the other peers and providers of each AS
        
    Args:
        ann_dict(:obj:`dict` of :obj:`list` of :obj:`named_tup.Announcement`): 
            dictionary using ASNs as keys with values being lists of 
            announcements that AS has received.
        as_graph(:obj:`dict` of :obj:`list` of :obj:`named_tup.Relationship`) 
            Dictionary using ASNs as keys and values being lists of
            relationship data.
        ases_with_customer_announcements(:obj:`list`, optional):
            a list of ASNs for ASes that have announcements from customers    
    
    Todo:
        Keep list instead of checking all ASes and announcements for sent_to

    """
    print("\tPropagating Announcements Sent to Peers/Providers...")
    
    #For all ASes with announcements
    for asn in ases_with_announcements:
        #For all announcements received by an AS
        for ann in as_graph[asn]:
            #If it was known to be sent to a peer/provider, send to other peers/providers
            if((ann.sent_to==0) or (ann.sent_to==1)):
                prop_one_announcement(asn, as_graph, ann, ann_dict, 1, None,ases_with_customer_announcements)
        return
def prop_best_to_customers(as_graph):
    """Iteratively send the best announcements at every AS to customers
    
    Args:
        ann_dict(:obj:`dict` of :obj:`list` of :obj:`named_tup.Announcement`): 
            dictionary using ASNs as keys with values being lists of 
            announcements that AS has received.
        as_graph(:obj:`dict` of :obj:`list` of :obj:`named_tup.Relationship`) 
            Dictionary using ASNs as keys and values being lists of
            relationship data.
    
    """
    #current_list = list(ready_dict.keys())
    current_list = list(ann_dict.keys())
    #for all ASes with announcements determine the best and send to customers
    while(current_list):
        asn = current_list.pop()
    ####TODO HERE
        best_announcements = best_from_multiple_prefixes(as_graph.ases[asn].anns_from_customers +
                            as_graph.ases[asn].anns_from_peer_providers)
        for ann in best_announcements:
            prop_one_announcement(asn, as_graph, ann, ann_dict, None, 1, None, current_list)
    return
def start_prop(output_file = None, as_graph = None, cursor = None):
    """Begins announcement propagation
        
        Calls:
            :meth:`~give_ann_to_as_path`\n
            :meth:`~prop_anns_sent_to_peers_providers`\n
            :meth:`~prop_anns_from_customers_to_peers_providers`\n
            :meth:`~prop_best_to_customers`
    Args:
        output_file(:obj:`str`, optional): output filename for ann_dict.
            If not provided, no file will be written.
        as_graph(:obj:`dict` of :obj:`list` of :obj:`named_tup.Relationship`) 
            Dictionary using ASNs as keys and values being lists of
            relationship data. If not provided, built in test set will be used.
        cursor(:obj:`psycopg2 cursor`): cursor that has a connection to 
            a database containing a table of announcements. If not provided,
            built in test set will be used.
    
    """
    print("Beginning Announcement Propagation...")

#ann_dict collects all announcements for each AS
#ready_dict collects announcements until they are sent
#ready_list lists ASes with entries in ready_dict

#as_graph normally comes from AS_graph_builder but is created manually here
#as_graph format is key: {(ASN,relationship)}, where relationship is 0/1/2, provider/peer/customer

    ann_dict = dict()

    if(as_graph is None):
        as_graph = AS_Graph()
        for i in range(1,14):
            as_graph.ases[i] = AS(i)    
        #Small manually made graph for small scale testing
        as_graph.ases[1].add_neighbor(2,2)
        as_graph.ases[2].add_neighbor(1,0)
        as_graph.ases[2].add_neighbor(3,2)
        as_graph.ases[3].add_neighbor(2,0)
        as_graph.ases[3].add_neighbor(4,2)
        as_graph.ases[4].add_neighbor(3,0)
        as_graph.ases[4].add_neighbor(5,0)
        as_graph.ases[4].add_neighbor(8,0)
        as_graph.ases[5].add_neighbor(4,2)
        as_graph.ases[5].add_neighbor(6,0)
        as_graph.ases[6].add_neighbor(5,2)
        as_graph.ases[6].add_neighbor(7,1)
        as_graph.ases[7].add_neighbor(6,1)
        as_graph.ases[8].add_neighbor(4,2)
        as_graph.ases[8].add_neighbor(9,2)
        as_graph.ases[8].add_neighbor(10,0)
        as_graph.ases[9].add_neighbor(8,0)
        as_graph.ases[10].add_neighbor(8,2)
        as_graph.ases[10].add_neighbor(11,0)
        as_graph.ases[10].add_neighbor(12,2)
        as_graph.ases[11].add_neighbor(10,2)
        as_graph.ases[12].add_neighbor(10,0)
        as_graph.ases[12].add_neighbor(13,0)
        as_graph.ases[13].add_neighbor(12,2)
        
    start_time = time.time()

    if(cursor == None):
        announcements = list()
        #Announcements from DB come in form (PRIMARY KEY,Type, ASN, Address, AS_PATH, prefix, NEXT_HOP (IP), Record ID)
        record1 = (1,'A',11,"192.168.1.1",[12,11],'111.111.111/20','197.50.78.101',12)
        record2 = (1,'A',10,"192.168.1.1",[10,5,1,2,3],'222.222.222/20','197.50.78.101',13)
        announcements.extend((record1,record2))   
    else:
        announcements = SQL_functions.select_announcements(cursor)
    
    for ann in announcements:
        #DB can be changed to return named items rather than only indexed
        give_ann_to_as_path(ann[4],ann[5], ann[6], as_graph, ann_dict)
            
    prop_anns_sent_to_peers_providers(ann_dict,as_graph.ases, ases_with_customer_announcements)
    prop_anns_from_customers_to_peers_providers(ann_dict, as_graph.ases, ases_with_customer_announcements)
    prop_best_to_customers(ann_dict, as_graph.ases)
    
    end_time = time.time()

    if(output_file is not None):
        with open(output_file, 'w') as fp:
            json.dump(ann_dict, fp)
    print("\t\tPropagation Took: " + str(end_time - start_time) + "s")
    return ann_dict
