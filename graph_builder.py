import os
import psutil
import time
import sys
import psycopg2
import SQL_functions
import named_tup
from AS import AS
from AS_Graph_iterative import AS_Graph
from progress_bar import progress_bar


def  add_neighbor(graph,asn,neighbor,relation):
    """Adds an AS relationship to the provided graph (dictionary)
    
        Appends (neighbor, relation) pair to list at graph[asn]
    
    Args:
        graph(:obj:`dict` of :obj:`list` of :obj:`named_tup.Relationship`): 
            Dictionary using ASNs as keys and values being lists of
            relationship data.
        asn(:obj:`int`):The identifying number of the system to which a 
            neighbor is added.
    """
    
    #append if this key has an entry already, otherwise make new entry
    if(asn not in graph.ases):
        graph.ases[asn] = AS(asn)
    graph.ases[asn].add_neighbor(neighbor,relation)
    return

def printDict(graph):
    """Prints entries for each node in a graph
    
    Args:
        graph(:obj:`dict` of :obj:`list` of :obj:`some object`): A dictionary
            with values being lists of some kind of object (Most likely 
            a namedtuple).
    
    """
    
    for node in graph:
        print("Entries for: " + str(node)) 
        for entry in graph[node]:  
            print(entry)
    return

#Creates a graph of ASes out of a dictionary / hash table
def create_relationship_graph(cursor, num_entries = None):
    """Creates and fills a dictionary with AS relationships
    
        e.g. as_graph[asn_0] = [(asn_1,relation_1),...,(asn_k,relation_k)]
    
    Args:
        cursor(:obj:`psycopg2 cursor`): cursor that has a connection to 
            a database containing a table of announcements. If not provided,
            a built in test set will be used
            
    Returns:
        graph(:obj:`dict` of :obj:`list` of :obj:`named_tup.Relationship`):
            Dictionary using ASNs as keys and values being lists of
            relationship information. 0/1/2 meaning provider/peer/customer.
            namedtuple format: (asn_1,relationship_1) => (50016,0).
            e.g. as_graph[asn_0] = [(asn_1,relation_1),...,(asn_k,relation_k)]
    """
    sys.stdout.write("Initializing Relationship Graph\n")

    #Select as_relationships table
#    if(num_entries is not None):
 #       cursor = SQL_functions.select_relationships(cursor,num_entries)
  #  else:
    numLines = SQL_functions.count_entries(cursor, 'relationships')
    cursor = SQL_functions.select_relationships(cursor)

    print("\tFilling Graph...")
    graph = AS_Graph()

    #Progress bar setup 
    start_time = time.time()
    if(num_entries is not None):
        i = 0
        progress = progress_bar(num_entries)
    else:
        progress = progress_bar(numLines)
    
    for record in cursor:
        named_r = named_tup.Relationship(*record)
        #If it's not a cone
        if named_r.cone_as is None:
            #If it's peer-peer (no customer)
            if(named_r.customer_as is None):
                add_neighbor(graph,named_r.peer_as_1,named_r.peer_as_2,1)
                add_neighbor(graph,named_r.peer_as_2,named_r.peer_as_1,1)
            #if it's provider-consumer
            if(named_r.provider_as is not None):
                add_neighbor(graph,named_r.customer_as[0],named_r.provider_as,0)
                add_neighbor(graph,named_r.provider_as,named_r.customer_as[0],2)
        progress.update()

        if(num_entries is not None):
            i = i + 1
            if(i>num_entries):
                break

    neighbor_time = time.time()
    sys.stdout.write('\n')
    cursor.close()
    print("\tAssigning Ranks...")
    graph.rank()
    for rank in graph.ases_by_rank:
        print("RANK: " + str(rank) + ": " + str(len(ases_by_rank[rank])) + " Entries.")
    for rank in graph.ases_by_rank:
       print("RANK " + str(rank) + ": " + str(ases_by_rank[rank]), file=open("output.txt","a"))
    ranking_time = time.time()

#    undefined = list()
 #   for asn in graph.ases:
  #      if(graph.ases[asn].rank is None):
   #         undefined.append(asn)
    #print("UNDEFINED ASES: " + str(len(undefined)))
#    print("STRONGLY CONNECTED COMPONENTS")
    i = 0
    print("Writing Components To File")
    progress = progress_bar(len(graph.strongly_connected_components))
    for component in graph.strongly_connected_components:
        i = i + 1
        print("COMPONENT " + str(i) + ": " + str(component), file=open("components_iter.txt","a"))
        progress.update()

    print("\nTime to construct relationship graph: " + str(neighbor_time - start_time) + "s") 
    print("Time to assign ranks: " + str(ranking_time - neighbor_time) + "s")
    print("Total time:  " + str(ranking_time - start_time) + "s")
    return graph
