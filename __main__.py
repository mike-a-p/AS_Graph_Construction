from Extrapolator import extrapolator
import argparse
from configparser import ConfigParser
#import pudb; pu.db



def main(args):
    cparser = ConfigParser()
    cparser.read("tables.conf")
    extrap = extrapolator()

    extrap.set_ann_input_table(cparser['tables']['input'])
    extrap.set_results_table(cparser['tables']['output'])
    extrap.set_peers_table(cparser['tables']['peers'])
    extrap.set_customer_provider_table(cparser['tables']['customer_providers'])
    extrap.set_graph_table(cparser['tables']['graph'])

    if(args['graph']):
        extrap.graph.load_graph_from_db()
    else:
        extrap.graph.update()

    extrap.perform_propagation(max_total_anns = args['announcement_count'], 
                                max_memory = args['announcement_memory'],
                                test = args['test'])
    
    if(not args['test']):
        extrap.graph.save_graph_to_db()        
        extrap.save_anns_to_db()

#TODO let -g argument take a date to load from
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-g","--graph", type=str,
                        help="use a pre-processed AS graph")
    parser.add_argument("-t","--test", action="store_true",
                        help="run in test mode (doesn't save to database)")
    parser.add_argument("-a","--announcement_count",type=int,
                        help="specify the number of announcements to read from database and propagate")
    parser.add_argument("-m","--announcement_memory",type=int,
                        help="""specify the amount of memory announcements should take.
                                 Breaks propagation into segments""")

    args = parser.parse_args()
    return vars(args)

if __name__=="__main__":
    args = parse_arguments()
    main(args)
