import psycopg2
import psycopg2.extras
import re
from datetime import date
from lib_bgp_data import Database
from progress_bar import progress_bar
from named_tup import What_if_tup
import random
import sys
#imports should be modified with file rearrangement

class SQL_querier:
    def __init__(self):
        self.database = Database(cursor_factory=psycopg2.extras.NamedTupleCursor)
        self.today = date.today()
        return

    def set_results_table(self,table_name):
        exists = self.exists_table(table_name)
        if(exists):
            self.results_table_name = table_name
        else:
            print("Table name \"" + table_name + "\" not found in database. Check config file.")
            sys.exit()
        return

    def select_table(self, table_name, num_entries = None):
        data = None
        if(num_entries):
            sql = "SELECT * FROM " + table_name +" LIMIT (%s);"
            data = (num_entries,)
            print("Selecting " + str(num_entries) + " \"" + table_name + "\" Records...")
        else:
            sql = "SELECT * FROM " + table_name +";"
            print("Selecting \"" + table_name + "\" Records...")
        return self.database.execute(sql,data)

    def select_row(self,table_name,primary_key_name = None,primary_key = None, customer_as = None ,provider_as = None):
        #Finds the row that contains the customer-provider
        #relationship between two ASes
        if(asn):
            sql = "SELECT * FROM " + table_name + " WHERE "+ primary_key_name +" = (%s);"
            data = (asn,)
        else:
            sql = "SELECT * FROM " + table_name + " WHERE customer_as = (%s) AND provider_as = (%s);"
            data = (customer_as,provider_as)
        record = self.database.execute(sql,data)
        return record

    def exists_edge(self,table_name,customer_as = None, provider_as = None):
        record = self.select_row(table_name,None,None,customer_as, provider_as)
        if(record):
            exists = True
        else:
            exists = False
        return exists

    def exists_table(self,table_name):
        sql = "SELECT EXISTS(SELECT * FROM information_schema.tables where table_name=(%s));"
        data = (table_name,)
        record = self.database.execute(sql,data)
        record = record[0].exists
        return(record)

    def count_entries(self,table_name):
        """Counts number of entries in desired table.

        Args: 
            table_name (:obj:`str`): name of table to count entries in.
        
        Returns:
            (:obj:`int`): number of entries found in a given table.

        """
        sql = "SELECT COUNT(*) FROM " + table_name + ";"
        count = self.database.execute(sql)

        print("\nCounting \"" + table_name + "\" Entries...")

        numLines = count[0].count
        print("\t" + str(numLines) + " Entries")
        return numLines

    def insert_results(self,asn, sql_anns):
        if(self.exists_row(table_name = self.results_table_name,primary_key_name = 'asn',primary_key = asn)):
            sql = ("""UPDATE """ + self.results_table_name + """ SET announcements = announcements||((%s)::announcement[])
                    WHERE asn = (%s);""")
            data = (sql_anns,asn)
        else:
            sql = """INSERT INTO """ + self.results_table_name + """ VALUES ((%s),(%s)::announcement[]);"""
            data = (asn,sql_anns)
        self.database.execute(sql,data)
        return

    def insert_as_graph_into_db(self,graph,graph_table_name):
        print("Saving AS Graph to DB")
        #TODO implement graph_id programatically, date or something
        #If records exist for graph today, overwrite data by deleting entries first
        sql = "DELETE FROM " + graph_table_name + " WHERE graph_date = (%s);"
        data = (self.today,)
        self.database.execute(sql,data)

        progress = progress_bar(len(graph.ases))
        sql = "INSERT INTO " + graph_table_name + " VALUES (DEFAULT,(%s),(%s),(%s),(%s),(%s),(%s),(%s));"
        for asn in graph.ases:
            AS = graph.ases[asn]
            if(AS.SCC_id == asn):
                members = graph.strongly_connected_components[asn]
                data = (asn, AS.customers, AS.peers, AS.providers,members,AS.rank,self.today)
                self.database.execute(sql,data)
            progress.update()
        progress.finish()
        return

    def insert_random_what_ifs(self):
        sql = "INSERT INTO api.whatif VALUES ((%s),(%s),(%s),(%s))"
        progress = progress_bar(50000)   
        used_asns = dict() 

        for i in range(50000):
            unique_asn = False
            while(not unique_asn):
                asn = random.randint(1,300000)
                if(asn not in used_asns):
                    used_asns[asn] = True
                    unique_asn = True

            policy_1 = What_if_tup(random.uniform(0,1),random.uniform(0,1))
            policy_2 = What_if_tup(random.uniform(0,1),random.uniform(0,1))
            policy_3 = What_if_tup(random.uniform(0,1),random.uniform(0,1))
            data = (asn,policy_1,policy_2,policy_3)
            self.database.execute(sql,data)
            progress.update()
        progress.finish()

    def insert_ann_occurances(self,prefix_origin_hash,occurances):
        if(self.exists_row(table_name = 'ann_occurances',primary_key_name = 'prefix_origin_hash',
                            primary_key = prefix_origin_hash)):
            sql = "UPDATE ann_occurances SET occurances = occurances ||(%s) WHERE prefix_origin_hash = (%s)"
            data = ([occurances,],prefix_origin_hash)
        else:
            sql = "INSERT INTO ann_occurances VALUES ((%s),(%s))"
            data = (prefix_origin_hash,[occurances,])
        self.database.execute(sql,data)

