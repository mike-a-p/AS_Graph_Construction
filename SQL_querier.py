import psycopg2
import psycopg2.extras
import re
from lib_bgp_data import Database
from progress_bar import progress_bar
#imports should be modified with file rearrangement

class SQL_querier:
    def __init__(self):
        self.database = Database(cursor_factory=psycopg2.extras.NamedTupleCursor)
        return

    def close(self,cur,conn):
        """Cleans up database connection

        Args:
            cur(:obj:`psycopg2 cursor`): cursor that points to specific database
                elements.
            conn(:obj:`psycopg2 conn`): conn that holds connection to database.

        """
    #database currently doesn't have a close() function
    #    self.database.close()
        return

    def select_table(self, table_name, num_entries = None):
        data = None
        if(num_entries):
            sql = "SELECT * FROM " + table_name +" LIMIT (%s);"
            data = (num_entries,)
            print("\tSelecting " + str(num_entries) + " \"" + table_name + "\" Records...")
        else:
            sql = "SELECT * FROM " + table_name +";"
            print("\tSelecting \"" + table_name + "\" Records...")
        return self.database.execute(sql,data)

    def select_row(self,table_name,asn = None, customer_as = None ,provider_as = None):
        #Finds the row that contains the customer-provider
        #relationship between two ASes
        if(asn):
            sql = "SELECT * FROM " + table_name + " WHERE asn = (%s);"
            data = (asn,)
        else:
            sql = "SELECT * FROM " + table_name + " WHERE customer_as = (%s) AND provider_as = (%s);"
            data = (customer_as,provider_as)
        record = self.database.execute(sql,data)
        return record

    def exists_row(self,table_name,asn = None, customer_as = None, provider_as = None):
        record = self.select_row(table_name,asn,customer_as, provider_as)
        if(record):
            exists = True
        else:
            exists = False
        return exists

    def count_entries(self,table_name):
        """Counts number of entries in desired table.

        Args: 
            table_name (:obj:`str`): name of table to count entries in.
        
        Returns:
            (:obj:`int`): number of entries found in a given table.

        """
        sql = "SELECT COUNT(*) FROM " + table_name + ";"
        numLines = self.database.execute(sql)

        print("\tCounting Entries...")

        #Database class calls fetchall() which returns count in strange format
        numLines = str(numLines[0])
        p = re.compile(r'Record\(count=(\d+)\)') 
        m = p.match(numLines)
        numLines = int(m.group(1))
        print("\t\t" + str(numLines) + " Entries")
        return numLines

    def insert_to_as_announcements(self,asn, sql_anns):
        if(exists_row(table_name = 'as_announcements',asn = asn)):
            sql = ("""UPDATE as_announcements SET announcements = announcements||((%s)::announcement[])
                    WHERE asn = (%s);""")
            data = (sql_anns,asn)
        else:
            sql = """INSERT INTO as_announcements VALUES ((%s),(%s)::announcement[]);"""
            data = (asn,sql_anns)
        self.database.execute(sql,data)
        return

    def insert_to_test_as_announcements(self,asn, sql_anns_arg):
        sql = """INSERT INTO test_as_announcements VALUES ((%s),(%s)::announcement[]);"""
        data = (asn,sql_anns_arg)
        self.database.execute(sql,data)
        return

    def insert_as_graph_into_db(self,graph,graph_table_name = None):
        #TODO implement graph_id programatically, date or something
        #TODO make test and regular tables follow same schema to clean up code

        progress = progress_bar(len(graph.ases))
        if(graph_table_name):    
            sql = "INSERT INTO " + graph_table_name + " VALUES (DEFAULT,(%s),(%s),(%s),(%s),(%s),(%s),(%s));"
            for asn in graph.ases:
                AS = graph.ases[asn]
                if(AS.SCC_id == asn):
                    members = graph.strongly_connected_components[asn]
                    data = (asn, AS.customers, AS.peers, AS.providers,members,0,AS.rank)
                    self.database.execute(sql,data)
                progress.update()
        else:
            sql = """INSERT INTO as_graph VALUES ((%s),(%s),(%s),(%s),(%s),(%s));"""
            for asn in graph.ases:
                AS = graph.ases[asn]
                if(AS.SCC_id == asn):
                    members = graph.strongly_connected_components[asn]
                    data = (asn, AS.customers,AS.peers,AS.providers,members,AS.rank)
                    self.database.execute(sql,data)
                progress.update()
        progress.finish()
        return

