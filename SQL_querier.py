import psycopg2
import psycopg2.extras
import re
from datetime import date
from lib_bgp_data import Database
from progress_bar import progress_bar
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

    #TODO rename this function, it's mainly for customer_provider relationships
    def exists_row(self,table_name,asn = None, customer_as = None, provider_as = None):
        record = self.select_row(table_name,asn,customer_as, provider_as)
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
        numLines = self.database.execute(sql)

        print("\nCounting \"" + table_name + "\" Entries...")

        #Database class calls fetchall() which returns count in strange format
        numLines = self.fetch_all_to_int(numLines)
        print("\t" + str(numLines) + " Entries")
        return numLines

    def fetch_all_to_int(self,fetch_all):
        numLines = str(fetch_all[0])
        p = re.compile(r'Record\(count=(\d+)\)') 
        m = p.match(numLines)
        numLines = int(m.group(1))        
        return numLines

    def insert_results(self,asn, sql_anns):
        if(self.exists_row(table_name = self.results_table_name,asn = asn)):
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

