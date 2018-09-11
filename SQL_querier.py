import psycopg2
import re
from lib_bgp_data import Database

#imports should be modified with file rearrangement

class SQL_querier:
    def __init__(self):
        self.database = Database()

    def close(self,cur,conn):
        """Cleans up database connection

        Args:
            cur(:obj:`psycopg2 cursor`): cursor that points to specific database
                elements.
            conn(:obj:`psycopg2 conn`): conn that holds connection to database.

        """
        cur.close()
        conn.close()
        return

    def select_relationships(self,num_entries = None):
        """Points cursor to relationships table
            
        Args:
            cur(:obj:`psycopg2 cursor`): cursor that points to specific database
                elements.
            num_entries(:obj:`int`,optional): if only a certain number of entries
                will be used, only that many will be selected.

        Todo:
            Modify select_ functions to accept variable table name. Variable
                substitution for table names currently not accepted by psycopg2.

        Returns:
            (:obj:`psycopg2 cursor`): cursor that points to specific database
                elements.
        """ 
        #Select All Relationship Records
        if(num_entries is None):
            print("\tSelecting All Relationship Records...")
            sql = """ SELECT * FROM as_relationships;"""
            data = None
        else:
            print("\tSelecting " + str(num_entries) + " Relationship Records...")
            sql = """SELECT * FROM as_relationships LIMIT (%s)"""
            data = (num_entries,)
        
        records = self.database.execute(sql,data)
        return records

    def select_announcements(self,num_entries = None):
        """Points cursor to announcements (elements) table
            
        Args:
            cur(:obj:`psycopg2 cursor`): cursor that points to specific database
                elements.

        Todo:
            Modify select_ functions to accept any table name. Variable
                substitution for table names currently not accepted by psycopg2.

        """
        print("\tSelecting Announcement Records...")
        #Select All Announcement Records
        self.database.execute("""
                SELECT * FROM elements;
                """)
        return cur

    def count_entries(self,table):
        """Counts number of entries in desired table.

        Args: 
            cur(:obj:`psycopg2 cursor`): cursor that points to specific database 
                elements.
        
        Todo:
            Modify function to accept any table name. Variable substitution
                for table names currently not accepted by psycopg2.

        Returns:
            (:obj:`int`): number of entries found in a given table

        """
        print("\tCounting Entries...")
        if(table == 'relationships'):
            numLines = self.database.execute("SELECT COUNT(*) FROM as_relationships;")
        if(table == 'announcements'):
            numLines = self.database.execute("SELECT COUNT(*) FROM elements;")
     #  numLines = cur.fetchone()[0]
        numLines = str(numLines[0])
        p = re.compile(r'\((\d+),\)') 
        m = p.match(numLines)
        numLines = int(m.group(1))
        print("\t\t" + str(numLines) + " Entries")
        return numLines
