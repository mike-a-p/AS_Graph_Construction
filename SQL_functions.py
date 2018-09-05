import psycopg2

def connect(credentials = None):
    """connects to database containing relevent tables

    Args:
        credentials(:obj:`tuple` of :obj:`string`, optional):A tuple containing
        data relevant to connecting to a database. tuple should come in form:
        (username, password, database, host).

    Returns:
        (:obj:`psycopg2 conn`):
            A connection via psycopg2 to the database described in credentials

    """
    #if no credentials provided use default
    if(credentials == None):
        username = "bgp_user"
        password = "Mta8Avm55zUCYtnDz"
        database = "bgp"
        host = "localhost"
        credentials = (username,password,database,host)

    print("\tConnecting to DataBase...")
    conn = psycopg2.connect(host=credentials[3],
                            database=credentials[2],
                            user=credentials[0],
                            password=credentials[1])
    return conn

def close(cur,conn):
    """Cleans up database connection

    Args:
        cur(:obj:`psycopg2 cursor`): cursor that points to specific database
            elements.
        conn(:obj:`psycopg2 conn`): conn that holds connection to database.

    """
    cur.close()
    conn.close()
    return

def select_relationships(cur, num_entries = None):
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
        cur.execute("""
                SELECT * FROM as_relationships;
                """)
    else:
        print("\tSelecting " + str(num_entries) + " Relationship Records...")
        cur.execute("""
                SELECT * FROM as_relationships LIMIT (%s)
                """,(num_entries,))
    return cur

def select_announcements(cur):
    """Points cursor to announcements (elements) table
        
    Args:
        cur(:obj:`psycopg2 cursor`): cursor that points to specific database
            elements.

    Todo:
        Modify select_ functions to accept any table name. Variable
            substitution for table names currently not accepted by psycopg2.

    Returns:
        (:obj:`psycopg2 cursor`): cursor that points to specific database
            elements.
    """
    print("\tSelecting Announcement Records...")
    #Select All Announcement Records
    cur.execute("""
            SELECT * FROM elements;
            """)
    return cur

def count_entries(cur, table):
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
        cur.execute("SELECT COUNT(*) FROM as_relationships;")
    if(table == 'announcements'):
        cur.execute("SELECT COUNT(*) FROM elements;")
    numLines = cur.fetchone()[0]
    print("\t\t" + str(numLines) + " Entries")
    return numLines
