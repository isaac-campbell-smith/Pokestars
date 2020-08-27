import pandas as pd

def pretty_query(cur, query, conn):
    conn.rollback()
    cur.execute(query)
    data = cur.fetchall() 
    #cur.description stores SQL column information as a tuple
        #the name method contains SQL column labels
    headers = [head.name for head in cur.description]
    out = pd.DataFrame(data=data, columns=headers)
    
    if 'id' in headers:
        out.set_index('id', inplace=True)
        
    return out