
import mysql.connector
def db_connect(port):
    config = {
        'user': 'root',
        'password': 'L0ck33D0wn',
        'host': '127.0.0.1',
        'port': port,
        'database': 'Footprints',
        'raise_on_warnings': True
    }
    cxn = mysql.connector.connect(**config)
    return cxn, cxn.cursor()

db1_cxn, db1 = db_connect(host='127.0.0.1', )
db2_cxn, db2 = db_connect(33062)
db1.execute("SELECT mrID,mrUpdate from MASTER66")
db2.execute("SELECT mrID,mrUpdate from MASTER66")
res1 = db1.fetchall()
set2 = db2.fetchall()
diff = set(res1) ^ set(res2)

pprint(diff)
