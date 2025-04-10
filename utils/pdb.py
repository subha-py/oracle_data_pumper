import sys
from oracledb.exceptions import DatabaseError
def check_pdb_status(connection, pdb_name):
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT open_mode FROM v$pdbs WHERE name = :pdb_name", pdb_name=pdb_name)
            result = cursor.fetchone()
            sys.exit(0 if result and result[0] == "READ WRITE" else 1)
    except DatabaseError:
        sys.exit(1)

