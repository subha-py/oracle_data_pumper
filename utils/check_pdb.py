from oracledb.exceptions import DatabaseError
def check_pdb_status(connection):
    print('Checking PDB open mode status...')
    with connection.cursor() as cursor:
        try:
            query = "SELECT name, open_mode FROM v$pdbs"
            cursor.execute(query)

            results = cursor.fetchall()

            if not results:
                print("No PDBs found.")
                return False

            for row in results:
                pdb_name, open_mode = row
                print(f"PDB: {pdb_name}, Open Mode: {open_mode}")

            return True
        except DatabaseError as e:
            print(f"Error checking PDB status: {e}")
            return False