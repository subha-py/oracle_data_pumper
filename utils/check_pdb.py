from oracledb.exceptions import DatabaseError
def check_pdb_status(connection, pdb_name):
    print(f'Checking open mode status for PDB: {pdb_name}...')

    with connection.cursor() as cursor:
        try:
            query = "SELECT name, open_mode FROM v$pdbs WHERE name = :pdb_name"
            cursor.execute(query, pdb_name=pdb_name)
            results = cursor.fetchall()

            if not results:
                print("No PDBs found.")
                return "NOT RW"

            pdb_name, open_mode = results[0]
            print(f"PDB: {pdb_name}, Open Mode: {open_mode}")
            return "RW" if open_mode == "READ WRITE" else "NOT RW"

        except DatabaseError as e:
            print(f"Error checking PDB status: {e}")
            return "NOT RW"