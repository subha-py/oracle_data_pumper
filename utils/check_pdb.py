from oracledb.exceptions import DatabaseError
def check_pdb_status(connection, pdb_name):
    print(f'Checking open mode status for PDB: {pdb_name}...')

    with connection.cursor() as cursor:
        try:
            query = "SELECT name, open_mode FROM v$pdbs WHERE name = :pdb_name"
            cursor.execute(query, pdb_name=pdb_name)
            results = cursor.fetchall()

            if not results:
                print(f"No PDB found with name: {pdb_name}")
                return "NOT READ WRITE"

            pdb_name, open_mode = results[0]
            print(f"PDB: {pdb_name}, Open Mode: {open_mode}")

            # More detailed status check
            if open_mode == "READ WRITE":
                return "IS READ WRITE"
            elif open_mode == "MOUNTED":
                print("PDB is mounted but not opened")
                return "NOT READ WRITE"
            elif open_mode == "READ ONLY":
                print("PDB is in read-only mode")
                return "NOT READ WRITE"
            else:
                print(f"PDB is in unexpected state: {open_mode}")
                return "NOT READ WRITE"

        except DatabaseError as e:
            print(f"Error checking PDB status: {e}")
            return "NOT READ WRITE"