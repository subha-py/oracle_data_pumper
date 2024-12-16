from oracledb.exceptions import DatabaseError
def enable_bct(connection):
    print('enabling block change tracker')
    with connection.cursor() as cursor:
        try:
            cursor.execute(
                "ALTER DATABASE ENABLE BLOCK CHANGE TRACKING")
        except DatabaseError as e:
            if 'block change tracking is already enabled' in str(e):
                pass
    print('Block change tracker is enabled')
    return