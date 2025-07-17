from utils.connection import connect_to_oracle


set_recovery_file_dest_size = 'alter system set db_recovery_file_dest_size=2000G scope=both'
set_db_files = 'alter system set db_files=20000 scope=spfile'
oracle_precheck_queries = [set_db_files, set_recovery_file_dest_size]

def run_queries(host, db):
    try:
        connection = connect_to_oracle(host,db)
    except Exception as e:
        print(f'cannot connect to oracle instance : {host}:{db} due to - {e}')
        return
    with connection.cursor() as cursor:
        try:
            for query in oracle_precheck_queries:
                cursor.execute(query)
        except Exception as e:
            print(f'cannot set query - {query} in {host}:{db} got error - {e}')



def run_queries_from_hostmap(hostmap):
    """
     use return hostmap of method filter_host_map_by_listener_connectivity this will be available in logs
    eg - 2025-07-17 16:27:25,370 - INFO - Final hostmap on which data pumping will happen
    {
    '10.14.69.139': ['FIDB0', 'FIDB1', 'FIDB2', 'FIDB3', 'FIDB4', 'FIDB5', 'FIDB6', 'FIDB7', 'FIDB8', 'FIDB9', 'FIDB10'],
    '10.14.69.186': ['FIT1TB', 'SAMEDF', 'LARGEDF']
    }
    """
    for host,dbs in hostmap.items():
        for db in dbs:
            run_queries(host,db)
    return

if __name__ == '__main__':
    hostmap = {'10.14.69.139': ['FIDB0', 'FIDB1', 'FIDB2', 'FIDB3', 'FIDB4', 'FIDB5', 'FIDB6', 'FIDB7', 'FIDB8', 'FIDB9', 'FIDB10'], '10.14.69.186': ['FIT1TB', 'SAMEDF', 'LARGEDF'], '10.3.63.222': [], '10.3.63.230': ['FIDB0', 'FIDB1', 'FIDB2'], '10.14.70.149': ['BCTSTAT'], '10.3.63.220': [], '10.14.69.187': ['FIDB7', 'FIDB10', 'STM2R']}
    run_queries_from_hostmap(hostmap)