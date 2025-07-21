import os
from oracledb.exceptions import DatabaseError
import random
from string import ascii_letters
from utils.memory import human_read_to_byte, bytes_to_human_read
class Tablespace:
    def __init__(self, db, table, data_filesize='2G',name=None, autoextend=False, random_size=True):
        self.name = name
        self.data_filesize = data_filesize
        self.autoextend = autoextend
        self.random_size = random_size
        self.db = db
        self.table = table
        self.datafiles = []
        if not self.is_created():
            self.create()
        self.datafile_basename = self.get_datafile_basename()

    def get_datafile_basename(self):
        if self.name is None:
            return None
        return os.path.join(self.db.get_datafile_dir(), self.name)
    def get_name(self):
        try:
            query = f"SELECT tablespace_name FROM user_tables WHERE table_name='{self.table.name}'"
            return self.db.run_query(query)[0][0]
        except IndexError:
            return None
    def is_created(self):
        self.name = self.get_name()
        if self.name is not None:
            self.datafiles = self.get_datafiles()
        return self.name is not None
    def get_datafiles(self):
        query = f"SELECT file_name FROM dba_data_files WHERE tablespace_name='{self.name}'"
        result = self.db.run_query(query)
        datafiles = []
        for datafile in result:
            datafiles.append(datafile[0])
        return datafiles

    def create_random_datafile_name(self):
        random_string = ''.join(random.choices(ascii_letters, k=10))
        datafile_name = f'{self.datafile_basename}_{random_string}.dbf'
        self.datafiles.append(datafile_name)
        return datafile_name

    def get_new_size(self):
        if self.random_size:
            min_size = human_read_to_byte("50M")
            max_size = human_read_to_byte(self.data_filesize)
            random_bytes = random.randint(min_size, max_size)
            datafile_size = bytes_to_human_read(random_bytes)
            self.db.log.info(f'Picked random datafile_size = {datafile_size}')
        else:
            datafile_size = self.data_filesize
        return datafile_size
    def create(self):
        self.name = f"{self.table.name}ts"
        self.datafile_basename = self.get_datafile_basename()
        if self.autoextend:
            # todo create big tablespace when autotextend is true
            cmd = (f"""create tablespace {self.name} \
                    datafile '{self.create_random_datafile_name()}' size {self.data_filesize} AUTOEXTEND 
                    ON NEXT {self.data_filesize} EXTENT MANAGEMENT LOCAL SEGMENT SPACE MANAGEMENT AUTO""")
        else:
            cmd = (f"create tablespace {self.name} \
                datafile '{self.create_random_datafile_name()}' size {self.get_new_size()}")
        self.db.log.info(f'creating tablespace with name - {self.name}')
        self.db.run_query(cmd)
        self.db.log.info(f'tablespace created with name - {self.name}')

    def extend(self):
        new_size = self.get_new_size()
        self.db.log.info(f'Extending tablespace by {new_size}')
        cmd = f"""ALTER TABLESPACE {self.name} ADD DATAFILE 
                                        '{self.create_random_datafile_name()}' 
                                        SIZE {new_size}"""
        self.db.run_query(cmd)
        self.db.log.info('tablespace successfully increased')

    def delete(self):
        with self.db.connection.cursor() as cursor:
            try:
                cursor.execute(f"drop tablespace {self.name} \
                    INCLUDING CONTENTS AND DATAFILES")
            except DatabaseError as e:
                if "does not exist" in str(e):
                    return

    def __repr__(self):
        return self.name
