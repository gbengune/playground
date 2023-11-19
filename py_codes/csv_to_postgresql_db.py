#This code is non productive
#There is a faster way but this one is just a sample
# =============================================================================

import glob
import re
import psycopg2 as pg_con
import time
import logging
from utils import pg_creds


class csv_worker:


    def __init__(self):
        try:
            
            
            self.conn = self.pg_con.connect(  host=pg_creds["pg_host"],
                                              user=pg_creds["pg_user"],
                                              port=pg_creds["pg_port"],
                                              password=pg_creds["pg_pass"],
                                              database=pg_creds["pg_sdb"]
                                  )
        
            self.cur = self.conn.cursor()
            print(f'COnnection Successfull')
            
        except Exception as errors:
            raise f'Error occured with connection: {errors}'
            
            
    def rd_csv_files_cr_db_table(self, db_schema_name, folder, file_format):
        try:

            st_time = time.time()



            path = str(folder) + '/*.' + str(file_format)

            # find all csv files in the given folder
            found = glob.glob(path, recursive=True)
            file = sorted(found)

            # read file data into a Dataframe
            # assume that column names are found in the second row
            # Loop through found files in given folder path
            for dok in file:

                with (open(dok, 'r', encoding='latin-1')) as data:

                    # get the names of files
                    list_conv = str(dok)
                    last_index = list_conv.split('/')[-1]
                    tb_name = last_index.split('.')[0]

                    # splitted till the format was stripped.
                    # Values derived after this point will be hard to fortell so no more splits.
                    print(f'Data converted was: {tb_name}')
                    print(f'------------------------------')
                    tb_names_list.append(tb_name)

                    # defining table columns
                    ln_split = data.readlines()
                    # here the values for columns were taken
                    col_ext = ln_split[0]

                    # specially carved-out for the insert statement below
                    col_ext_ins = col_ext.replace(';', ',')

                    col_ext_v1 = col_ext.replace('\n', '').split(';')
                    # print(col_ext_v1)

                    #define default global db column types with list annotation
                    col_ext_v2 = '  varchar,'.join('"' + f + '"' for f in col_ext_v1)
                    col_end = col_ext_v2 + ' ' + 'varchar'

                    # ------------------

                    # ------------------ #creating tables in database
                    self.cur.execute(sql2 % (db_schema_name,
                                             tb_name,
                                             # ---------------
                                             db_schema_name,
                                             tb_name,
                                             # --------
                                             col_end,
                                             # ---------
                                             db_schema_name,
                                             tb_name,
                                             )

                                     )
                    self.conn.commit()
                    print(f'Table {tb_name.upper()} created in {self.cred["pg_sdb"].upper()}')
                    print(f'------------------------------')

                    # ------------------ #PIPING data to table in the given database

                    print(f'Total Row_length {len(ln_split)}')

                    #---instanciating no row scenario
                    if len(ln_split) == 0:
                        pass

                    #instanciating 1 row scenario
                    # cuz only one dataset will be coming in as string characters
                    elif len(ln_split) == 1:

                        self.cur.execute(sql3 % (
                            db_schema_name,
                            tb_name,
                            col_ext_ins,
                            # -------------
                            tb_name,
                            re.sub('\[|\]', '', str(ln_split[1].split(';')))

                        )
                                         )
                        self.conn.commit()

                        print(f'All values (1) inserted in table: {tb_name.upper()}')

                        #instanciating all other row number scenarios
                    else:
                        # DANGER
                        # because datasets would be read as lists
                        # -------------------->> twitch range to adjust how many rows will be transfered into database table
                        test = ln_split[1:10]
                        #test = ln_split[1:]


                        print(f'To be inserted row length more than 1')
                        print(type(test))

                        for f in test:
                 

                            self.cur.execute(sql3 % (
                                db_schema_name,
                                tb_name,
                                col_ext_ins,
                                # -------------
                                # tb_name,
                                # gbenga comment the next line out and uncomment the previous after data piping for this specific csv files
                                tb_name.split('_')[0],
                                re.sub('\[|\]', '', str(str(f).replace('\n', '').split(';')))

                            )
                                             )

                            self.conn.commit()

              

                    # --------------------Only enable the scripts below if you need to update the data type of geom columns as well
                    #self.cur.execute(sql4 % (db_schema_name,
                     #                        tb_name,

                      #                       )
                       #              )

                    #self.conn.commit()
                    #print('Geom Column for table', tb_name, 'updated')



        except Exception as er1:
            print(f'Error: {er1}')
        finally:
            if self.conn is not None:
                self.conn.close()

        print(f'Tables created: {tb_names_list}')
        print(f'Time taken is: {time.time() - st_time}')


tb_names_list = []



sql2 = """   
drop table if exists  "%s"."%s";
create table "%s"."%s" (sn INT GENERATED ALWAYS AS IDENTITY,ags_sh varchar(50), %s);
alter table "%s"."%s" add primary key (sn)

"""

sql3 = """
insert into "%s"."%s"(ags_sh,%s)
values ('%s',%s)
on conflict do nothing

"""

sql4 = """
alter table "%s"."%s" alter column geom type geometry  using st_transform(geom :: geometry,4326)
s
"""

# format (db_Schemaname|absolutepathtofolder|formatofdatatobeconverted)
csv_worker().rd_csv_files_cr_db_table('public', '/home/###/###/##/#######/test', 'csv')




