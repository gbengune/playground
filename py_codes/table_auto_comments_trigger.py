#/*
#   Created on:  03-08-2023
#   Created by:  O.Aremu
#   Description: Captures all tables in a given PostgreSQL DB \
#                with and without comments and automatically creates comments template  in all
#


#/*


from sqlalchemy import create_engine, inspect,text
#from sqlalchemy import text
from datetime import datetime
import yaml



with open("config_file.yml") as conf:
    config = yaml.safe_load(conf)
try:
    # specify the database connection details
    db_name = ""
    db_user = ""
    db_password = config[""]
    db_host = ""
    db_port = ''

    # create an engine to connect to the database
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
except Exception as err1:
    print('Error encountered was:',err1)

    # specify the db_schema and db_table and the format for the auto comments to be added

schema = "roh"

#Specify the structure of the comments
comment = f'''
        description: _?_,
        Title: _?_,
        Table_owner_external: _?_,
        Table_owner_internal: _?_,
        Table_contact: _?_,
        Table_delivery_date: _?_,
        Table Validity: _?_,
        Table Source: _?_,
        Table Creator: _?_,
        Table_date_default: {datetime.now().replace(microsecond=0)}
        '''

# Define variable checker to get information about the table
checker = inspect(engine)

# Collate a list of all table names in the specified schema
table_names = checker.get_table_names(schema=schema)
print('Table(s) found were','\n',table_names)

# loop through tables
for tabs in table_names:

    # concat table name and schema to string pairs
    schema_table_name = ".".join([schema, tabs])

    # check if the table already has a comment
    if checker.get_table_comment(tabs, schema=schema)["text"] is not None:
        print('Table already has a comment.')
    else:

        # add a comment to the table.
        #With automatically handles exceptions
        with engine.connect() as conn:
            conn.execute(text(f"COMMENT ON TABLE {schema_table_name} IS '{comment}';"))
            conn.commit()
        print('Comment added to table.')
print("Done.")
