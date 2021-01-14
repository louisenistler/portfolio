#!/usr/bin/python3
# coding: utf-8

from sys import argv

# Catch errors from importing two modules that users might not have installed.
try:
    import pandas as pd
except ModuleNotFoundError:
    print("Could not import pandas module. Please install pandas:\nhttps://pandas.pydata.org/pandas-docs/stable/getting_started/install.html#installing-from-pypi")

try:
    from simply import redshift
except ModuleNotFoundError:
    print("Could not import from simply. Please install:\nhttps://github.banksimple.com/data/simply#setting-up-environment")

# Make sure users are supplying enough arguments. 
if argv[1] == '-h' or argv[1] == '--help' or len(argv) < 3:
    # print("""\nUsage:\n\n> python csv_to_redshift.py {PATH_TO_FILE} {REDSHIFT_TABLE_NAME}\n\nPlease provide all required arguments.\n""")
    print("""⚠️ Usage:

$> python csv_to_redshift.py {PATH_TO_FILE} {REDSHIFT_TABLE_NAME}

Please provide all required arguments.
        """)
else:
    TABLE_NAME = argv[2]

    # Make support multiple filetypes (previously only used pd.read_csv)
    if argv[1].endswith('.csv'):
        INPUT_CSV = pd.read_csv(argv[1])
    elif argv[1].endswith('.xlsx'):
        INPUT_CSV = pd.read_excel(argv[1])
    else:
        print("Unrecognized file format! Please use .csv or .xlsx and retry.")


    # ## Create column definitions and datatypes for DDL
    def create_column_definitions(INPUT_CSV):
        column_definitions = """"""

        dtype_mapping = {'object':  'varchar(65535)',
                         'float64': 'float',
                         'int64':   'bigint',
                         'bool':    'boolean'}
        
        for col in INPUT_CSV.columns:
            column_definitions = column_definitions + f"""{col}\t{dtype_mapping[str(INPUT_CSV.dtypes[col])]},\n"""
            trimmed_column_definitions = column_definitions[:-2]
            
        return trimmed_column_definitions


    # ## Format data as a stringed tuple 
    def format_for_redshift(data):
        # Adapted from https://github.banksimple.com/data/utils/blob/main/iterable/export_users_to_redshift.py#L84-L91

        string_data = str([tuple(x) for x in INPUT_CSV.fillna('NULL').values])[1:-1].replace("'NULL'", 'NULL').replace('"NULL"', 'NULL').replace('/', '').replace("\\", "").replace(":", "\:")
        return string_data

    # Catches exceptions which may result from unsupported filetypes. 
    try:
        column_string = create_column_definitions(INPUT_CSV)

        insert_string = format_for_redshift(INPUT_CSV)

        base_ddl_string = f"""
        drop table if exists public.{TABLE_NAME};
        create table public.{TABLE_NAME} (
            {column_string}
        )
        ;

        insert into public.{TABLE_NAME} values
            {insert_string}
        ; 

        grant select on public.{TABLE_NAME} to 
            group analyticsusers,
            group moderaterisk_pii
        ;

        select *
        from public.{TABLE_NAME}
        limit 1
        ;
        """
        
        print(redshift(base_ddl_string))

    except NameError:
        pass