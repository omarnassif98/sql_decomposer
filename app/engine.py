    # %%
from helper import HandleError, InitEnvironment, RetrieveSetting
from lex.lexql import QuerySkeleton, InitEngine
from drivers.driver import Driver, GetDriver
import os
import sqlalchemy as sa
from sqlalchemy import exc

InitEnvironment()
InitEngine()
db_config = RetrieveSetting('db_connection')

db_url = \
f'{db_config["engine"]}://{db_config["username"]}:{db_config["password"]}@host.docker.internal:60001/{db_config["database"]}'
sql_engine =  sa.create_engine(db_url)

def DecomposeQuery(filename : str):
    quer_name = filename[:-4]
    print('Decomposing ' + quer_name, flush=True)
    with open(f'./input/{filename}', 'r') as f:
        quer = f.read()
        try:
            with sql_engine.connect() as conn:
                skel = QuerySkeleton(quer, quer_name, conn)
                skel.Execute()
        except exc.OperationalError as ex:
            HandleError('Operational Error')

mode = RetrieveSetting('INTERFACE_MODE')
print(f'Mounting driver: {mode.upper()}', flush=True)
driver = GetDriver(mode.lower())
driver.run(decomp_func=DecomposeQuery)