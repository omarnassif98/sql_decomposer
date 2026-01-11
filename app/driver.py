# %%
from helper import HandleError, InitEnvironment, RetrieveSetting
from lexql import QuerySkeleton, InitEngine
import os
import sqlalchemy as sa
from sqlalchemy import exc

InitEnvironment()
InitEngine()

db_config = RetrieveSetting('db_connection')

db_url = \
f'{db_config["engine"]}://{db_config["username"]}:{db_config["password"]}@host.docker.internal:60001/{db_config["database"]}'


sql_engine =  sa.create_engine(db_url)

for file in os.listdir('./input'):
    if not file.endswith('.sql'): continue
    quer_name = file[:-4]
    print(quer_name)
    with open(f'./input/{file}', 'r') as f:
        quer = f.read()
        try:
            with sql_engine.connect() as conn:
                skel = QuerySkeleton(quer, quer_name, conn)
                skel.Execute()
        except exc.OperationalError as ex:
            HandleError(str(ex).splitlines()[0], 1)
