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
            HandleError(str(ex).splitlines()[0], 1)


mode = RetrieveSetting('INTERFACE_MODE')
print(f'running in {mode.upper()} mode')
match mode.lower():
    case 'sequential':
        for file in os.listdir('./input'):
            if not file.endswith('.sql'): continue
            DecomposeQuery(file)
    case 'cli':
        while True:
            try:
                q = input('Enter name of decompsed query (Ctrl + C => Enter to quit)\n\t>')
                if not q.endswith('.sql'): q += '.sql'
                DecomposeQuery(q.lower())
            except KeyboardInterrupt as ex:
                print('Quitting', flush=True)
                exit(0)