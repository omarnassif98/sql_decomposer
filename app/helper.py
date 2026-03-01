import os
import json
import traceback
import shutil
import pathlib
from polars import read_csv, read_parquet, DataFrame
from typing import Callable
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    ITALICS = '\033[3m'



def InitEnvironment():
    print(ColorText('SQL Decomposer v3.3', [bcolors.BOLD]), flush=True)
    with open('settings.json', 'r') as f:
        default_data = json.load(f)

    if not os.path.exists('./config/settings.json'):
        os.makedirs('./config', exist_ok=True)
        default_data = ''
        with open('./config/settings.json', 'w') as f:
            json.dump(default_data, f, indent=3)
        HandleError('Config missing, populating with template data, ensure mounted volume points to /app/config')

    with open('./config/settings.json', 'r') as s:
            user_data = json.load(s)
    validity_flag = True
    for k1 in default_data:
        if k1 not in user_data:
            validity_flag = False
            user_data[k1] = default_data[k1]
    
    if validity_flag:return
    with open('./config/settings.json', 'w') as f:
            json.dump(user_data, f, indent=3)
    print(ColorText('New settings written to config/settings.json', bcolors.HEADER))
    

def HandleError(ex : str):
    LogQuery(traceback.format_exc(), './logs', 'error_log')
    print(ColorText(ex, bcolors.FAIL), flush=True)
    exit()

def ColorText(msg, dec : str | list):
    if isinstance(dec, str): return f'{dec}{msg}{bcolors.ENDC}'
    elif isinstance(dec, list):
        popped = f'{dec.pop()}{msg}{bcolors.ENDC}'
        if len(dec) == 0: return popped
        else: return ColorText(popped, dec)

def LogQuery(quer : str, path : str, title : str):
    os.makedirs(path, exist_ok=True)
    with open(f'{path}/{title}.sql', 'w') as f:
        f.write(quer)

def RetrieveSetting(key : str | list[str]) -> str | tuple[str]:
    try:
        with open('./config/settings.json', 'r') as f:
            data = json.load(f)
        if isinstance(key, str):
            return data[key]
        return (data[k] for k in key)
    except Exception as ex:
        HandleError(f'Setting not found make sure {key if isinstance(key,str) else ', '.join(key)} is in settings file')

def MigrateSandbox():
    if RetrieveSetting('LEGACY_FILE_SYSTEM'): return
    os.makedirs('sandbox', exist_ok=True)
    for f in os.listdir('input'):
        if not f.endswith('.sql'): continue
        os.makedirs(f'sandbox/{f.removesuffix('.sql')}/materializations', exist_ok=True)
        shutil.copy(f'input/{f}',f'sandbox/{f.removesuffix('.sql')}')
        print(ColorText('At this point it is safe to delete the input content for ' +  f, bcolors.OKGREEN))
        if not pathlib.Path(f'output/{f.removesuffix('.sql')}').exists(): continue
        shutil.copytree(f'output/{f.removesuffix('.sql')}', f'sandbox/{f.removesuffix('.sql')}/materializations', dirs_exist_ok=True)
        print('Setup sandbox for ' +  ColorText(f, bcolors.HEADER))
        print(ColorText('At this point it is safe to delete the output folder ' +  f, bcolors.OKGREEN))


def GetOutputFolder(proj, root = False):
    LEGACY_FILE_SYSTEM = RetrieveSetting('LEGACY_FILE_SYSTEM')
    return  f'./output/{proj}' if LEGACY_FILE_SYSTEM else f'./sandbox/{proj}{'' if root else '/materializations'}'

def GetInputFolder(proj):
    LEGACY_FILE_SYSTEM = RetrieveSetting('LEGACY_FILE_SYSTEM')
    return  f'./input/{proj}' if LEGACY_FILE_SYSTEM else f'./sandbox/{proj}'

def ResolveDecompMethods():
    global decomp_read_comp, decomp_write_comp


    

def DecompReadFunction(nam : str):
    DECOMP_FORMAT = RetrieveSetting('DECOMP_FORMAT')
    decomp_read_registry = {
        'csv' : lambda path : (read_csv, {
            "source": f"{path}.csv",
            "try_parse_dates":True
        }),
        'parquet' : lambda path : (read_parquet, {
            "source": f"{path}.parquet"
        })
    }
    func, args = decomp_read_registry[DECOMP_FORMAT](nam)
    return func(**args)

def DecompWriteFunction(df, nam):
    DECOMP_FORMAT = RetrieveSetting('DECOMP_FORMAT')
    decomp_write_registry = {
        'csv' : lambda df, path : (df.write_csv, {
            "file": f"{path}.csv",
            "float_scientific":False
        }),
        'parquet' : lambda df, path : (df.write_parquet, {
            "file": f"{path}.parquet"
        })
    }
    func, args = decomp_write_registry[DECOMP_FORMAT](df, nam)
    return func(**args)