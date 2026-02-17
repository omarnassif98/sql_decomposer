import os
import json
import traceback
import shutil

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


def InitEnvironment():
    print(ColorText('SQL Decomposer v3.2.1', [bcolors.BOLD]), flush=True)
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
        print('Setup sandbox for ' +  ColorText(f, bcolors.HEADER))