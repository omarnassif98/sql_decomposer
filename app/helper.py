import os
import json
import traceback

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
    print(ColorText('SQL Decomposer v3.2', bcolors.BOLD), flush=True)
    if not os.path.exists('./config/settings.json'):
        os.makedirs('./config', exist_ok=True)
        data = ''
        with open('settings.json', 'r') as f:
            data = f.read()
        with open('./config/settings.json', 'w') as f:
            f.write(data)
        HandleError('Config missing, populating with template data, ensure mounted volume points to /app/config')

def HandleError(ex : str, indents = 0):
    LogQuery(traceback.format_exc(), './logs', 'error_log')
    print(ColorText(ex, bcolors.FAIL), flush=True)
    exit()

def ColorText(msg, dec : bcolors):
    return f'{dec}{msg}{bcolors.ENDC}'

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