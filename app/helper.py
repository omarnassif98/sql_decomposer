import os
import json
import traceback

def InitEnvironment():
    print('\033[1mSQL Decomposer v3.1\033[0m', flush=True)
    if not os.path.exists('./config/settings.json'):
        os.makedirs('./config', exist_ok=True)
        data = ''
        with open('settings.json', 'r') as f:
            data = f.read()
        with open('./config/settings.json', 'w') as f:
            f.write(data)
        HandleError('Config missing, populating with template data, ensure mounted volume points to /app/config')

def HandleError(ex : str, indents = 0):
    prefix = '  ' * indents
    traceback.print_exc()
    print(f'{prefix}\033[31m{ex}\033[0m', flush=True)
    exit()

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