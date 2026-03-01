import os
from helper import HandleError, ColorText, bcolors, GetOutputFolder, GetInputFolder
import polars as pl
import shutil
import subprocess
import sys
import lex.lexql as lexql
from typing import Callable

def CleanMaterialization(skel: 'QuerySkeleton', val):
        if not os.path.exists(skel.mat_paths[0]): return
        shutil.rmtree(skel.mat_paths[0])
    
def SetupBank(skel : 'QuerySkeleton', val : list[str]):
    os.makedirs('knowledge_bank', exist_ok=True)
    try:
        for b in val:
            banked_cte = lexql.CTENode(b, 'select 1', skel)
            banked_cte.df = pl.read_parquet(f'knowledge_bank/{b}.parquet', try_parse_dates=True)
            print(ColorText(f'    Loaded {b} from knowledge bank', bcolors.OKGREEN), flush=True)
            banked_cte.is_materialized = True
            skel.banked_ctes[b] = banked_cte
    except Exception as ex:
        HandleError('Invalid Bank')


def StageRepeater(skel : 'QuerySkeleton', repeater_params : dict):

    name_frmt : str
    name_frmt = 'part_{idx}'
    name_func = lambda args: name_frmt.format(**args)

    def StageSteps(steps : list):
        nonlocal name_func
        for idx, step in zip(range(len(steps)), steps):
            
            if 'idx' not in step:
                step['idx'] = idx + 1
            
            step_args = step.copy()

            for k in step:
                if isinstance(step[k], str):
                    step[k] = f"'{step[k]}'"
                elif isinstance(step[k], list):
                    step[k] = ','.join([str(x) for x in step[k]])
            skel.steps.append(lexql.QueryStruct(skel,skel.quer.format(**step),name_func(step_args)))

    def SetNameScheme(scheme : str):
        nonlocal name_frmt
        name_frmt = scheme

    def StageStrategy(strat : str):

        if strat == 'wide':
            pass

    repeater_logic_lookup = {
        'name_scheme' : (SetNameScheme,1),
        'steps' : (StageSteps,2),
        'strategy' : (StageStrategy,3)
    }

    if 'steps' not in repeater_params: HandleError('repeater_params need steps')

    execution_order = sorted([k for k in repeater_logic_lookup if k in repeater_params.keys()], key = lambda x: repeater_logic_lookup[x][1])
    
    for ex in execution_order:
        repeater_logic_lookup[ex][0](repeater_params[ex])


def StageKnowledge(skel : 'QuerySkeleton', bank : list[str]):
    if len(skel.steps) > 1 : print('NOTICE: banking only happens after all steps are materialized', flush=True)
    for k in bank:
        def post(k=k):
            kn = pl.concat(skel.recomp_dfs[k])
            kn.write_parquet(f'knowledge_bank/{k}.parquet')
            print(f'\t{k} has been added to knowledge bank')
        skel.post_funcs.append(post)
    print(f'Staged knowlege bank {bank}')

def StagePost(skel : 'QuerySkeleton', post_ex : dict):
    file = post_ex['file']
    args = post_ex['args']
    def post():
        print('Starting subprocess', flush=True)
        os.system(f'cp ./{GetInputFolder(skel.name)}/{file} ./{GetOutputFolder(skel.name, True)}/{file}')
        process = subprocess.Popen(
            [sys.executable, f'{GetOutputFolder(skel.name, True)}/{file}'] + args,
            stdout=subprocess.PIPE,
            text=True,
            bufsize=1
        )

        if process.stdout:
            for line in process.stdout:
                print('> ' + line.strip(), flush=True)

        process.wait()
        print('Finished subprocess', flush=True)
    skel.post_funcs.append(post)


extensionLookup = {
    'clean' : (CleanMaterialization,1),
    'banked' : (SetupBank,3),
    'repeater_params' : (StageRepeater,4),
    'knowledge_bank' : (StageKnowledge,5),
    'post_exec' : (StagePost,6)
}

def SetupExtensions(skel : 'QuerySkeleton', conf : dict):
    chain = sorted([(extensionLookup[k][0], conf[k], k, extensionLookup[k][1]) for k in conf if k in extensionLookup], key = lambda x: x[2])
    for func, arg, k, _ in chain:
        print(f'Running extension: {k}', flush=True)
        func(skel, arg)