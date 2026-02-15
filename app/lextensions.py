import os
from helper import HandleError, ColorText, bcolors
import polars as pl
import shutil
import subprocess
import sys
import lexql

def ApplyVariant(skel: 'QuerySkeleton', val : str):
    skel.name += f'_{val}'

def CleanMaterialization(skel: 'QuerySkeleton', val):
        shutil.rmtree(f'./output/{skel.name}')
    
def SetupBank(skel : 'QuerySkeleton', val : list[str]):
    os.makedirs('knowledge_bank', exist_ok=True)
    try:
        for b in val:
            banked_cte = lexql.CTENode(b, 'select 1', skel)
            banked_cte.df = pl.read_csv(f'knowledge_bank/{b}.csv', try_parse_dates=True)
            print(ColorText(f'    Loaded {b} from knowlede bank', bcolors.OKGREEN), flush=True)
            banked_cte.is_materialized = True
            skel.banked_ctes[b] = banked_cte
    except Exception as ex:
        HandleError('Invalid Bank')


def StageRepeater(skel : 'QuerySkeleton', repeater_params : dict):
    steps = repeater_params['steps']
    for idx, step in zip(range(len(steps)), steps):
        for k in step:
            if isinstance(step[k], str):
                step[k] = f"'{step[k]}'"
            elif isinstance(step[k], list):
                step[k] = ','.join([str(x) for x in step[k]])
        skel.steps.append(lexql.QueryStruct(skel,skel.quer.format(**step),f'part_{idx+1}'))

    if 'anchored_ctes' not in repeater_params: return
    anchors = repeater_params['anchored_ctes']
    for cte_name in anchors:
        ctes = [struct.cte_lookup[cte_name] for struct in skel.steps]
        seeded_outputs = [f'./{'output' if lexql.MATERIAL_PERMANENCE else 'eph_materializations'}/{cte.parent.skeleton.name}/{'' if cte.parent.name == '' else f'{cte.parent.name}/'}materializations' for cte in ctes]
        ctes[0].mat_paths = seeded_outputs
        def callback():
            for cte in ctes[1:]:
                cte.is_materialized = True
                print(f'Anchored CTE - {cte.parent.name}.{cte.name}- {cte.is_materialized}')
        ctes[0].callbacks.append(callback)


def StageKnowledge(skel : 'QuerySkeleton', bank : list[str]):
    if len(skel.steps) > 1 : print('NOTICE: banking only happens after all steps are materialized', flush=True)
    for k in bank:
        def post():
            dfs = []
            for st in skel.steps:
                dfs.append(st.cte_lookup[k].df)
            kn = pl.concat(dfs)
            kn.write_csv(f'knowledge_bank/{k}.csv')
            print(f'\t{k} has been added to knowledge bank')
        skel.post_funcs.append(post)
    print('Staged knowlege bank')

def StagePost(skel : 'QuerySkeleton', post_ex : dict):
    file = post_ex['file']
    args = post_ex['args']
    def post():
        print('Starting subprocess', flush=True)
        os.system(f'cp ./input/{file} ./output/{skel.name}/{file}')
        process = subprocess.Popen(
            [sys.executable, f'./output/{skel.name}/{file}'] + args,
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
    'variant' : (ApplyVariant, 2),
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