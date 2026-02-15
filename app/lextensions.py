import os
from helper import HandleError
import polars as pl
import shutil
import subprocess
import sys
import lexql

def ApplyVariant(skel:'QuerySkeleton', val : str):
    skel.name += f'_{val}'

def CleanMaterialization(skel: 'QuerySkeleton', val):
        shutil.rmtree(f'./output/{skel.name}')
        print(f'Cleaned {skel.name} material')
    
def SetupBank(skel : 'QuerySkeleton', val : list[str]):
    os.makedirs('knowledge_bank', exist_ok=True)
    try:
        for b in val:
            banked_cte = CTENode(b, 'select 1', skel)
            banked_cte.df = pl.read_csv(f'knowledge_bank/{b}.csv', try_parse_dates=True)
            print(f'\tLoaded {b} from knowlede bank', flush=True)
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
        seeded_outputs = [f'./{'output' if MATERIAL_PERMANENCE else 'eph_materializations'}/{cte.parent.skeleton.name}/{'' if cte.parent.name == '' else f'{cte.parent.name}/'}materializations' for cte in ctes]
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

EXTENDABLE_LOOKUP = {
    'variant' : ApplyVariant,
    'clean' : CleanMaterialization,
    'banked' : SetupBank,
    'repeater_params' : StageRepeater,
    'knowledge_bank' : StageKnowledge,
    'post_exec' : StagePost
}