from helper import HandleError, LogQuery, RetrieveSetting
from typing import Callable
import re
import polars as pl
from polars._typing import ConnectionOrCursor
from psycopg2 import errors
import json
import os
import subprocess
import sys
import shutil

AUTORUN : bool
MATERIAL_PERMANENCE : bool
BACKLINK_LOGGING : bool
INJECT_LOGGING : bool

def InitEngine():
    global AUTORUN, MATERIAL_PERMANENCE, BACKLINK_LOGGING, INJECT_LOGGING
    AUTORUN, MATERIAL_PERMANENCE, BACKLINK_LOGGING, INJECT_LOGGING = RetrieveSetting(['AUTORUN', 'MATERIAL_PERMANENCE', 'BACKLINK_LOGGING', 'INJECT_LOGGING'])



os.makedirs('./output', exist_ok=True)
os.makedirs('./input', exist_ok=True)

def GetQueryIdxs(quer):
    idxs = []
    chunk_idx = 0
    running_idx = 0
    recursiveness = 0
    quer_end_idx = len(quer) - 1
    while running_idx < quer_end_idx:
        cte_name = ''
        while recursiveness == 0:
            if running_idx == quer_end_idx: return idxs
            if quer[running_idx] == '(':
                start_pos = running_idx
                recursiveness += 1
                matches = re.findall(r'(\w+)\s+as\b', quer[chunk_idx:running_idx], flags=re.IGNORECASE)
                if len(matches) == 0: return idxs
                cte_name = matches[0]
            running_idx += 1
        while recursiveness > 0:
            recursiveness += 1 if quer[running_idx] == '(' else -1 if quer[running_idx] == ')' else 0
            running_idx += 1
            if recursiveness == 0 and running_idx < quer_end_idx and quer[running_idx] != ',':
                idxs.append((cte_name, start_pos + 1, running_idx - 1))
                return idxs  
        idxs.append((cte_name, start_pos + 1, running_idx - 1))
        chunk_idx = running_idx
    return idxs


def SerializeDataframe(df : pl.DataFrame, name : str):
    for i in range(len(df.dtypes)):
        col = df.columns[i]
        dtype = df.dtypes[i]
        dummy_vals = []
        if dtype == pl.Datetime:
            dummy_vals.append("'1970-01-01'::timestamp")
            df = df.with_columns(
                (
                    pl.lit("'") +                                          
                    pl.col(col).dt.strftime("%Y-%m-%d %H:%M:%S") +         
                    pl.lit("'::timestamp")                                 
                ).alias(col)
            )
        elif dtype == pl.Date:
            dummy_vals.append("'1970-01-01'::date")
            df = df.with_columnNs(
                (
                    pl.lit("'") +                                          
                    pl.col(col).dt.strftime("%Y-%m-%d") +         
                    pl.lit("'::date")                                 
                ).alias(col)
            )
        elif dtype == pl.String:
            dummy_vals.append("'dummy'")
        elif dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Int128, pl.Float16, pl.Float32, pl.Float64):
            dummy_vals.append(-1)

    vals = ',\n\t\t\t'.join([f'({str(x)[1:-1].rstrip(',')})' for x in df.unique().rows()] ).replace('"','') if len(df) > 0 else f'({', '.join(dummy_vals)})'
    cols = ', '.join(list(df.columns))
    return \
f'''{name} as (
    select 
        * from ( VALUES
            {vals}
        ) as t({cols})
    ),'''


class CTENode:
    raw_query : str
    transformed : str
    name : str
    backlinks : list[str]
    is_materialized : bool
    df : pl.DataFrame
    mat_paths : list
    backlink_path : str
    inject_path : str
    parent : "QueryStruct"
    callbacks : list[Callable]

    def __init__(self, name : str, query : str, struct : "QueryStruct"):
        self.is_materialized = False
        self.serialized_fields = {}
        self.callbacks = []
        self.parent = struct
        self.name = name.lower()
        self.backlinks = []
        self.raw_query = query.lower()
        if not isinstance(self.parent, QueryStruct): return
        self.mat_paths = [f'./{'output' if MATERIAL_PERMANENCE else 'eph_materializations'}/{self.parent.skeleton.name}/{'' if self.parent.name == '' else f'{self.parent.name}/'}materializations']
        os.makedirs(self.mat_paths[0], exist_ok=True)
        self.inject_path = f'./injected_queries/{self.parent.skeleton.name}/{'' if self.parent.name == '' else f'{self.parent.name}/'}'
        if INJECT_LOGGING: os.makedirs(self.inject_path, exist_ok=True)
        self.backlink_path = f'./backlink_queries/{self.parent.skeleton.name}/{'' if self.parent.name == '' else f'{self.parent.name}/'}'
        if INJECT_LOGGING: os.makedirs(self.inject_path, exist_ok=True)
        self.AnalyzeSubquery()

    def ExtractBacklinks(self):
        lexemes = re.sub(r'\s+', ' ', self.raw_query)
        return list(set([lex for lex in lexemes if lex in self.parent.cte_lookup]))[::-1]

    def AnalyzeSubquery(self):
        pattern = r"(\(\s*select\s+.*?from\s+(\w+)+.*?\))"
        inner_pattern = r"from\s+(\w+)"

        serializations = [] 
        query = self.raw_query
        backlink_count = 0
        for obj in list(re.finditer(pattern, self.raw_query)):
            if obj.group(2) not in self.parent.cte_lookup.keys():
                continue
            backlink_query = re.sub(inner_pattern, 'from self', obj.group(1))[1:-1]
            backlink_sub = re.sub(inner_pattern, f'from backlink_{backlink_count}', obj.group(1))
            serializations.append((obj.group(2), backlink_query))
            query = query.replace(obj.group(1), backlink_sub)
            backlink_count += 1
        self.transformed = query
        for obj in serializations:
            self.backlinks.append((obj[0], obj[1]))

    def ResolveQuery(self, quer : str):
            res = self.df.sql(quer)
            return res
        

    def Materialize(self):
        print(f' Resolving backlinks for {self.name}', flush=True)
        materialization_fnames = [f'{path}/{self.name}.csv' for path in self.mat_paths]
        if self.is_materialized:
            try:
                self.df = pl.read_csv(materialization_fnames[0], try_parse_dates=True)
                print(f' Read anchored query for {self.name}', flush=True)
                return
            except Exception as ex:
                HandleError('CTE marked as pre-materialized but csv missing')
        skip_flag = True
        injects = []
        for idx, (cte_name, quer) in zip(range(len(self.backlinks)),self.backlinks):
            injects.append(SerializeDataframe(self.parent.cte_lookup[cte_name].ResolveQuery(quer), f'backlink_{idx}'))
        if BACKLINK_LOGGING : LogQuery(self.transformed, f'{self.backlink_path}', self.name)
        
        injected_pref = ''
        if len(injects) > 0:
            injected_pref = 'with '
            for quer in injects:
                injected_pref += quer
            injected_pref = injected_pref[:-1]
        self.transformed = injected_pref + self.transformed
        
        if INJECT_LOGGING : LogQuery(self.transformed, f'{self.inject_path}', self.name)
        try:
            self.df = pl.read_csv(materialization_fnames[0], try_parse_dates=True)
        except Exception as ex:
            print('    Serialization not found, skipping is not an option', flush=True)
            skip_flag = False

        if(skip_flag and not AUTORUN):
            choice = input('    Serialializtion detected. Want to skip materialization? (y)')
            if choice == 'y': return
        try:
            print('    Fetching', flush=True)
            self.df = pl.read_database(
                query = self.transformed,
                connection = self.parent.skeleton.conn
            )
        except errors.StatementTooComplex as ex:
            HandleError('Statement too complex, avoid multi-indexing')
        except Exception as ex:
            LogQuery(str(ex), './logs', 'error_log')
            HandleError('ERROR LOGGED',2)
        for mat_name in materialization_fnames:
            self.df.write_csv(mat_name)
        self.is_materialized = True
        self.MaterializationCallback()

    def MaterializationCallback(self):
        for func in self.callbacks:
            func()

class QueryStruct:
    cte_lookup : dict[str, CTENode]
    raw_quer : str
    recomp_quer : str
    name : str
    df : pl.DataFrame
    skeleton : "QuerySkeleton"

    def __init__(self, parent : "QuerySkeleton", quer : str, name : str):
        self.df = pl.DataFrame()
        self.raw_quer = quer
        self.skeleton = parent
        self.name = name
        self.cte_lookup = {}
        for cte_name, start, end in GetQueryIdxs(quer):
            self.cte_lookup[cte_name] = CTENode(cte_name, quer[start:end], self)
        self.recomp_quer = quer[end+1:]
        LogQuery(self.recomp_quer, f'./logs', self.skeleton.name)

    def Execute(self):
        sql_ctx = {}
        print(f'Executing struct {self.name}')
        for cte_name in self.cte_lookup:
            self.cte_lookup[cte_name].Materialize()
        lookup = self.skeleton.banked_ctes | self.cte_lookup
        for cte_name in lookup:
            sql_ctx[cte_name] = lookup[cte_name].df

        sql_sandbox = pl.SQLContext(frames=sql_ctx)
        if self.recomp_quer.lower().strip() == 'pass':
            print(' No recomposition needed')
            return
        print(' Recomposing', flush=True)
        self.df = sql_sandbox.execute(self.recomp_quer).collect()
        print(' Recomposed', flush=True)
        self.df.write_csv(f'./output/{self.skeleton.name}/{'' if self.name == '' else f'{self.name}/'}{'final' if self.name == '' else self.name}.csv')


class QuerySkeleton:
    name : str
    conn : ConnectionOrCursor
    steps : list[QueryStruct]
    banked_ctes : dict[str,CTENode]
    post_func : Callable
    def __init__(self, text : str, name : str, conn : ConnectionOrCursor):
        config = {}
        self.name = name
        self.conn = conn
        self.banked_ctes = {}
        start_idx = text.find('with')
        quer = text[start_idx:]
        self.steps = []
        try:
            config = json.loads(text[:start_idx])
            print('Loaded config', flush=True)
        except Exception as ex:
            print('Malformed config', flush=True)
            HandleError(ex)
        self.SetupBank(config)
        self.StageStructs(quer,config)
        self.StageKnowledge(config)
        os.makedirs(f'./output/{self.name}', exist_ok=True)
        self.StagePost(config)

        

    def SetupBank(self, config : dict):
        os.makedirs('knowledge_bank', exist_ok=True)
        bank = list[str]
        try:
            bank = config['banked']
        except Exception as ex:
            print('No banked CTEs', flush=True)
            return
        
        try:
            for b in bank:
                banked_cte = CTENode(b, 'select 1', self)
                banked_cte.df = pl.read_csv(f'knowledge_bank/{b}.csv')
                banked_cte.is_materialized = True
                self.banked_ctes[b] = banked_cte
        except Exception as ex:
            HandleError('Invalid Bank')



    def StageStructs(self, quer : str, config : dict):
        try:
            if config['clean']: 
                shutil.rmtree(f'./output/{self.name}')
        except Exception as ex:
            pass
        repeater_params : dict
        try:
            repeater_params = config['repeater_params']
        except Exception as ex:
            print('No repeater_params detected, running in mono')
            self.steps.append(QueryStruct(self,quer,''))
            return
           
        for idx, step in zip(range(len(config['repeater_params']['steps'])), config['repeater_params']['steps']):
            for k in step:
                if isinstance(step[k], str):
                    step[k] = f"'{step[k]}'"
            self.steps.append(QueryStruct(self,quer.format(**step),f'part_{idx+1}'))

        if 'anchored_ctes' not in config: return
        
        for cte_name in repeater_params['anchored_ctes']:
            ctes = [struct.cte_lookup[cte_name] for struct in self.steps]
            seeded_outputs = [f'./{'output' if MATERIAL_PERMANENCE else 'eph_materializations'}/{cte.parent.skeleton.name}/{'' if cte.parent.name == '' else f'{cte.parent.name}/'}materializations' for cte in ctes]
            ctes[0].mat_paths = seeded_outputs
            def callback():
                for cte in ctes[1:]:
                    cte.is_materialized = True
                    print(f'Anchored CTE - {cte.parent.name}.{cte.name}- {cte.is_materialized}')
            ctes[0].callbacks.append(callback)


    def StageKnowledge(self, config : dict):
        if 'knowledge_bank' not in config: return
        if len(self.steps) > 1 : print('NOTICE: only first step comit to knowledge bank', flush=True)
        for k in config['knowledge_bank']:
            self.steps[0].cte_lookup[k].mat_paths.append(f'./knowledge_bank/')
        print('Staged knowlege bank')

    def StagePost(self, config : dict):
        if 'post_exec' not in config:
            print('No post_exec',flush=True)
            self.post_func = lambda : print('No post func', flush=True)
            return
        post_ex = config['post_exec']
        file = post_ex['file']
        args = post_ex['args']
        def post():
            print('Starting subprocess', flush=True)
            os.system(f'cp ./input/{file} ./output/{self.name}/{file}')
            process = subprocess.Popen(
                [sys.executable, f'./output/{self.name}/{file}'] + args,
                stdout=subprocess.PIPE,
                text=True,
                bufsize=1
            )

            if process.stdout:
                for line in process.stdout:
                    print('> ' + line.strip(), flush=True)

            process.wait()
            print('Finished subprocess', flush=True)
        self.post_func = post



    def Execute(self):
        dfs = []
        for st in self.steps:
            st.Execute()
            dfs.append(st.df)
        fin = pl.concat(dfs)
        if len(fin) > 0: fin.write_csv(f'./output/{self.name}/final.csv')
        self.post_func()

