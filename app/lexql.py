from helper import HandleError, LogQuery, RetrieveSetting
import re
import polars as pl
from polars.exceptions import ColumnNotFoundError
from polars._typing import ConnectionOrCursor
from sqlalchemy import exc
from psycopg2 import errors
import json
import os

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
        idxs.append((cte_name, start_pos + 1, running_idx - 1))
        chunk_idx = running_idx
    return idxs


def SerializeDataframe(df : pl.DataFrame, name : str):
    for i in range(len(df.dtypes)):
        col = df.columns[i]
        dtype = df.dtypes[i]
        if dtype == pl.Datetime:
            df = df.with_columns(
                (
                    pl.lit("'") +                                          
                    pl.col(col).dt.strftime("%Y-%m-%d %H:%M:%S") +         
                    pl.lit("'::timestamp")                                 
                ).alias(col)
            )
        elif dtype == pl.Date:
            df = df.with_columnNs(
                (
                    pl.lit("'") +                                          
                    pl.col(col).dt.strftime("%Y-%m-%d") +         
                    pl.lit("'::date")                                 
                ).alias(col)
            )

    vals = ',\n\t\t\t'.join([f'({str(x)[1:-1].rstrip(',')})' for x in df.unique().rows()]).replace('"','')
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
    backlinks : list
    is_materialized : bool
    df : pl.DataFrame
    mat_paths : list
    backlink_path : str
    inject_path : str
    parent : "QueryStruct"
    callbacks : list 

    def __init__(self, name : str, query : str, skeleton : "QuerySkeleton"):
        self.is_materialized = False
        self.serialized_fields = {}
        self.callbacks = []
        self.parent = skeleton
        self.name = name.lower()
        self.backlinks = []
        self.raw_query = query.lower()
        self.mat_paths = [f'./{'output' if MATERIAL_PERMANENCE else 'eph_materializations'}/{self.parent.skeleton.name}/{'' if self.parent.name == '' else f'{self.parent.name}/'}materializations']
        os.makedirs(self.mat_paths[0], exist_ok=True)
        self.inject_path = f'./injected_queries/{self.parent.skeleton.name}/{'' if self.parent.name == '' else f'{self.parent.name}/'}'
        if INJECT_LOGGING: os.makedirs(self.inject_path, exist_ok=True)
        self.backlink_path = f'./backlink_queries/{self.parent.skeleton.name}/{'' if self.parent.name == '' else f'{self.parent.name}/'}'
        if INJECT_LOGGING: os.makedirs(self.inject_path, exist_ok=True)
        self.AnalyzeSubquery()

    def ExtractBacklinks(self):
        lexemes = re.sub(r'\s+', ' ', self.raw_query)
        return list(set([lex for lex in lexemes if lex in self.parent.cte_list]))[::-1]

    def AnalyzeSubquery(self):
        pattern = r"(\(\s*select\s+.*?from\s+(\w+)+.*?\))"

        serializations = [] 
        query = self.raw_query
        backlink_count = 0
        for obj in list(re.finditer(pattern, self.raw_query)):
            if obj.group(2) not in self.parent.cte_list.keys(): 
                continue
            backlink_query = obj.group(1)[1:-1].replace(obj.group(2), 'self')
            serializations.append((obj.group(2), backlink_query))
            query = query.replace(obj.group(2), f'backlink_{backlink_count}')
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
            injects.append(SerializeDataframe(self.parent.cte_list[cte_name].ResolveQuery(quer), f'backlink_{idx}'))
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
            self.df = pl.read_database(
                query = self.transformed,
                connection = self.parent.skeleton.conn
            )
        except errors.StatementTooComplex as ex:
            HandleError('Statement too complex, avoid multi-indexing')
        except Exception as ex:
            LogQuery(str(ex), '.', 'error_log')
            HandleError('ERROR LOGGED',2)
        for mat_name in materialization_fnames:
            self.df.write_csv(mat_name)
        self.is_materialized = True
        self.MaterializationCallback()

    def MaterializationCallback(self):
        for func in self.callbacks:
            func()

class QueryStruct:
    cte_list : dict[str, CTENode]
    raw_quer : str
    recomp_quer : str
    name : str
    df : pl.DataFrame
    skeleton : "QuerySkeleton"

    def __init__(self, parent : "QuerySkeleton", quer : str, name : str):
        self.raw_quer = quer
        self.skeleton = parent
        self.name = name
        self.cte_list = {}
        for cte_name, start, end in GetQueryIdxs(quer):
            self.cte_list[cte_name] = CTENode(cte_name, quer[start:end], self)
        self.recomp_quer = quer[end+1:]

    def Execute(self):
        sql_ctx = {}
        print(f'Executing struct {self.name}')
        for cte_name in self.cte_list:
            self.cte_list[cte_name].Materialize()
            sql_ctx[cte_name] = self.cte_list[cte_name].df
        sql_sandbox = pl.SQLContext(frames=sql_ctx)
        try:
            if self.recomp_quer.lower().strip() == 'pass':
                print(' No recomposition needed')
                return
            print(' Recomposing')
            self.df = sql_sandbox.execute(self.recomp_quer).collect()
            self.df.write_csv(f'./output/{self.skeleton.name}/{'' if self.name == '' else f'{self.name}/'}{'final' if self.name == '' else self.name}.csv')
        except Exception as ex: 
            HandleError(ex,1)

class QuerySkeleton:
    name : str
    conn : ConnectionOrCursor
    steps : list[QueryStruct]
    def __init__(self, text : str, name : str, conn : ConnectionOrCursor):
        config = {}
        self.name = name
        self.conn = conn
        start_idx = text.find('with')
        quer = text[start_idx:]
        self.steps = []
        try:
            config = json.loads(text[:start_idx])
            print('JSON Created')

        except Exception as ex:
            pass
        
        self.SetupSteps(quer,config)

    def SetupSteps(self, quer : str, config : dict):
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

        try:
            for cte_name in repeater_params['anchored_ctes']:
                ctes = [struct.cte_list[cte_name] for struct in self.steps]
                seeded_outputs = [f'./{'output' if MATERIAL_PERMANENCE else 'eph_materializations'}/{cte.parent.skeleton.name}/{'' if cte.parent.name == '' else f'{cte.parent.name}/'}materializations' for cte in ctes]
                ctes[0].mat_paths = seeded_outputs
                def callback():
                    for cte in ctes[1:]:
                        cte.is_materialized = True
                        print(f'Anchored CTE - {cte.parent.name}.{cte.name}- {cte.is_materialized}')
                ctes[0].callbacks.append(callback)

        except Exception as ex:
            pass
    def Execute(self):
        dfs = []
        for st in self.steps:
            st.Execute()
            dfs.append(st.df)
        pl.concat(dfs).write_csv(f'./output/{self.name}/final.csv')
