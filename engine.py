import re
import polars as pl
from polars.exceptions import ColumnNotFoundError
from polars._typing import ConnectionOrCursor
from sqlalchemy import exc
from psycopg2 import errors
import json
import os

AUTORUN = False
MATERIAL_PERMANENCE = True
BACKLINK_LOGGING = True
INJECT_LOGGING = True

print('\033[1mSQL Decomposer v3.0.1\033[0m', flush=True)

os.makedirs('./output', exist_ok=True)
os.makedirs('./input', exist_ok=True)

def HandleError(ex : str, indents = 0):
    prefix = '  ' * indents
    print(f'{prefix}\033[31m{ex}\033[0m', flush=True)
    exit()

def LogQuery(quer : str, path : str, title : str):
    os.makedirs(path, exist_ok=True)
    with open(f'{path}/{title}.sql', 'w') as f:
        f.write(quer)

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
    vals = ',\n\t\t\t'.join([f'({str(x)[1:-1].rstrip(',')})' for x in df.unique().rows()])
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
    config : dict

    parent : "QueryStruct"

    def __init__(self, name : str, query : str, skeleton : "QuerySkeleton"):
        self.is_materialized = False
        self.serialized_fields = {}
        self.parent = skeleton
        self.name = name.lower()
        self.backlinks = []
        self.raw_query = query.lower()
        self.mat_path = f'./{'output' if MATERIAL_PERMANENCE else 'eph_materializations'}/{self.parent.skeleton.name}/{'' if self.parent.name == '' else f'{self.parent.name}/'}materializations'
        os.makedirs(self.mat_path, exist_ok=True)
        self.inject_path = f'./injected_queries/{self.parent.skeleton.name}/{'' if self.parent.name == '' else f'{self.parent.name}/'}'
        self.backlink_path = f'./backlink_queries/{self.parent.skeleton.name}/{'' if self.parent.name == '' else f'{self.parent.name}/'}'
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
        os.makedirs(self.mat_path, exist_ok=True)
        materialization_fname = f'{self.mat_path}/{self.name}.csv'
        skip_flag = True
        injects = []
        for idx, (cte_name, quer) in zip(range(len(self.backlinks)),self.backlinks):
            injects.append(SerializeDataframe(self.parent.cte_list[cte_name].ResolveQuery(quer), f'backlink_{idx}'))
        if BACKLINK_LOGGING : LogQuery(self.transformed, f'{self.backlink_path}/{self.parent.name}', self.name)
        
        injected_pref = ''
        if len(injects) > 0:
            injected_pref = 'with '
            for quer in injects:
                injected_pref += quer
            injected_pref = injected_pref[:-1]
        self.transformed = injected_pref + self.transformed
        
        if INJECT_LOGGING : LogQuery(self.transformed, f'{self.inject_path}/{self.parent.name}', self.name)
        try:
            self.df = pl.read_csv(materialization_fname)
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
        self.df.write_csv(materialization_fname)
        self.is_materialized = True

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
            self.df.write_csv(f'./output/{self.skeleton.name}/{'final' if self.name == '' else self.name}.csv')
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
            for idx, d in zip(range(len(config['repeater_params'])), config['repeater_params']):
                self.steps.append(QueryStruct(self,quer.format(**d),f'part_{idx+1}'))
        except Exception as ex:
            self.steps.append(QueryStruct(self,quer,''))                

    def Execute(self):
        for st in self.steps:
            st.Execute()
