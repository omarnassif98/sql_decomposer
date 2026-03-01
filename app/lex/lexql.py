from helper import HandleError, LogQuery, RetrieveSetting, ColorText, bcolors, GetOutputFolder, DecompReadFunction, DecompWriteFunction
from typing import Callable
import re
import polars as pl
from polars._typing import ConnectionOrCursor
from psycopg2 import errors
import json
import os
from lex.lextensions import SetupExtensions
import shutil

AUTORUN : bool
BACKLINK_LOGGING : bool
INJECT_LOGGING : bool

def InitEngine():
    global AUTORUN, BACKLINK_LOGGING, INJECT_LOGGING
    AUTORUN, BACKLINK_LOGGING, INJECT_LOGGING  = RetrieveSetting(['AUTORUN', 'BACKLINK_LOGGING', 'INJECT_LOGGING'])


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
            df = df.with_columns(
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
        self.mat_paths = [f'./{self.parent.skeleton.mat_paths[0]}/{self.parent.name}']
        os.makedirs(self.mat_paths[0], exist_ok=True)
        self.inject_path = f'./injected_queries/{self.parent.skeleton.name}/{'' if self.parent.name == '' else f'{self.parent.name}/'}'
        if INJECT_LOGGING: os.makedirs(self.inject_path, exist_ok=True)
        self.backlink_path = f'./backlink_queries/{self.parent.skeleton.name}/{'' if self.parent.name == '' else f'{self.parent.name}/'}'
        if INJECT_LOGGING: os.makedirs(self.inject_path, exist_ok=True)
        self.AnalyzeSubquery()

    def AnalyzeSubquery(self):
        pattern = r"(\(\s*select\s+.*?from\s+(\w+)+.*?\))"
        inner_pattern = r"from\s+(\w+)"

        serializations = [] 
        query = self.raw_query
        backlink_count = 0
        for obj in list(re.finditer(pattern, self.raw_query)):
            if obj.group(2) not in self.parent.cte_lookup.keys() and obj.group(2) not in self.parent.skeleton.banked_ctes.keys():
                continue
            backlink_query = re.sub(inner_pattern, 'from self', obj.group(1))[1:-1]
            backlink_sub = re.sub(inner_pattern, f'from backlink_{backlink_count}', obj.group(1))
            serializations.append((obj.group(2), backlink_query))
            query = query.replace(obj.group(1), backlink_sub)
            backlink_count += 1
        self.transformed = query
        if self.transformed.strip().startswith('with') and backlink_count > 0:
            self.transformed = self.transformed.replace('with', ' ,', 1)
        for obj in serializations:
            self.backlinks.append((obj[0], obj[1]))

    def ResolveQuery(self, quer : str):
            res = self.df.sql(quer)
            return res
        

    def Materialize(self):
        print(f' Resolving backlinks for {self.name}', flush=True)
        materialization_fnames = [f'{path}/{self.name}' for path in self.mat_paths]
        if self.is_materialized:
            try:
                self.df = DecompReadFunction(materialization_fnames[0])
                print(ColorText(f' Read anchored query for {self.name}', bcolors.OKGREEN), flush=True)
                return
            except Exception as ex:
                HandleError('CTE marked as pre-materialized but parquet missing')
        skip_flag = True
        injects = []
        for idx, (cte_name, quer) in zip(range(len(self.backlinks)),self.backlinks):
            if cte_name in self.parent.cte_lookup:
                injects.append(SerializeDataframe(self.parent.cte_lookup[cte_name].ResolveQuery(quer), f'backlink_{idx}'))
            else:
                injects.append(SerializeDataframe(self.parent.skeleton.banked_ctes[cte_name].ResolveQuery(quer), f'backlink_{idx}'))
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
            self.df = DecompReadFunction(materialization_fnames[0])
        except Exception as ex:
            print('    Serialization not found, skipping is not an option', flush=True)
            skip_flag = False

        if skip_flag:
            if AUTORUN: 
                print(ColorText('    Using premade materialization', bcolors.OKCYAN), flush=True)
                return
            choice = input('    Serialializtion detected. Want to skip materialization? (y)')
            if choice == 'y': 
                print(ColorText('    Skipping', bcolors.OKCYAN), flush=True)
                return
        try:
            print('    Fetching', flush=True)
            self.df = pl.read_database(
                query = self.transformed,
                connection = self.parent.skeleton.conn,
                infer_schema_length=None
            )
            print(ColorText('    Fetched', bcolors.OKGREEN), flush=True)
        except errors.StatementTooComplex as ex:
            HandleError('Statement too complex, avoid multi-indexing')
        except Exception as ex:
            HandleError('ERROR LOGGED')
        for mat_name in materialization_fnames:
            DecompWriteFunction(self.df, mat_name)
        self.is_materialized = True
        self.MaterializationCallback()

    def MaterializationCallback(self):
        for func in self.callbacks:
            func()

class RecompStruct:
    df : pl.DataFrame
    name : str
    recomp_quer : str
    quer_struct : "QueryStruct"
    def __init__(self, name : str, quer : str, struct : "QueryStruct"):
        self.df = pl.DataFrame()
        self.name = name
        self.recomp_quer = quer
        self.quer_struct = struct
    
    def Execute(self, ctx : pl.SQLContext):
        if self.recomp_quer.lower().strip() == 'pass':
            print(' No recomposition needed')
            return
        print(' Recomposing ' + ColorText(self.name, bcolors.ITALICS), flush=True)
        self.df = ctx.execute(self.recomp_quer).collect()
        print(ColorText(' Recomposed', bcolors.OKGREEN), flush=True)
        self.df.write_csv(f'{self.quer_struct.skeleton.mat_paths[0]}/{self.quer_struct.name}/{self.name}.csv', float_scientific=False)



class QueryStruct:
    cte_lookup : dict[str, CTENode]
    raw_quer : str
    name : str
    recomps : list[RecompStruct]
    skeleton : "QuerySkeleton"

    def __init__(self, parent : "QuerySkeleton", quer : str, name : str):
        self.recomps = []
        self.raw_quer = quer
        self.skeleton = parent
        self.name = name
        self.cte_lookup = {}
        for cte_name, start, end in GetQueryIdxs(quer):
            self.cte_lookup[cte_name] = CTENode(cte_name, quer[start:end], self)
        recomp_section = quer[end+1:]
        recomp_pattern = r'>>\s*(\w+)\s*\n\s*((?:select|with).*?)(?=\n\s*>>|$)' # AI is a trillion dollar industry
        matches = re.findall(recomp_pattern, recomp_section, flags=re.IGNORECASE | re.DOTALL)
        if not matches:
            self.recomps = [RecompStruct(self.skeleton.name, recomp_section, self)]
        else: 
            for nam, body in matches:
                self.recomps.append(RecompStruct(nam,body,self))


    def Execute(self):
        sql_ctx = {}
        print(f'Executing struct {self.name}', flush=True)
        for cte_name in self.cte_lookup:
            self.cte_lookup[cte_name].Materialize()
        lookup = self.skeleton.banked_ctes | self.cte_lookup
        for cte_name in lookup:
            sql_ctx[cte_name] = lookup[cte_name].df

        sql_sandbox = pl.SQLContext(frames=sql_ctx)
        
        for recomp in self.recomps:
            recomp.Execute(sql_sandbox)

class QuerySkeleton:
    name : str
    conn : ConnectionOrCursor
    quer : str
    steps : list[QueryStruct]
    banked_ctes : dict[str,CTENode]
    post_funcs : list[Callable]
    mat_paths : list[str]
    recomp_strategy : Callable
    recomp_dfs : dict[list[pl.DataFrame]]  
    def __init__(self, text : str, name : str, conn : ConnectionOrCursor):
        self.name = name
        self.conn = conn
        self.banked_ctes = {}
        self.post_funcs = []
        start_idx = text.strip().find('with')
        self.quer = text[start_idx:]
        self.steps = []
        main_mat_path = GetOutputFolder(self.name)

        self.mat_paths = [main_mat_path]
        os.makedirs(self.mat_paths[0], exist_ok=True)
        self.recomp_strategy = QuerySkeleton.WideRecompilation

        try:
            conf = json.loads(text[:start_idx])
            SetupExtensions(self, conf)
        except Exception as ex:
            if start_idx == 0: print(ColorText('JSON not being used', bcolors.WARNING), flush=True)
            else: HandleError('JSON Error')
        if len(self.steps) == 0 : self.steps.append(QueryStruct(self, self.quer, 'default'))

    @staticmethod
    def WideRecompilation(skel : "QuerySkeleton"):
        print(ColorText('Running wide strategy', bcolors.OKBLUE))
        skel.recomp_dfs = {}
        for st in skel.steps:
            for recomp in st.recomps:
                if recomp.name not in skel.recomp_dfs: skel.recomp_dfs[recomp.name] = []
                if len(recomp.df) == 0: continue
                skel.recomp_dfs[recomp.name].append(recomp.df)

        for nam in skel.recomp_dfs:
            if len(skel.recomp_dfs[nam]) > 0: 
                final_df = pl.concat(skel.recomp_dfs[nam])
                for path in skel.mat_paths:
                    final_df.write_csv(f'{path}/{nam}.csv', float_scientific=False)


    def Execute(self):
        for st in self.steps:
            st.Execute()
        self.recomp_strategy(self)
        for func in self.post_funcs:
            func()

