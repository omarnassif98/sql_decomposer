from abc import ABC, abstractmethod
from typing import Callable
import os
import readline
from helper import HandleError, MigrateSandbox, ColorText, bcolors

class Driver(ABC):
    def __init__(self):
        MigrateSandbox()

    @abstractmethod
    def run(self, decomp_func : Callable[[str], None]) -> None: pass

class SequentialDriver(Driver):
    def run(self, decomp_func : Callable[[str], None]) -> None:
        for file in os.listdir('./input'):
            if not file.endswith('.sql'): continue
            decomp_func(file)

class CommandLineDriver(Driver):
    def LoadHistory():
        try:
            readline.read_history_file('config/cli.hist')

        except Exception as ex:
            readline.write_history_file('config/cli.hist')
            return []
        
    def AddToHistory(skel):
        readline.write_history_file('config/cli.hist')

    def run(self, decomp_func : Callable[[str], None]) -> None:
        CommandLineDriver.LoadHistory()
        while True:
            try:
                q = input('Enter name of decompsed query ' + ColorText('(Ctrl + C to quit)', bcolors.HEADER) + '\n\t>')
                CommandLineDriver.AddToHistory(q)
                if not q.endswith('.sql'): q += '.sql'
                decomp_func(q.lower())
            except KeyboardInterrupt as ex:
                print(ColorText('Quitting', bcolors.FAIL), flush=True)
                exit(0)
    

lookup : dict[str, Driver] 
lookup = {
    'cli' : CommandLineDriver,
    'sequential' : SequentialDriver
}

def GetDriver(driver_name : str) -> Driver:
    if driver_name not in lookup:
        HandleError('Improper interface mode')
    return lookup[driver_name]()