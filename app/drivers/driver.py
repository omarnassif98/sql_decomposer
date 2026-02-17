from abc import ABC, abstractmethod
from typing import Callable
import os
from helper import HandleError, MigrateSandbox

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
    def run(self, decomp_func : Callable[[str], None]) -> None:
        while True:
            try:
                q = input('Enter name of decompsed query (Ctrl + C => Enter to quit)\n\t>')
                if not q.endswith('.sql'): q += '.sql'
                decomp_func(q.lower())
            except KeyboardInterrupt as ex:
                print('Quitting', flush=True)
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