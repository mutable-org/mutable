from benchmark_utils import *

import os
import subprocess
from abc import ABC, abstractmethod
from typing import TypeAlias, Callable, Sequence, Any
import multiprocessing

DEFAULT_TIMEOUT: int  = 60   # seconds
TIMEOUT_PER_CASE: int = 10   # seconds

# Type definitions used to represent connector results
Case: TypeAlias = int | float | str
ConfigResult: TypeAlias = dict[Case, list[float]]      # case        --> list of measurements (size = NUM_RUNS)
ConnectorResult: TypeAlias = dict[str, ConfigResult]   # config name --> ConfigResult


#=======================================================================================================================
# Connector exceptions
#=======================================================================================================================

class ConnectorException(Exception):
    def __init__(self, what: str) -> None:
        super().__init__(what)

class ExperimentTimeoutExpired(ConnectorException):
    def __init__(self, what: str) -> None:
        super().__init__(what)

class AttributeTypeUnknown(ConnectorException):
    def __init__(self, what: str) -> None:
        super().__init__(what)


# Helper function to get number of cores
def get_num_cores() -> int:
    return multiprocessing.cpu_count()


#=======================================================================================================================
# Connector Abstract Base Class (ABC)
#=======================================================================================================================

class Connector(ABC):

    # Function that performs an experiment n_runs times given the parameters `params`.
    # Returns a dict with the measured times for the experiment and configuration.
    # Result has the form:
    # results
    # └── configurations
    #     └── cases
    #         └── times (list)
    #
    # results:          configuration name      --> configuration
    # configuration:    case  --> times
    # times:            list of floats (size=n_runs)
    #
    # Example: (n_runs=2)
    #   {
    #       'PostgreSQL':
    #           1: [1235.093, 1143.43],
    #           2: [1033.711, 1337.37],
    #           3: [1043.452, 1010.01],
    #           4: [1108.702, 1234.56]
    #   }
    @abstractmethod
    def execute(self, n_runs: int, params: dict[str, Any]) -> ConnectorResult:
        pass

    # Parse attributes of one table, return as string
    @staticmethod
    def parse_attributes(typeParser: dict[str, Callable[[list[str]], str]], attributes: dict[str, str]) -> str:
        columns: list[str] = list()
        for columnName, typeInfo in attributes.items():
            typeList: list[str] = [columnName]

            ty = typeInfo.split(' ')
            if ty[0] in typeParser:
                parse = typeParser[ty[0]]
                typeList.append(parse(ty))
            else:
                raise AttributeTypeUnknown(f"Unknown type given for '{columnName}'")

            if 'NOT NULL' in typeInfo and 'NOT NULL' in typeParser:
                typeList.append(typeParser['NOT NULL'](ty))
            if 'PRIMARY KEY' in typeInfo and 'PRIMARY KEY' in typeParser:
                typeList.append(typeParser['PRIMARY KEY'](ty))
            if 'UNIQUE' in typeInfo and 'UNIQUE' in typeParser:
                typeList.append(typeParser['UNIQUE'](ty))

            columns.append(' '.join(typeList))
        return '(' + ',\n'.join(columns) + ')'


    # Check whether all cases of an experiment should be executed together or each one singly.
    # Single execution is needed if the benchmark is not readonly or if there is at least one
    # table that has different scale factors for different cases.
    @staticmethod
    def check_execute_single_cases(params: dict[str, Any]) -> bool:
        readonly: bool = params.get('readonly', False)
        return not readonly or Connector.check_with_scale_factors(params)


    @staticmethod
    def check_with_scale_factors(params: dict[str, Any]) -> bool:
        for table in params.get('data', dict()).values():
            if table.get('scale_factors'):
                return True
        return False


    #===================================================================================================================
    # Start the shell with `command` and pass `query` to its stdin.  If the process does not respond after `timeout`
    # milliseconds, raise a ExperimentTimeoutExpired().  Return the stdout of the process containing the
    # measured results.  Parsing these results is expected to happen inside the connector.
    #===================================================================================================================
    def benchmark_query(
        self,
        command: str | bytes | Sequence[str | bytes],
        query: str,
        timeout: int,
        benchmark_info: str,
        verbose: bool,
        popen_args: dict[str, Any] = dict(),
        encode_query: bool = True,
    ) -> str:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   cwd=os.getcwd(), **popen_args)
        try:
            if encode_query:
                proc_out, proc_err = process.communicate(query.encode('latin-1'), timeout=timeout)
            else:
                proc_out, proc_err = process.communicate(query, timeout=timeout)
        except subprocess.TimeoutExpired:
            if verbose:
                tqdm_print(f"    ! Query \n'{query}'\n' timed out after {timeout} seconds")
            raise ExperimentTimeoutExpired(f'Query timed out after {timeout} seconds')
        finally:
            if process.poll() is None:          # if process is still alive
                process.terminate()             # try to shut down gracefully
                try:
                    process.wait(timeout=1)     # give process 1 second to terminate
                except subprocess.TimeoutExpired:
                    process.kill()              # kill if process did not terminate in time

        out: str = proc_out.decode('latin-1') if encode_query else proc_out
        err: str = proc_err.decode('latin-1') if encode_query else proc_err

        assert process.returncode is not None
        if process.returncode or len(err):
            outstr: str = '\n'.join(out.split('\n')[-20:])
            tqdm_print(f'''\
    Unexpected failure during execution of benchmark "{benchmark_info}" with return code {process.returncode}:''')
            self.print_command(command, query)
            tqdm_print(f'''\
    ===== stdout =====
    {outstr}
    ===== stderr =====
    {err}
    ==================
    ''')
            if process.returncode:
                raise ConnectorException(f'Benchmark failed with return code {process.returncode}.')

        return out


    # Connector should override this method
    def print_command(self, command: str | bytes | Sequence[str | bytes], query: str, indent: str = '') -> None:
        pass
