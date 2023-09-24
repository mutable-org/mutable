from abc import ABC, abstractmethod
from typing import TypeAlias, Callable
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
    def execute(self, n_runs: int, params: dict) -> ConnectorResult:
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
