from abc import ABC, abstractmethod
import os
import multiprocessing

DEFAULT_TIMEOUT = 60  # seconds
TIMEOUT_PER_CASE = 10 # seconds

#=======================================================================================================================
# Connector exceptions
#=======================================================================================================================

class ConnectorException(Exception):
    def __init__(self, what :str):
        super().__init__(what)

class ExperimentTimeoutExpired(ConnectorException):
    def __init__(self, what :str):
        super().__init__(what)

class AttributeTypeUnknown(ConnectorException):
    def __init__(self, what :str):
        super().__init__(what)



# Helper function to get number of cores
def get_num_cores():
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
    def execute(self, n_runs: int, params: dict):
        pass
