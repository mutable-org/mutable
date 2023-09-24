import sys
from tqdm import tqdm
from typeguard import typechecked


#=======================================================================================================================
# Print information to console using `tqdm`
#=======================================================================================================================
@typechecked
def tqdm_print(text: str) -> None:
    tqdm.write(text)
    sys.stdout.flush()
