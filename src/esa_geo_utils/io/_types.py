from enum import Enum
from typing import Tuple, Union


class ConcurrencyStrategy(Enum):
    """Valid concurrency strategies."""

    FILES = "files"
    ROWS = "rows"


Chunk = Union[Tuple[int, int], Tuple[None, None]]
Chunks = Tuple[Chunk, ...]
