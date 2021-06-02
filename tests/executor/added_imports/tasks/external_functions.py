from typing import Union


def add_one(x: Union[int, str]) -> int:
    if isinstance(x, int):
        return x + 1
    return sum([ord(_c) for _c in x]) + 1
