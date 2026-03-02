FIRST_USAGE_HEURISTIC_BONUS = -50000

DEFAULT_TYPE_SIZE_BYTES = {
    "int": 8,
    "float": 8,
    "str": 20,
    "bool": 1,
    "numeric": 8,
    "list": 1000,
    "dict": 5000,
    "tuple": 1000,
    "set": 5000,
    "DataFrame": 100000,
    "Series": 10000,
    "unknown": 8,
}

MIN_VALID_CUT_LINE = 3
