"""
Module holds class TypeChecker for simple function type check at runtime prior to function call
"""
import inspect
from collections import namedtuple
from typing import get_type_hints, Union, Type, get_args


class TypeChecker:
    """
    Class TypeChecker has simple decorator method to check if function has been called with specified parameters
    and to handle type checking if not yet called. Decorates at class level

    @TypeChecker()
    class TestClass:
        ...

    If TYPECHECKER=off is set as an environment variable, then no runtime checking will be handled.
    """
    # Default error strings
    ERR_STR = "Argument '%s' must be of type {}"
    RETURN_ERR_STR = "Returned object must be of type {} but '%s' was found"
    # Default cache size
    _max_cache_size = 1024
    # Tracking stats for current TypeChecker
    _cached_calls = 0
    _missed_calls = 0
    _total_calls = 0
    # Internal cache
    _cache = set()
    # Results struct
    CacheResults = namedtuple("CacheResults", ("cached_calls", "missed_calls", "total_calls", "current_cache_size"))
    function_type = (lambda: None).__class__

    @staticmethod
    def __call__(cls: type):
        """ Check if types of args/kwargs passed to function/method are valid for provided type signatures

        :param func: Called function/method
        :raises: TypeError for improper arg/kwarg type combinations
        :return: Decorated function/method. Raises TypeError if improper type/arg combination is found
        """

        def _check_function_call(func, *args, **kwargs):
            TypeChecker._clear_if_surpassed_max_size()
            # Get passed args as dict
            passed_args = inspect.signature(func).bind(*args, **kwargs).arguments
            # Get types specified by type annotations
            specified_types = get_type_hints(func)
            # Calculate id of function data
            cache_add_id = hash(tuple((*(type(arg) for arg in args), *(type(arg) for arg in kwargs.values()),
                                       id(func), func.__name__)))
            # Update call count
            TypeChecker._total_calls += 1
            # Check if cached
            if cache_add_id not in TypeChecker._cache:
                # Track as missed cache call
                TypeChecker._missed_calls += 1
                # Check arguments passed to ensure valid
                available_args = set(passed_args.keys()).intersection(set(specified_types.keys()))
                for arg_name in available_args:
                    TypeChecker._validate_type(
                        specified_types[arg_name], passed_args[arg_name], TypeChecker.ERR_STR % arg_name
                    )
            else:
                # Track as using cache call
                TypeChecker._cached_calls += 1
            # Get function output
            output = func(*args, **kwargs)
            # Confirm output is valid
            if "return" in specified_types.keys():
                TypeChecker._validate_type(specified_types["return"], output,
                                           TypeChecker.RETURN_ERR_STR % str(type(output)))
            # Add successful call to cache
            TypeChecker._cache.add(cache_add_id)
            return output

        def fxn(*args, **kwargs):
            for attr in dir(cls):
                _attr = getattr(cls, attr)
                if isinstance(_attr, TypeChecker.function_type):
                    setattr(cls, attr, _check_function_call(_attr, *args, **kwargs))
            return cls
        return fxn

    @staticmethod
    def set_max_cache_size(max_size: int):
        """ Set max cache. If current cache size exceeds max_size, current cache is cleared

        :param max_size: Number > 0 of cached checked-function calls to store
        :raises: TypeError for improper arg/kwarg type combinations
        """
        if isinstance(max_size, int) and max_size > 0:
            TypeChecker._max_cache_size = max_size
            TypeChecker._clear_if_surpassed_max_size()
            return
        raise TypeError("Must provide positive cache size")

    @staticmethod
    def clear_cache():
        """ Clear current cache contents

        """
        TypeChecker._cached_calls = 0
        TypeChecker._missed_calls = 0
        TypeChecker._total_calls = 0
        TypeChecker._cache = set()

    @staticmethod
    def get_current_cache_size() -> int:
        """ Get number of function call types stored in cache

        :return: Current number of call types stored in cache
        """
        return len(TypeChecker._cache)

    @staticmethod
    def get_cache_stats() -> "TypeChecker.CacheResults":
        """ Get current cache stats

        :return: (#cached calls, #non-cached calls, #total calls, current cache size)
        """
        return TypeChecker.CacheResults(
            cached_calls=TypeChecker._cached_calls,
            missed_calls=TypeChecker._missed_calls,
            total_calls=TypeChecker._total_calls,
            current_cache_size=TypeChecker.get_current_cache_size()
        )

    @staticmethod
    def _clear_if_surpassed_max_size():
        """ Check if cache size surpasses largest allowed and clear

        """
        if TypeChecker.get_current_cache_size() >= TypeChecker._max_cache_size:
            TypeChecker.clear_cache()

    @staticmethod
    def _check_union(arg_type: Union, passed_value: object) -> bool:
        """ Check Union type annotation to see if passed value is one of the specified types

        :param arg_type: Union annotation
        :param passed_value: type of argument passed
        :return: Status if passed_value is valid based on contents of Union
        """
        for avail_arg_type in get_args(arg_type):
            if isinstance(passed_value, avail_arg_type) or issubclass(type(passed_value), avail_arg_type):
                return True
        return False

    @staticmethod
    def _validate_type(arg_type: Type, output: object, err_string: str):
        """ Check if arg type matches actual arg value, if not display error string

        :param arg_type: Expected type
        :param output: Actual value
        :param err_string: Error string to display if failed
        :raises: TypeError if improper type found
        """
        if getattr(arg_type, "__origin__", None) is not None:
            if "Union" in str(arg_type) and not TypeChecker._check_union(arg_type, output):
                raise TypeError(err_string.format(" or ".join(list(map(str, get_args(arg_type))))))
            if "Union" not in str(arg_type) and not (isinstance(output, arg_type.__origin__)
                                                     or issubclass(type(output), arg_type.__origin__)):
                raise TypeError(err_string.format(arg_type))
        else:
            if not (isinstance(output, arg_type) or issubclass(type(output), arg_type)):
                raise TypeError(err_string.format(arg_type))
