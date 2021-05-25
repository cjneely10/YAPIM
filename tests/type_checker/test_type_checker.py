import abc
from typing import Union, Optional, Set, Sequence
from unittest import TestCase

from yapm.utils.type_checker import TypeChecker


class Str:
    def __init__(self, *args):
        self.args = args

    def __str__(self):
        return " ".join(self.args)


class TestTypeChecker(TestCase):

    def test_simple_class(self):
        @TypeChecker()
        class Simple:
            def returns_int(_self) -> int:
                return 1.0

            def proper_method(self, value: float) -> float:
                return value

        val = Simple()
        with self.assertRaises(TypeError):
            val.returns_int()
        self.assertEqual(1.0, val.proper_method(1.0))

    def test_nested(self):
        class Outer:
            @TypeChecker()
            class Inner:
                def returns_int(_self) -> int:
                    return 1.0

        with self.assertRaises(TypeError):
            Outer().Inner().returns_int()

    def test_child(self):
        class Parent:
            def returns_int(_self) -> int:
                return 1

        @TypeChecker()
        class Child(Parent):
            def returns_int(_self) -> int:
                return 1.0

        with self.assertRaises(TypeError):
            Child().returns_int()

    def test_abc(self):
        class AbstractParent(abc.ABC):
            @abc.abstractmethod
            def returns_int(self):
                """"""

        @TypeChecker()
        class Child(AbstractParent):
            def returns_int(self) -> int:
                return 1.0

        with self.assertRaises(TypeError):
            Child().returns_int()

    def test_user_class(self):

        @TypeChecker()
        class Simple:
            def simple(self, val: Str):
                pass

        Simple().simple(Str("val"))

    def test_union(self):

        @TypeChecker()
        class Simple:
            def simple(self, val: Union[str, Str]):
                pass

        with self.assertRaises(TypeError):
            Simple().simple([Str("val")])

    def test_bad_return(self):

        @TypeChecker()
        class Simple:
            def simple(self, val: Union[str, Str]) -> str:
                return int(val)

        with self.assertRaises(TypeError):
            Simple().simple("1")

    def test_good_return(self):

        @TypeChecker()
        class Simple:
            def simple(self, val: Union[str, Str]) -> str:
                return val

        Simple().simple("1")

    def test_return_union(self):

        @TypeChecker()
        class Simple:
            def simple(self, val: Union[str, Str]) -> Optional[str]:
                return val

        Simple().simple("1")

    def test_return_bad_union(self):

        @TypeChecker()
        class Simple:
            def simple(self, val: Union[str, Str]) -> Union[str, float]:
                return int(val)

        with self.assertRaises(TypeError):
            Simple().simple("1")

    def test_cache(self):
        TypeChecker.clear_cache()

        @TypeChecker()
        class Simple:
            def simple(self, val: Union[str, Str]) -> Union[str, int]:
                if isinstance(val, Str):
                    return int(str(val))
                return int(val)

        s = Simple()
        s.simple("1")
        s.simple(Str("1"))
        s.simple("2")
        s.simple(Str("2"))

        self.assertEqual(TypeChecker.CacheResults(2, 2, 4, 2), TypeChecker.get_cache_stats())

    def test_cache_rollover(self):
        TypeChecker.clear_cache()
        TypeChecker.set_max_cache_size(1)

        @TypeChecker()
        class Simple:
            def simple(self, val: Union[str, Str]) -> Union[str, int]:
                if isinstance(val, Str):
                    return int(str(val))
                return int(val)

        s = Simple()
        s.simple("1")
        s.simple(Str("1"))

        self.assertEqual(1, TypeChecker.get_current_cache_size())
        with self.assertRaises(TypeError):
            TypeChecker.set_max_cache_size(-1)

    def test_good_subclass(self):
        class Val(str):
            pass

        @TypeChecker()
        class Simple:
            def fxn(self, value: str):
                return value

        type(Simple().fxn(Val()))

    def test_bad_subclass(self):
        class Val(str):
            pass

        @TypeChecker()
        class Simple:
            def fxn(self, value: int):
                return value

        with self.assertRaises(TypeError):
            print(type(Simple().fxn(Val())))

    def test_nested_type(self):

        @TypeChecker()
        class Simple:
            def fxn(self, value: Set[str]):
                return value

        Simple().fxn({"1", "2"})

    def test_generic(self):

        @TypeChecker()
        class Simple:
            def fxn(self, value: Sequence[str]):
                return value

        Simple().fxn(["1", "2"])

    def test_bad_collection(self):

        @TypeChecker()
        class Simple:
            def fxn(self, value: Set[str]):
                return value

        with self.assertRaises(TypeError):
            Simple().fxn(["1", "2"])
