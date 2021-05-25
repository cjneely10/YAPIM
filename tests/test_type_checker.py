from unittest import TestCase

from yapm.utils.type_checker import TypeChecker


@TypeChecker()
class Simple:
    def returns_int(self) -> int:
        return 0


class TestTypeChecker(TestCase):

    def test_simple_class(self):
        val = Simple()
        self.assertEqual(val.returns_int(), 0)
