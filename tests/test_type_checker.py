from unittest import TestCase

from yapm.utils.type_checker import TypeChecker


class TestTypeChecker(TestCase):

    def test_simple_class(self):
        @TypeChecker()
        class Simple:
            def returns_int(_self) -> int:
                return 1.0
        with self.assertRaises(TypeError):
            val = Simple()
            self.assertEqual(val.returns_int(), 0)
