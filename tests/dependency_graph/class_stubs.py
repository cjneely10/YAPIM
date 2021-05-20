from typing import List

from HPCBioPipe import Task, DependencyInput


class A(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def requires() -> List[str]:
        return []

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        pass


class B(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def requires() -> List[str]:
        return ["A"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        pass


class C(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def requires() -> List[str]:
        return ["B"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        pass


class D(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def requires() -> List[str]:
        return ["B"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return []

    def run(self):
        pass


class E(Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def requires() -> List[str]:
        return ["B"]

    @staticmethod
    def depends() -> List[DependencyInput]:
        return [DependencyInput("C")]

    def run(self):
        pass
