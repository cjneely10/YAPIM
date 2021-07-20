"""Immutable input dictionary to track results as they complete for each item in input"""


class InputDict(dict):
    """ Class holds a dictionary and provides a top-level preventative step for overwriting contents.

    This measure is entirely superficial, but is a simple way to keep top-level changes to the underlying data.

    For example, this is prevented:

    self.input["key"] = "value"

    However, this can easily be circumvented:

    data: list = self.input["key"]

    data.append(1)


    So, use with caution.
    """
    # pylint: disable=useless-super-delegation
    def __init__(self, input_data: dict):
        super().__init__(input_data)

    def __setitem__(self, key, value):
        raise AttributeError("Input is immutable and unable to be modified")
