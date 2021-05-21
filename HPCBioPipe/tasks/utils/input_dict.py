class ImmutableDict(dict):
    def __init__(self, input_data: dict):
        super().__init__(input_data)

    def __setitem__(self, key, value):
        raise AttributeError("Input is immutable and unable to be modified")
