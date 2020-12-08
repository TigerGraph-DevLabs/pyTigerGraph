class TigerGraphException(Exception):
    """Generic TigerGraph related exception.

    Where possible, error message and code returned by TigerGraph will be used.
    """

    def __init__(self, message, code=None) -> object:
        self.message = message
        self.code = code
