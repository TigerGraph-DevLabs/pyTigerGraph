class TigerGraphException(Exception):
    """Generic TigerGraph related exception.

    Where possible, error message and code returned by TigerGraph will be used.
    """

    def __init__(self, message: str, code: [int, str] = None):
        """

        :param code:

        """
        self.message = message
        self.code = code
