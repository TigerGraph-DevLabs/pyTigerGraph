class TigerGraphException(Exception):
    """Generic TigerGraph related exception.

    Where possible, error message and code returned by TigerGraph will be used.
    """

    def __init__(self, message: str, code: [int, str] = None):
        """Generic TigerGraph specific exception.

        Where possible, error message and code returned by TigerGraph will be used.

        :param str message:
        :param int|str code:
        """
        self.message = message
        self.code = code
