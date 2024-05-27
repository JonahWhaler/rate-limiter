class ExceededRateLimitError(Exception):
    __slots__ = ()

    def __init__(self, message: str):
        super().__init__(message)
