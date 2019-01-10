class Error(Exception):
    """Base error for LogDB system"""


class EncodeError(Error):
    """Error while encode value"""


class DecodeError(Error):
    """Error while decode bytes"""


class UnknownType(Error):
    """Unknow type"""


class APIError(Error):
    """API exception"""


class ProgramError(Error):
    """Program code exception"""


class WrongAmountOfArguments(Error):
    """Wrong amount of arguments"""
    # def __init__(self, given, must_be):
    #     self.given = given
    #     self.nust_be = must_be


class ToStringError:
    """Error in to_string method"""
