from GLS.das_decennial.programs.schema.attributes.hhrace import HHRaceAttr
from GLS.das_decennial.das_constants import CC


class RaceAttr(HHRaceAttr):

    @staticmethod
    def getName():
        return CC.ATTR_RACE

    @staticmethod
    def getLevels():
        return {
            'white'      : [0],
            'black'      : [1],
            'aian'       : [2],
            'asian'      : [3],
            'nhopi'      : [4],
            'sor'        : [5],
            'two or more': [6]
        }

    @staticmethod
    def recodeWhiteAlone():
        name = "whitealone"
        groupings = {
            "White alone": [0]
        }
        return name, groupings
    @staticmethod
    def recodeBlackAlone():
        name = "blackalone"
        groupings = {
            "Black alone": [1]
        }
        return name, groupings

    @staticmethod
    def recodeAIANAlone():
        name = "aianalone"
        groupings = {
            "AIAN alone": [2]
        }
        return name, groupings

    @staticmethod
    def recodeAsianAlone():
        name = "asianalone"
        groupings = {
            "Asian alone": [3]
        }
        return name, groupings

    @staticmethod
    def recodeNHOPIAlone():
        name = "nhopialone"
        groupings = {
            "NHOPI alone": [4]
        }
        return name, groupings

    @staticmethod
    def recodeSORAlone():
        name = "soralone"
        groupings = {
            "Some other race alone": [5]
        }
        return name, groupings

    @staticmethod
    def recodeTOMR():
        name = "tomr"
        groupings = {
            "Two or more races": [6]
        }
        return name, groupings