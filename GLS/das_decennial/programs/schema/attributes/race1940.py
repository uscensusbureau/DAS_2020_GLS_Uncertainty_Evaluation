from GLS.das_decennial.programs.schema.attributes.abstractattribute import AbstractAttribute
from GLS.das_decennial.das_constants import CC


class Race1940Attr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_RACE_1940

    @staticmethod
    def getLevels():
        return {
            "White"                           : [0],
            "Black/African American"          : [1],
            "American Indian or Alaska Native": [2],
            "Chinese"                         : [3],
            "Japanese"                        : [4],
            "Other Asian or Pacific Islander" : [5]
        }
