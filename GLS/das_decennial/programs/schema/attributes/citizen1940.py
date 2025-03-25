from GLS.das_decennial.programs.schema.attributes.abstractattribute import AbstractAttribute
from GLS.das_decennial.das_constants import CC


class Citizen1940Attr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_CITIZEN_1940

    @staticmethod
    def getLevels():
        return {
            'Not a Citizen': [0],
            'Citizen': [1]
        }
