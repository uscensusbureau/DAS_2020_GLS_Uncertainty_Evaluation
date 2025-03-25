from GLS.das_decennial.programs.schema.attributes.abstractattribute import AbstractAttribute
from GLS.das_decennial.das_constants import CC


class CitizenAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_CITIZEN

    @staticmethod
    def getLevels():
        return {
                    'Non-citizen': [0],
                    'Citizen'    : [1]
                }