from GLS.das_decennial.programs.schema.attributes.abstractattribute import AbstractAttribute
from GLS.das_decennial.das_constants import CC

class SexAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_SEX

    @staticmethod
    def getLevels():
        return {
            'Male'  : [0],
            'Female': [1]
        }

    @staticmethod
    def recodeMale():
        name = CC.SEX_MALE
        groups = {
            "Male": [0]
        }
        return name, groups

    @staticmethod
    def recodeFemale():
        name = CC.SEX_FEMALE
        groups = {
            "Female": [1]
        }
        return name, groups
