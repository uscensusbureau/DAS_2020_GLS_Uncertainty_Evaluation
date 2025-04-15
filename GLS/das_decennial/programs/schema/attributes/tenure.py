from GLS.das_decennial.programs.schema.attributes.abstractattribute import AbstractAttribute
from GLS.das_decennial.das_constants import CC

class TenureAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_TENURE

    @staticmethod
    def getLevels():
        return {
            "Mortgage": [0],
            "Owned"   : [1],
            "Rented"  : [2],
            "No Pay"  : [3]
        }

    @staticmethod
    def recodeTenure2Levels():
        """
        Returns number of owned units (owned or mortgage) and number of not owned (rented or no pay)
        """
        name = CC.TEN_2LEV
        groups = {
            "Owned": [0, 1],
            "Not owned": [2, 3]
        }
        return name, groups