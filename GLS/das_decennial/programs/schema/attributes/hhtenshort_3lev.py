from GLS.das_decennial.programs.schema.attributes.abstractattribute import AbstractAttribute
from GLS.das_decennial.das_constants import CC

class HHTenShort3LevAttr(AbstractAttribute):

    cef_names = ("TEN",)

    @staticmethod
    def getName():
        return CC.ATTR_HHTENSHORT_3LEV

    @staticmethod
    def getLevels():
        return {
            "Owned with mortgage"   : [0],
            "Owned without mortgage"  : [1],
            "Rented"  : [2]
        }

    @staticmethod
    def recodeTenure3Levels():
        """
        Returns number of owned units (owned or mortgage) and number of not owned (rented or no pay)
        """
        name = CC.TEN_3LEV
        groups = {
            "Owned with mortgage"   : [0],
            "Owned without mortgage"  : [1],
            "Not owned"  : [2]
        }
        return name, groups

    @staticmethod
    def recodeTenure2Levels():
        """
        Returns number of owned units (owned or mortgage) and number of not owned (rented or no pay)
        """
        name = "hhtenshort_2lev"
        groups = {
            "Owned": [0, 1],
            "Not owned": [2]
        }
        return name, groups

    @staticmethod
    def recodeTenureTotal():
        """
        Returns total number of owned plus rented units
        """
        name = "hhtentotal"
        groups = {
            "Total": [0, 1, 2]
        }
        return name, groups
