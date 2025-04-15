"""
census race2010 abstract attribute module
"""
from GLS.das_decennial.programs.schema.attributes.abstractattribute import AbstractAttribute
from GLS.das_decennial.das_constants import CC


class Race2010Attr(AbstractAttribute):
    """Census RACE2010 Class"""
    @staticmethod
    def getName():
        """Get Attribute Name"""
        return CC.ATTR_RACE2010

    @staticmethod
    def getLevels():
        """Get Levels"""
        return {
                'white'                           : [0],
                'black'                           : [1],
                'aian'                            : [2],
                'asian'                           : [3],
                'nhopi'                           : [4],
                'white-black'                     : [5],
                'white-aian'                      : [6],
                'white-asian'                     : [7],
                'white-nhopi'                     : [8],
                'black-aian'                      : [9],
                'black-asian'                     : [10],
                'black-nhopi'                     : [11],
                'aian-asian'                      : [12],
                'aian-nhopi'                      : [13],
                'asian-nhopi'                     : [14],
                'white-black-aian'                : [15],
                'white-black-asian'               : [16],
                'white-black-nhopi'               : [17],
                'white-aian-asian'                : [18],
                'white-aian-nhopi'                : [19],
                'white-asian-nhopi'               : [20],
                'black-aian-asian'                : [21],
                'black-aian-nhopi'                : [22],
                'black-asian-nhopi'               : [23],
                'aian-asian-nhopi'                : [24],
                'white-black-aian-asian'          : [25],
                'white-black-aian-nhopi'          : [26],
                'white-black-asian-nhopi'         : [27],
                'white-aian-asian-nhopi'          : [28],
                'black-aian-asian-nhopi'          : [29],
                'white-black-aian-asian-nhopi'    : [30],
            }
