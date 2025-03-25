"""
This attribute simply copies almost all of the recodes of the attribute HHGQPersonSimpleAttr. It has a different name than HHGQPersonSimpleAttr to emphasize
that three levels have a slightly different definition than the standard definition of these levels used for HHGQPersonSimpleAttr. This difference is
that GQ types 106 and 404 are included in the Military Quarters major GQ group.
"""

from GLS.das_decennial.programs.schema.attributes.hhgq_person_simple import HHGQPersonSimpleAttr
from GLS.das_decennial.das_constants import CC


class HHGQ_EPAttr(HHGQPersonSimpleAttr):
    @staticmethod
    def getName():
        return CC.ATTR_HHGQ_EP

    @staticmethod
    def recodeGqlevels():
        """
        Simply returns each level of HHGQ_EPAttr
        """
        name = CC.HHGQ_GQLEVELS
        groups = {k: v for k, v in HHGQ_EPAttr.getLevels().items()}

        return name, groups
