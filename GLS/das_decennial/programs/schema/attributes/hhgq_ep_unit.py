"""
This attribute simply copies all of the recodes of the attribute HHGQUnitSimpleRecodedAttr. It has a different name than HHGQUnitSimpleRecodedAttr to emphasize
that three levels have a slightly different definition than the standard definition of these levels used for HHGQPersonSimpleAttr. This difference is
that GQ types 106 and 404 are included in the Military Quarters major GQ group.
"""

from GLS.das_decennial.programs.schema.attributes.hhgq_unit_simple_recoded import HHGQUnitSimpleRecodedAttr
from GLS.das_decennial.das_constants import CC


class HHGQEPUnitAttr(HHGQUnitSimpleRecodedAttr):
    @staticmethod
    def getName():
        return CC.ATTR_HHGQ_EP_UNIT
