import pandas as pd
from typing import Dict, List
from itertools import product

from GLS.das_decennial.programs.schema.attributes.abstractattribute import AbstractAttribute
from GLS.das_decennial.das_constants import CC
from GLS.das_decennial.programs.schema.attributes.dhch.hhtype_map import mapping

# DataFrame of HHType
_HHType_df = pd.DataFrame(data=[v.values() for v in mapping.values()],
                          index=[k for k in mapping.keys()],
                          columns=[v for v in list(mapping.values())[0]])


class HHTypeDHCHAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHTYPE_DHCH

    @staticmethod
    def get_index(in_attr: Dict) -> int:
        """
        Get the HHType index for the attributes.
        This is the inverse of the get_attributes method.

        :param in_attr: Dictionary of the attributes.
        :return: Index.
        """
        expr = ""
        first = True

        for c in _HHType_df.columns:
            if first:
                first = False
            else:
                expr = expr + ' & '
            attr = in_attr.get(c)
            if attr is None:
                raise ValueError(f"Attribute not found in query for get_index: {c}. All attributes are required! The input attribute dictionary is: {in_attr}")
            expr = expr + f'{c} == {attr}'
        indices = HHTypeDHCHAttr.get_grouping(expr=expr)
        if len(indices) != 1:
            raise ValueError(f"Attributes do not correspond to a valid index. {in_attr}")
        # Convert to int as Panda's use of int64 is not usable in Spark DataFrames
        return int(indices[0])

    @staticmethod
    def get_attributes(index: int) -> Dict:
        """
        Get the attributes of the HHType for the index.
        This is the inverse of get_index method.

        :param index: Index value of the HHType.
        :return: Dictionary of the attributes.
        """
        return _HHType_df.iloc[index]

    @staticmethod
    def get_description(attrs: Dict) -> str:
        """
        Get the description from the attributes.  The description is the key for the getLevels method.

        :param attrs: Dictionary of the attributes.
        :return: Text
        """
        size = attrs.get('SIZE', 0)
        if size == 1:
            if attrs.get('CHILD_UNDER_18', 0):
                return "Size 1, not multig, no spouse or partner, living alone, c"
            else:
                return "Size 1, not multig, no spouse or partner, living alone, no c"
        size_desc = f"Size {size if size < 7 else '7+'}"
        multig_desc = f"{'multig' if attrs.get('MULTIG', 0) else 'not multig'}"
        if attrs.get('MARRIED', 0):
            married_desc = f"married {'opposite-sex' if attrs.get('MARRIED_OPPOSITE_SEX') else 'same-sex'}"
        elif attrs.get('COHABITING', 0):
            married_desc = f"cohabiting {'opposite-sex' if attrs.get('COHABITING_OPPOSITE_SEX') else 'same-sex'}"
        elif attrs.get('NO_SPOUSE_OR_PARTNER', 0):
            married_desc = f"no spouse or partner"
        else:
            married_desc = ""

        if attrs.get('NO_OWN_CHILD_UNDER_18', 0):
            if not attrs.get('MARRIED', 0):
                if attrs.get('NO_RELATIVES', 0):
                    married_desc = married_desc + f" with no relatives,"
                elif attrs.get('WITH_RELATIVES', 0):
                    married_desc = married_desc + f" with relatives,"
                child_desc = "no oc"
            else:
                child_desc = "with no oc"
            if attrs.get('NO_CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER', 0):
                child_desc = child_desc + ", no cxhhsup"
                if attrs.get("CHILD_UNDER_18", 0):
                    child_desc = child_desc + ", but has c"
                else:
                    child_desc = child_desc + ", no c"
            elif attrs.get('CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER', 0):
                if not attrs.get('MARRIED', 0):
                    cxhhsup_desc = f"cxhhsup u6 only" if attrs.get('CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER', 0) else \
                        f"cxhhsup 6to17" if attrs.get('CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER', 0) else \
                        f"cxhhsup both" if attrs.get('CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER', 0) else ""
                else:
                    cxhhsup_desc = f"cxhhsup u6 only" if attrs.get('CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER', 0) else \
                        f"cxhhsup 6to17 only" if attrs.get('CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER', 0) else \
                        f"cxhhsup both" if attrs.get('CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER', 0) else ""
                child_desc = child_desc + ", but has " + cxhhsup_desc
        elif attrs.get('OWN_CHILD_UNDER_18', 0):
            child_desc = f"with oc u6 only" if attrs.get('OWN_CHILD_UNDER_6_ONLY', 0) else \
                f"with oc 6to17 only" if attrs.get('OWN_CHILD_BETWEEN_6_AND_17', 0) else \
                f"with oc both" if attrs.get('OWN_CHILD_IN_BOTH_RANGES', 0) else ""
            if attrs.get('NO_SPOUSE_OR_PARTNER', 0):
                married_desc = married_desc + ","
            if attrs.get('CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER', 0):
                if attrs.get('OWN_CHILD_UNDER_6_ONLY', 0) and \
                        attrs.get('CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER', 0):
                    child_desc = child_desc + ", no cxhhsup 6to17"
                elif attrs.get('OWN_CHILD_UNDER_6_ONLY', 0) and \
                        attrs.get('CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER', 0):
                    child_desc = child_desc + ", cxhhsup both"
                elif attrs.get('OWN_CHILD_BETWEEN_6_AND_17', 0) and \
                        attrs.get('CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER', 0):
                    child_desc = child_desc + ", no cxhhsup u6"
                elif attrs.get('OWN_CHILD_BETWEEN_6_AND_17', 0) and \
                        attrs.get('CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER', 0):
                    child_desc = child_desc + ", cxhhsup both"
        else:
            child_desc = ""
        return f"{size_desc}, {multig_desc}, {married_desc} {child_desc}"

    @staticmethod
    def getLevels():
        levels = {}
        for index, attrs in mapping.items():
            description: str = HHTypeDHCHAttr.get_description(attrs)
            levels[description] = [index]

        return levels

    @staticmethod
    def get_grouping(expr: str) -> List[int]:
        """
        Get the List of HHType indices from the query of the data using the expression.

        :param expr: Expression for the query
        :return: List of valid indices.
        """
        return _HHType_df.query(expr=expr, inplace=False).index

    @staticmethod
    def recodeMultig():
        name = CC.HHTYPE_MULTIG
        groupings = {
                "Not a Multi-generational Household"          :   HHTypeDHCHAttr.get_grouping(expr=f'MULTIG == 0'),
                "Multi-generational Household"                :   HHTypeDHCHAttr.get_grouping(expr=f'MULTIG == 1'),
        }
        return name, groupings

    @staticmethod
    def recodePartnerTypeOwnChildStatus():
        name = CC.PARTNER_TYPE_OWN_CHILD_STATUS
        groupings = {
            "Married with own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED==1 and OWN_CHILD_UNDER_18==1'),
            "Married without own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED==1 and OWN_CHILD_UNDER_18==0'),
            "Cohabiting with own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'COHABITING==1 and OWN_CHILD_UNDER_18==1'),
            "Cohabiting without own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'COHABITING==1 and OWN_CHILD_UNDER_18==0'),
            "No spouse/partner, with own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'NO_SPOUSE_OR_PARTNER==1 and '
                                                                                                + f'OWN_CHILD_UNDER_18==1'),
            "No spouse/partner, without own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'NO_SPOUSE_OR_PARTNER==1 and '
                                                                                                + f'OWN_CHILD_UNDER_18==0'),
        }
        return name, groupings

    @staticmethod
    def recodePartnerTypeDetailedOwnChildStatus():
        name = CC.PARTNER_TYPE_DETAILED_OWN_CHILD_STATUS
        groupings = {}
        # TODO: Replace "OWN_CHILD_BETWEEN_6_AND_17" in dhch_map.py with "OWN_CHILD_BETWEEN_6_AND_17_ONLY":
        # Note that "OWN_CHILD_BETWEEN_6_AND_17==1" implies "OWN_CHILD_IN_BOTH_RANGES==0":
        for expr_tuple in product(["NO_SPOUSE_OR_PARTNER==1", "MARRIED==1", "COHABITING==1"],
                                  ["OWN_CHILD_UNDER_6_ONLY==1", "NO_OWN_CHILD_UNDER_18==1", "OWN_CHILD_BETWEEN_6_AND_17==1", "OWN_CHILD_IN_BOTH_RANGES==1"]):
            expr = " and ".join(expr_tuple)
            groups = HHTypeDHCHAttr.get_grouping(expr=expr)
            if len(groups) > 0:
                groupings[expr] = groups
        return name, groupings

    @staticmethod
    def recodeCoupledHHType():
        """Coupled household type [Official Metrics 13a-c]"""
        name = CC.COUPLED_HH_TYPE
        groupings = {
            "Married Same Sex": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED_SAME_SEX==1'),
            "Married Opposite Sex": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED_OPPOSITE_SEX==1'),
            "Unmarried Same Sex": HHTypeDHCHAttr.get_grouping(expr=f'COHABITING_SAME_SEX==1'),
            "Unmarried Opposite Sex": HHTypeDHCHAttr.get_grouping(expr=f'COHABITING_OPPOSITE_SEX==1'),
            "No spouse/partner": HHTypeDHCHAttr.get_grouping(expr=f'NO_SPOUSE_OR_PARTNER==1'),
        }
        return name, groupings

    @staticmethod
    def recodeDetailedCoupleTypeMultGenOwnChildSize():
        name = CC.DETAILED_COUPLE_TYPE_MULTG_CHILD_SIZE
        groupings = {}
        for expr_tuple in product(["NO_SPOUSE_OR_PARTNER==1", "MARRIED_SAME_SEX==1", "MARRIED_OPPOSITE_SEX==1", "COHABITING_SAME_SEX==1", "COHABITING_OPPOSITE_SEX==1"],
                                  ["MULTIG==0", "MULTIG==1"], ["OWN_CHILD_UNDER_18==0", "OWN_CHILD_UNDER_18==1"],
                                  ["SIZE==1", "SIZE==2", "SIZE==3", "SIZE==4", "SIZE==5", "SIZE==6", "SIZE==7"]):
            expr = " and ".join(expr_tuple)
            groups = HHTypeDHCHAttr.get_grouping(expr=expr)
            if len(groups) > 0:
                groupings[expr] = groups
        return name, groupings

    @staticmethod
    def recodeDetCoupleTypeMultGenDetOwnChildSize():
        name = CC.DET_COUPLE_TYPE_MULTG_DET_CHILD_SIZE
        groupings = {}
        for expr_tuple in product(["NO_SPOUSE_OR_PARTNER==1", "MARRIED_SAME_SEX==1", "MARRIED_OPPOSITE_SEX==1", "COHABITING_SAME_SEX==1", "COHABITING_OPPOSITE_SEX==1"],
                                  ["MULTIG==0", "MULTIG==1"], ["OWN_CHILD_UNDER_18==0", "OWN_CHILD_UNDER_6_ONLY==1", "OWN_CHILD_BETWEEN_6_AND_17==1", "OWN_CHILD_IN_BOTH_RANGES==1"],
                                  ["SIZE==1", "SIZE==2", "SIZE==3", "SIZE==4", "SIZE==5", "SIZE==6", "SIZE==7"]):
            expr = " and ".join(expr_tuple)
            groups = HHTypeDHCHAttr.get_grouping(expr=expr)
            if len(groups) > 0:
                groupings[expr] = groups
        return name, groupings

    @staticmethod
    def recodeDetCoupleTypeDetOwnChildSize():
        name = CC.DET_COUPLE_TYPE_DET_CHILD_SIZE
        groupings = {}
        for expr_tuple in product(["NO_SPOUSE_OR_PARTNER==1", "MARRIED_SAME_SEX==1", "MARRIED_OPPOSITE_SEX==1", "COHABITING_SAME_SEX==1", "COHABITING_OPPOSITE_SEX==1"],
                                  ["OWN_CHILD_UNDER_18==0", "OWN_CHILD_UNDER_6_ONLY==1", "OWN_CHILD_BETWEEN_6_AND_17==1", "OWN_CHILD_IN_BOTH_RANGES==1"],
                                  ["SIZE==1", "SIZE==2", "SIZE==3", "SIZE==4", "SIZE==5", "SIZE==6", "SIZE==7"]):
            expr = " and ".join(expr_tuple)
            groups = HHTypeDHCHAttr.get_grouping(expr=expr)
            if len(groups) > 0:
                groupings[expr] = groups
        return name, groupings

    @staticmethod
    def recodeFamilyNonfamilySize():
        name = CC.FAMILY_NONFAMILY_SIZE
        groupings = {
            "Family, Size=2"        : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==1 and SIZE==2'),
            "Family, Size=3"        : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==1 and SIZE==3'),
            "Family, Size=4"        : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==1 and SIZE==4'),
            "Family, Size=5"        : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==1 and SIZE==5'),
            "Family, Size=6"        : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==1 and SIZE==6'),
            "Family, Size=7+"       : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==1 and SIZE==7'),
            "Non-Family, Size=1"    : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==0 and SIZE==1'),
            "Non-Family, Size=2"    : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==0 and SIZE==2'),
            "Non-Family, Size=3"    : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==0 and SIZE==3'),
            "Non-Family, Size=4"    : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==0 and SIZE==4'),
            "Non-Family, Size=5"    : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==0 and SIZE==5'),
            "Non-Family, Size=6"    : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==0 and SIZE==6'),
            "Non-Family, Size=7+"   : HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES==0 and SIZE==7'),
        }
        return name, groupings

    @staticmethod
    def recodeHHsize():
        """
            [Official Metrics, 7a-c]
        """
        name = CC.HHSIZE_ALL
        groupings = {
            "1-person household": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 1'),
            "2-person household": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 2'),
            "3-person household": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 3'),
            "4-person household": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 4'),
            "5-person household": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 5'),
            "6-person household": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 6'),
            "7+-person household": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 7'),
        }
        return name, groupings

    @staticmethod
    def recodeDHCH_HHTYPE_P12_PART1():
        name = "dhch_hhtype_p12_part1"
        groupings = {
        "Married Couple HH with Own Children <18" : HHTypeDHCHAttr.get_grouping(expr=f'MARRIED==1 and OWN_CHILD_UNDER_18==1'),
        "Married Couple HH W/o Own Children <18" : HHTypeDHCHAttr.get_grouping(expr=f'MARRIED==1 and OWN_CHILD_UNDER_18==0'),
        "Cohabiting Couple HH with Own Children <18" : HHTypeDHCHAttr.get_grouping(expr=f'COHABITING==1 and OWN_CHILD_UNDER_18==1'),
        "Cohabiting Couple HH W/o Own Children <18" : HHTypeDHCHAttr.get_grouping(expr=f'COHABITING==1 and OWN_CHILD_UNDER_18==0'),
                    }
        return name, groupings

    @staticmethod
    def recodeDHCH_HHTYPE_P12_PART2():
        name = "dhch_hhtype_p12_part2"
        groupings = {
        "No Spouse/Partner, W/ Own Children under 18" : HHTypeDHCHAttr.get_grouping(expr=f'NO_SPOUSE_OR_PARTNER==1 ' +
                        f'and OWN_CHILD_UNDER_18==1'),
        "No Spouse/Partner, W/ Relatives, W/o Own Children under 18" : HHTypeDHCHAttr.get_grouping(expr=f'NO_SPOUSE_OR_PARTNER==1 ' +
                        f'and OWN_CHILD_UNDER_18==0 and WITH_RELATIVES==1'),
        "No Spouse/Partner, Not Alone, Only non-relatives present" : HHTypeDHCHAttr.get_grouping(expr=f'NO_SPOUSE_OR_PARTNER==1 ' +
                        f'and WITH_RELATIVES==0 and NOT_ALONE==1'),
                    }
        return name, groupings

    @staticmethod
    def recodeDHCH_HHTYPE_P12_PART3():
        name = "dhch_hhtype_p12_part3"
        groupings = {
        "Alone" : HHTypeDHCHAttr.get_grouping(expr=f'NOT_ALONE==0'),
                    }
        return name, groupings

    @staticmethod
    def recodeFamilyOther_DHCH_PCO3_MARGIN2():
        """Other (non-married-couple) family, w or w/o children (See PCO3)"""
        name = "dhch_pco3_margin2"
        groupings = {
            "Other family (not married couple)": HHTypeDHCHAttr.get_grouping(expr=
                        f'MARRIED == 0 and WITH_RELATIVES == 1'),
        }
        return name, groupings

    @staticmethod
    def recodeDHCH_P16_MARGIN():
        name = "dhch_p16_margin"
        groupings = {
            "Households with one or more people under 18:": HHTypeDHCHAttr.get_grouping(expr=f'CHILD_UNDER_18==1'),
            "Households with no people under 18:": HHTypeDHCHAttr.get_grouping(expr=f'CHILD_UNDER_18==0'),
        }
        return name, groupings

    @staticmethod
    def recodeSize1_Size2Plus():
        name = "size1_size2plus"
        groupings = {
            "Size 1": HHTypeDHCHAttr.get_grouping(expr=f'SIZE==1'),
            "Size 2+": HHTypeDHCHAttr.get_grouping(expr=f'SIZE>=2'),
                    }
        return name, groupings

    @staticmethod
    def recodeFamilyOther_PCO3_MARGIN1():
        name = "dhch_pco3_margin1"
        groupings = {
            "Married Couple Family": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 1'),
            "Other Family (Unmarried/No Spouse)": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and WITH_RELATIVES == 1'),
        }
        return name, groupings

    @staticmethod
    def recodeDHCH_PCO3_MARGIN3():
        name = "dhch_pco3_margin3"
        groupings = {
            "Family Households": HHTypeDHCHAttr.get_grouping(expr=f'WITH_RELATIVES == 1'),
            "Non-Family Households (2+/not alone)": HHTypeDHCHAttr.get_grouping(expr=f'NOT_ALONE == 1 and WITH_RELATIVES == 0'),
        }
        return name, groupings

    @staticmethod
    def recodeDHCH_HHTYPE_HCT3():
        name = "dhch_hhtype_hct3"
        groupings = {
            "With children under 6 only": HHTypeDHCHAttr.get_grouping(expr=f'CHILD_UNDER_6_ONLY_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER==1'),
            "With children 6-17 only": HHTypeDHCHAttr.get_grouping(expr=f'CHILD_BETWEEN_6_AND_17_EXCLUDING_HOUSEHOLDER_PARTNER==1'),
            "With children under 6 and 6-17": HHTypeDHCHAttr.get_grouping(expr=f'CHILD_IN_BOTH_RANGES_EXCLUDING_HOUSEHOLDER_PARTNER==1'),
            "No children under 18": HHTypeDHCHAttr.get_grouping(expr=f'CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER==0'),
        }
        return name, groupings

    @staticmethod
    def recodeMarriedFam_PCO11_P1():
        name = "hhtype_dhch_married_fam_pco11_p1"
        groupings = {
            "Married with no own children under 18 years"   : HHTypeDHCHAttr.get_grouping(expr=
                                                f'MARRIED == 1 and OWN_CHILD_UNDER_18 == 0'),
            "Married with own children under 6 only"        : HHTypeDHCHAttr.get_grouping(expr=
                                                f'MARRIED == 1 and OWN_CHILD_UNDER_6_ONLY == 1'),
            "Married with own children 6 to 17 years only": HHTypeDHCHAttr.get_grouping(expr=
                                                f'MARRIED == 1 and OWN_CHILD_BETWEEN_6_AND_17 == 1'),
            "Married with own children under 6 years and 6 to 17 years": HHTypeDHCHAttr.get_grouping(expr=
                                                f'MARRIED == 1 and OWN_CHILD_IN_BOTH_RANGES == 1'),
        }
        return name, groupings

    @staticmethod
    def recodePopSehsdTargetUPART():
        """HHTYPE part of Pop/Sehsd target for CEF UPART variable"""
        name = CC.POP_SEHSD_TARGET_UPART
        groupings = {
            "Same-sex Partner" : HHTypeDHCHAttr.get_grouping(expr=f'MARRIED_SAME_SEX==1 or COHABITING_SAME_SEX==1'),
            "Opposite-sex Partner" : HHTypeDHCHAttr.get_grouping(expr=f'MARRIED_OPPOSITE_SEX==1 or COHABITING_OPPOSITE_SEX==1'),
            "No partner" : HHTypeDHCHAttr.get_grouping(expr=f'NO_SPOUSE_OR_PARTNER==1'),
                    }
        return name, groupings

    @staticmethod
    def recodeSizeOnePerson():
        name = CC.HHSIZE_ONE
        groupings = {
            "1-person household": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 1')
        }
        return name, groupings

    @staticmethod
    def recodeSizeTwoPersons():
        name = CC.HHSIZE_TWO
        groupings = {
            "2-person household": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 2')
        }
        return name, groupings

    @staticmethod
    def recodeFamily():
        """Recode Family as married or has own child or with relatives."""
        name = CC.HHTYPE_FAMILY
        groupings = {
            "Family": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0 or OWN_CHILD_UNDER_18 != 0 or WITH_RELATIVES != 0')
        }
        return name, groupings

    @staticmethod
    def recodeNonfamily():
        """Recode non-family as not married and no own children and no relatives."""
        name = CC.HHTYPE_NONFAMILY
        groupings = {
            "Non-Family": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_UNDER_18 == 0 and WITH_RELATIVES == 0')
        }
        return name, groupings

    @staticmethod
    def recodeFamilyMarried():
        """Recode family married as just married."""
        name = CC.HHTYPE_FAMILY_MARRIED
        groupings = {
            "Married Family": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0')
        }
        return name, groupings

    @staticmethod
    def recodeFamilyMarriedWithChildrenIndicator():
        """Recode Family married with children indicator as grouping of married with own child and
        married without own child."""
        name = CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_INDICATOR
        groupings = {
            "Married with own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0 and OWN_CHILD_UNDER_18 != 0'),
            "Married without own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0 and OWN_CHILD_UNDER_18 == 0')
        }
        return name, groupings

    @staticmethod
    def recodeFamilyMarriedWithChildrenLevels():
        """Recode Family married with children levels as grouping of married with only own children under 6,
        married with own children only between 6 and 17 and married with own children in both groups."""
        name = CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_LEVELS
        groupings = {
            "Married with children under 6 only": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0 and OWN_CHILD_UNDER_6_ONLY != 0'),
            "Married with children 6 to 17 years only": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0 and OWN_CHILD_BETWEEN_6_AND_17 != 0'),
            "Married with children under 6 years and 6 to 17 years": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED != 0 and OWN_CHILD_IN_BOTH_RANGES != 0')
        }
        return name, groupings

    @staticmethod
    def recodeFamilyOther():
        """Recode family other as not married and either with cwn child or relatives."""
        name = CC.HHTYPE_FAMILY_OTHER
        groupings = {
            "Other Family": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and (OWN_CHILD_UNDER_18 != 0 or WITH_RELATIVES != 0)')
        }
        return name, groupings

    @staticmethod
    def recodeFamilyOtherWithChildrenIndicator():
        """Recode Family other with children indicator as grouping of not married with own child and
        not married without own child."""
        name = CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_INDICATOR
        groupings = {
            "Other with own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_UNDER_18 != 0'),
            "Other without own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_UNDER_18 == 0')
        }
        return name, groupings

    @staticmethod
    def recodeFamilyOtherWithChildrenLevels():
        """Recode Family other with children levels as grouping of not married with only own children under 6,
        not married with own children only between 6 and 17 and not married with own children in both groups."""
        name = CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_LEVELS
        groupings = {
            "Other with children under 6 only": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_UNDER_6_ONLY != 0'),
            "Other with children 6 to 17 years only": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_BETWEEN_6_AND_17 != 0',),
            "Other with children under 6 years and 6 to 17 years": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_IN_BOTH_RANGES != 0')
        }
        return name, groupings

    @staticmethod
    def recodeAlone():
        """Recode Along as not not alone."""
        name = CC.HHTYPE_ALONE
        groupings = {
            "Alone": HHTypeDHCHAttr.get_grouping(expr=f'NOT_ALONE == 0')
        }
        return name, groupings

    @staticmethod
    def recodeNotAlone():
        """Recode Not Alone as not along."""
        name = CC.HHTYPE_NOT_ALONE
        groupings = {
            "Not alone": HHTypeDHCHAttr.get_grouping(expr=f'NOT_ALONE != 0')
        }
        return name, groupings

    @staticmethod
    def recodeNonfamilyNotAlone():
        """Recode non-family not alone as not not alone and not married and no own children and no relatives."""
        name = CC.HHTYPE_NONFAMILY_NOT_ALONE
        groupings = {
            "Non-Family not alone": HHTypeDHCHAttr.get_grouping(expr=f'NOT_ALONE != 0 and MARRIED == 0 and OWN_CHILD_UNDER_18 == 0 and WITH_RELATIVES == 0')
        }
        return name, groupings

    @staticmethod
    def recodeNotSizeTwo():
        """Recode not size two as size not equal two."""
        name = CC.HHTYPE_NOT_SIZE_TWO
        groupings = {
            "Not size two": HHTypeDHCHAttr.get_grouping(expr=f'SIZE != 2')
        }
        return name, groupings

    @staticmethod
    def recodeChildAlone():
        name = CC.HHTYPE_CHILD_ALONE
        groupings = {
            "Child alone": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 1 and CHILD_UNDER_18 == 1')
        }
        return name, groupings

    @staticmethod
    def recodeSizeTwoWithChild():
        """Recode Size two with child as size equals two and either has own child or co-habitat has child or child. This includes a child householder and other person not a child"""
        name = CC.HHTYPE_SIZE_TWO_WITH_CHILD
        groupings = {
            "Size two with child": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 2 and (OWN_CHILD_UNDER_18 != 0 or CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER != 0 or CHILD_UNDER_18 != 0)')
        }
        return name, groupings

    @staticmethod
    def recodeSizeTwoWithChildExclHH():
        """Recode Size two with child as size equals two and has a child excluding householder"""
        name = CC.HHTYPE_SIZE_TWO_WITH_CHILD_EXCLUDING_HH
        groupings = {
            "Size two with child": HHTypeDHCHAttr.get_grouping(
                expr=f'SIZE == 2 and (OWN_CHILD_UNDER_18 != 0 or CHILD_UNDER_18_EXCLUDING_HOUSEHOLDER_SPOUSE_PARTNER != 0)')
        }
        return name, groupings

    @staticmethod
    def recodeSizeTwoCouple():
        name = CC.HHTYPE_SIZE_TWO_COUPLE
        groupings = {
            "Size 2, householder with spouse/unmarried partner": HHTypeDHCHAttr.get_grouping(expr=f'SIZE == 2 and (MARRIED != 0 or COHABITING != 0)')
        }
        return name, groupings

    @staticmethod
    def recodeOwnChildrenUnderSix():
        name = CC.HHTYPE_OWNCHILD_UNDERSIX
        groupings = {
            "Householder has own child under 6": HHTypeDHCHAttr.get_grouping(expr=f'OWN_CHILD_UNDER_6_ONLY != 0 or OWN_CHILD_IN_BOTH_RANGES != 0')
        }
        return name, groupings

    @staticmethod
    def recodeOwnChildUnder18():
        name = CC.HHTYPE_OWNCHILD_UNDER18
        groupings = {
            "Householder has own child under 18": HHTypeDHCHAttr.get_grouping(expr=f'OWN_CHILD_UNDER_18 != 0')
        }
        return name, groupings

    @staticmethod
    def recodeSizeThreeWithTwoChildren():
        """ Two children (can be known for sure only if in both ranges) and no spouse or partner, size=3. Only one HHTYPE, 436."""
        name = CC.HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN
        groupings = {
            "Size 3, two children": HHTypeDHCHAttr.get_grouping(expr=f'SIZE==3 and OWN_CHILD_IN_BOTH_RANGES != 0')
        }
        return name, groupings

    @staticmethod
    def recodeSizeThreeCoupleWithOneChild():
        name = CC.HHTYPE_SIZE_THREE_COUPLE_WITH_ONE_CHILD
        groupings = {
            # "Size 3, couple with one child": [0, 17, 86, 103, 172, 189, 282, 299]  # 8 HHTYPES, like with the old HHTYPE attribute
            "Size 3, couple with one child": HHTypeDHCHAttr.get_grouping(expr=f'NO_SPOUSE_OR_PARTNER == 0 and SIZE == 3 and OWN_CHILD_UNDER_18 != 0')
        }
        return name, groupings

    @staticmethod
    def recodeSizeFourCoupleWithTwoChildren():
        name = CC.HHTYPE_SIZE_FOUR_COUPLE_WITH_TWO_CHILDREN
        groupings = {
            "Size 4, couple with two children": HHTypeDHCHAttr.get_grouping(expr=f'SIZE==4 and OWN_CHILD_IN_BOTH_RANGES != 0 and NO_SPOUSE_OR_PARTNER == 0')
        }
        # Like in the old HHTYPE only 4 types: [34, 120, 206, 316]
        return name, groupings

    @staticmethod
    def recodeFamilyOtherWithChildrenIndicator2():
        """Recode Family other with children indicator as grouping of not married with own child and
        not married without own child."""
        name = "other_with_children_indicator_2"
        groupings = {
            "Other with own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_UNDER_18 != 0'),
            "Other without own children under 18": HHTypeDHCHAttr.get_grouping(expr=f'MARRIED == 0 and OWN_CHILD_UNDER_18 == 0 and WITH_RELATIVES == 1')
        }
        return name, groupings