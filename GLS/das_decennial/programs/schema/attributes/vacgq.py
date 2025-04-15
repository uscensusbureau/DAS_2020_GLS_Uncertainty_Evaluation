"""
Class stacking attributes Vacancy and HHGQ.
Or, HHGQ expanded into more levels:
 (vacant) is expanded to types of vacancy
"Not vacant" and "Not a GQ" levels are, therefore, removed.
"""

from GLS.das_decennial.programs.schema.attributes.abstractattribute import AbstractAttribute
from GLS.das_decennial.programs.schema.attributes.hhgq import HHGQAttr
from GLS.das_decennial.programs.schema.attributes.vacs import VacancyAttr
from GLS.das_decennial.das_constants import CC


class VacancyGQAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_VACGQ

    @staticmethod
    def getVGQLevels():
        """ Get levels of the Vacancy and HHGQ attributes, which are being stacked to produce this one"""
        vacs_levels = [key for key in VacancyAttr.getLevels()][1:]  # Not getting level 0 which is "Non Vacant", those are accounted for by Tenure and HHGQ
        gq_levels = [key for key in HHGQAttr.getLevels()][1:]  # Not getting level 0 which is "Household", those are accounted for by Tenure
        return gq_levels, vacs_levels

    @staticmethod
    def getLevels():
        gq, vac = VacancyGQAttr.getVGQLevels()

        keys = ["Household"] + vac + gq

        return {key: [num] for num, key in enumerate(keys)}

        # # The following is produced by running the above
        # return {
        #  'Household': [0],
        #  'Vacant, for rent': [1],
        #  'Vacant, rented, not occupied': [2],
        #  'Vacant, for sale only Pay': [3],
        #  'Vacant, sold, not occupied': [4],
        #  'Vacant, for seasonal, recreational, or occasional use': [5],
        #  'Vacant, for migrant workers': [6],
        #  'Vacant, other': [7],
        #  'GQ 101 Federal detention centers': [8],
        #  'GQ 102 Federal prisons': [9],
        #  'GQ 103 State prisons': [10],
        #  'GQ 104 Local jails and other municipal confinements': [11],
        #  'GQ 105 Correctional residential facilities': [12],
        #  'GQ 106 Military disciplinary barracks': [13],
        #  'GQ 201 Group homes for juveniles': [14],
        #  'GQ 202 Residential treatment centers for juveniles': [15],
        #  'GQ 203 Correctional facilities intended for juveniles': [16],
        #  'GQ 301 Nursing facilities': [17],
        #  'GQ 401 Mental hospitals': [18],
        #  'GQ 402 Hospitals with patients who have no usual home elsewhere': [19],
        #  'GQ 403 In-patient hospice facilities': [20],
        #  'GQ 404 Military treatment facilities': [21],
        #  'GQ 405 Residential schools for people with disabilities': [22],
        #  'GQ 501 College/university student housing': [23],
        #  'GQ 601 Military quarters': [24],
        #  'GQ 602 Military ships': [25],
        #  'GQ 701 Emergency and transitional shelters': [26],
        #  'GQ 801 Group homes intended for adults': [27],
        #  'GQ 802 Residential treatment centers for adults': [28],
        #  'GQ 900 Maritime/merchant vessels': [29],
        #  "GQ 901 Workers' group living quarters and job corps centers": [30],
        #  'GQ 997 Other noninstitutional (GQ types 702, 704, 706, 903, 904)': [31]
        # }

    @staticmethod
    def recodeHhgqSpineTypes():
        """
            The currently used GQ types in the geographic spine optimization routines are the 7 major GQ types.
        """
        # This query is for using when defining geospine, not as DPQuery, so it has impact gap
        #  (asking about only GQs doesn't make use of parallel composition)
        name = CC.HHGQ_SPINE_TYPES
        groupings = {
            'GQ 101-106 Correctional facilities for adults' : list(range(8, 13+1)),
            'GQ 201-203 Juvenile facilities' : list(range(14, 16+1)),
            'GQ 301 Nursing facilities/Skilled-nursing facilities' : [17],
            'GQ 401-405 Other institutional facilities' : list(range(18, 22+1)),
            'GQ 501 College/University student housing' : [23],
            'GQ 601-602 Military quarters' : [24, 25],
            'GQ 701-702, 704, 706, 801-802, 900-901, 903-904 Other noninstitutional facilities' : list(range(26, 31+1))
        }
        return name, groupings
    # ('hhgqSpineTypes',
    #  {'GQ 101-106 Correctional facilities for adults': [8, 9, 10, 11, 12, 13],
    #   'GQ 201-203 Juvenile facilities': [14, 15, 16],
    #   'GQ 301 Nursing facilities/Skilled-nursing facilities': [17],
    #   'GQ 401-405 Other institutional facilities': [18, 19, 20, 21, 22],
    #   'GQ 501 College/University student housing': [23],
    #   'GQ 601-602 Military quarters': [24, 25],
    #   'GQ 701-702, 704, 706, 801-802, 900-901, 903-904 Other noninstitutional facilities': [26, 27, 28, 29, 30, 31]})

    @staticmethod
    def recodeVacant():
        """
        returns number of vacant units
        """
        name = CC.VACANT
        # groups = {
        #      "Household-or-GQ": [0] + list(range(1, 31+1)),
        #     "Vacant": list(range(1, 8))
        # }
        gq, vac = VacancyGQAttr.getVGQLevels()
        groups = {
            "Household-or-GQ": [0] + list(range(len(vac) + 1, len(vac) + len(gq) + 1)),
            "Vacant": list(range(1, len(vac) + 1))  # Start from where household levels end, end where vacancy levels end
        }
        return name, groups
        #  ('vacant',
        #  {'Household-or-GQ': [0, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31],
        #   'Vacant': [1, 2, 3, 4, 5, 6, 7]})

    @staticmethod
    def recodeHouseholds():
        """
        returns number of household units
        """
        # This query is for using in constraints, not DPQueries, so it has impact gap (asking about only households doesn't make use of parallel composition)
        name = CC.HHGQ_HOUSEHOLD_TOTAL
        # gq, vac = VacancyGQAttr.getVGQLevels()
        groups = {
            "Households": [0],
            #"Vacant-or-GQ": list(range(1, len(vac)))
        }
        return name, groups
        # ('householdTotal', {'Households': [0]})

    @staticmethod
    def recodeVacancyLevels():
        """
        Returns number of owned units (owned or mortgage) and number of not owned (rented or no pay)
        """
        name = CC.ATTR_VACS
        gq, vac = VacancyGQAttr.getVGQLevels()
        groups = {k: v for k, v in VacancyGQAttr.getLevels().items() if 1 <= v[0] < 1 + len(vac)}
        groups.update({"Household-or-GQ": [0] + list(range(len(vac) + 1, len(vac) + len(gq) + 1))})
        return name, groups
        # ('vacs',
        #  {'Vacant, for rent': [1],
        #   'Vacant, rented, not occupied': [2],
        #   'Vacant, for sale only Pay': [3],
        #   'Vacant, sold, not occupied': [4],
        #   'Vacant, for seasonal, recreational, or occasional use': [5],
        #   'Vacant, for migrant workers': [6],
        #   'Vacant, other': [7],
        #   'Household-or-GQ': [0, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]})

    @staticmethod
    def recodeHousingUnit():
        name = CC.HHGQ_UNIT_HOUSING
        # This query is for using in invariants/constraints and has impact gap
        # groupings = {
        #     "Housing Units": list(range(7))
        # }
        gq, vac = VacancyGQAttr.getVGQLevels()
        groupings = {
            "Housing Units": list(range(1 + len(vac))),
        }
        return name, groupings
        # ('housing_units', {'Housing Units': [0, 1, 2, 3, 4, 5, 6, 7]})

    @staticmethod
    def recodeTotalGQ():
        name = CC.HHGQ_UNIT_TOTALGQ
        # This query is for using in invariants/constraints and has impact gap
        # groupings = {
        #     "Group Quarters": list(range(8, 32))
        # }
        gq, vac = VacancyGQAttr.getVGQLevels()
        groupings = {
            "Group Quarters": list(range(1 + len(vac), 1 + len(vac) + len(gq)))
        }
        return name, groupings
        # ('total_gq',
        #  {'Group Quarters': [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]})

    @staticmethod
    def recodeHhgqVector():
        name = CC.HHGQ_UNIT_VECTOR
        groupings = VacancyGQAttr.getGqVectGropings()
        # This query is for using in invariants/constraints (even though it doesn't have an impact gap)
        return name, groupings
        # ('hhgq_vector',
        #  {'Housing Units': [0, 1, 2, 3, 4, 5, 6, 7],
        #   'GQ 101 Federal detention centers': [8],
        #   'GQ 102 Federal prisons': [9],
        #   'GQ 103 State prisons': [10],
        #   'GQ 104 Local jails and other municipal confinements': [11],
        #   'GQ 105 Correctional residential facilities': [12],
        #   'GQ 106 Military disciplinary barracks': [13],
        #   'GQ 201 Group homes for juveniles': [14],
        #   'GQ 202 Residential treatment centers for juveniles': [15],
        #   'GQ 203 Correctional facilities intended for juveniles': [16],
        #   'GQ 301 Nursing facilities': [17],
        #   'GQ 401 Mental hospitals': [18],
        #   'GQ 402 Hospitals with patients who have no usual home elsewhere': [19],
        #   'GQ 403 In-patient hospice facilities': [20],
        #   'GQ 404 Military treatment facilities': [21],
        #   'GQ 405 Residential schools for people with disabilities': [22],
        #   'GQ 501 College/university student housing': [23],
        #   'GQ 601 Military quarters': [24],
        #   'GQ 602 Military ships': [25],
        #   'GQ 701 Emergency and transitional shelters': [26],
        #   'GQ 801 Group homes intended for adults': [27],
        #   'GQ 802 Residential treatment centers for adults': [28],
        #   'GQ 900 Maritime/merchant vessels': [29],
        #   "GQ 901 Workers' group living quarters and job corps centers": [30],
        #   'GQ 997 Other noninstitutional (GQ types 702, 704, 706, 903, 904)': [31]})

    @staticmethod
    def getGqVectGropings():
        gq, vac = VacancyGQAttr.getVGQLevels()
        groupings = {
            "Housing Units": list(range(1 + len(vac))),
        }
        groupings.update({k: v for k, v in VacancyGQAttr.getLevels().items() if v[0] >= 1 + len(vac)})
        return groupings
        # {'Housing Units': [0, 1, 2, 3, 4, 5, 6, 7],
        #  'GQ 101 Federal detention centers': [8],
        #  'GQ 102 Federal prisons': [9],
        #  'GQ 103 State prisons': [10],
        #  'GQ 104 Local jails and other municipal confinements': [11],
        #  'GQ 105 Correctional residential facilities': [12],
        #  'GQ 106 Military disciplinary barracks': [13],
        #  'GQ 201 Group homes for juveniles': [14],
        #  'GQ 202 Residential treatment centers for juveniles': [15],
        #  'GQ 203 Correctional facilities intended for juveniles': [16],
        #  'GQ 301 Nursing facilities': [17],
        #  'GQ 401 Mental hospitals': [18],
        #  'GQ 402 Hospitals with patients who have no usual home elsewhere': [19],
        #  'GQ 403 In-patient hospice facilities': [20],
        #  'GQ 404 Military treatment facilities': [21],
        #  'GQ 405 Residential schools for people with disabilities': [22],
        #  'GQ 501 College/university student housing': [23],
        #  'GQ 601 Military quarters': [24],
        #  'GQ 602 Military ships': [25],
        #  'GQ 701 Emergency and transitional shelters': [26],
        #  'GQ 801 Group homes intended for adults': [27],
        #  'GQ 802 Residential treatment centers for adults': [28],
        #  'GQ 900 Maritime/merchant vessels': [29],
        #  "GQ 901 Workers' group living quarters and job corps centers": [30],
        #  'GQ 997 Other noninstitutional (GQ types 702, 704, 706, 903, 904)': [31]}

    @staticmethod
    def cef2das(tenure, vac, gqtype):
        g, v = VacancyGQAttr.getVGQLevels()
        if gqtype == "000" or gqtype == "   ":
            if int(vac) == 0:
                if int(tenure) == 0:
                    raise ValueError("Tenure not set for non-vacant unit")
                return 0  # Household / Occupied housing unit
            return int(vac)  # vacancy types from 1 to 7 just like in CEF, no shifting

        try:
            return HHGQAttr.reader_recode[gqtype] + len(v)
        except KeyError:
            raise ValueError(f"GQTYPE value of '{gqtype}' is not supported in {HHGQAttr.getName()} hhgq reader recode", gqtype)
