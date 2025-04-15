from itertools import product
import numpy as np
import json

#################
# Cenrace class
#################
class Cenrace():
    """
    Creates and houses dictionaries for accessing the CENRACE recodes
    Notes:
        number of categories is 2^(number of labels) - 1 because, by definition,
        no person is allowed to respond as having no race
    """

    def __init__(self, race_labels=["white", "black", "aian", "asian", "nhopi", "sor"]):
        self.race_labels = race_labels
        self.num_categories = (2 ** len(self.race_labels)) - 1
        self.race_dict = self.buildRaceDict(self.race_labels, reverse_keyval=False)
        self.rev_race_dict = self.buildRaceDict(self.race_labels, reverse_keyval=True)
        
        self.race_categories = self.getRaceCategories(racesOnly=False)
        
    
    def __repr__(self):
        items = [ "Cenrace",
                  "Race Labels:     {}".format(self.race_labels),
                  "Race Categories: {}".format("\n                 ".join([str(rc) for rc in self.race_categories])) ]
        
        return "\n".join(items)
    
    def getCenrace(self, key):
        """
        Converts either a string of 1s and 0s or an index to a hyphenated series of race labels.
        :param key: In the case that this is a string, it will be treated as a series of 1s and 0s, where each 1
            represents the inclusion of the associated race label. If this is an integer, it'll be used as an index for
            the rev_race_dict which pairs indices with these 1 and 0 strings.
        :return: A single string, listing each included race, separated with hyphens.
        """
        assert isinstance(key, str) or isinstance(key, int)
        
        if isinstance(key, str):
            return self.stringToCenrace(key, self.race_labels)
        else: # type is int
            return self.stringToCenrace(self.rev_race_dict[key], self.race_labels)
    
    def translateKey(self, key):
        """
        Translates between strings of 1s and 0s and the indices of those strings via the race_dict and rev_race_dict.
        :param key: Either a string of 1s and 0s, or an integer.
        :return: The value corresponding to the key in the other format.
        """
        assert isinstance(key, str) or isinstance(key, int)
        
        if isinstance(key, str):
            return self.race_dict[key]
        else: # type is int
            return self.rev_race_dict[key]
    
    def stringToCenrace(self, string, labels):
        """
        Converts a string of 1s and 0s to a hyphenated series of race labels.
        :param string: String containing 1s and 0s, where 1s indicate the inclusion of that race label.
        :param labels: List of race labels to convert string through.
        :return: A single string, listing each included race, separated with hyphens.
        """
        return "-".join([labels[i] for i,x in enumerate(string) if int(x) == 1])
    
    def buildRaceDict(self, race_labels, reverse_keyval=False):
        """
        Modified from William's recode function(s)
        Creates a mapping between an integer index, and strings of 1s and 0s representing all possible race label
            combinations.
        :param race_labels: A list of all race labels. Only the length is relevant.
        :param reverse_keyval: If true, the values of the mapping will be the strings, and not the integer index.
        :return: A mapping of string permutations of 1s and 0s and an integer index.
        """
        race_map = ["".join(x) for x in product("10", repeat=len(race_labels))]
        race_map.sort(key=lambda s: s.count("1"))
        race_dict = {}
        
        for i,k in enumerate(race_map[1:]):
            ind = i + 1
            if reverse_keyval:
                race_dict[ind] = k
            else:
                race_dict[k] = ind
        
        return race_dict
    
    def getCenraceDict(self, reverse_map=False):
        """
        Returns a mapping between integer indices and strings of 1s and 0s representing all possible race combinations.
        :param reverse_map: Returns a mapping where the integer indices are the index, instead of the inverse.
        :return: Mapping between integer index and race permutations.
        """
        if reverse_map:
            return self.race_dict
        else:
            return self.rev_race_dict
    
    def getRaceCategories(self, racesOnly=True):
        """
        Returns a list of all race combinations, each a string of labels separated by hyphens.
        :param racesOnly: If false, each item in the list will be a tuple of the index and the string.
        :return: List of all race categories.
        """
        if racesOnly:
            return [self.getCenrace(i+1) for i in range(self.num_categories)]
        else:
            return [(i+1, self.getCenrace(i+1)) for i in range(self.num_categories)]
    
    def getRace(self, keys):
        races = []
        for key in keys:
            race = [x for x in self.race_categories if x[0] == key or x[1] == key]
            if len(race) > 0:
                races.append(race)
        
        return races
    
    def toDict(self, reversed=False):
        if reversed:
            return dict([(x[1], x[0]) for x in self.race_categories])
        else:
            return dict(self.race_categories)
    
    def toJSON(self):
        """
        :return: Race Labels and Race Categories in json string format.
        """
        jsondict = { "Race Labels": self.race_labels,
                     "Race Categories": self.race_categories }
        return json.dumps(jsondict)


def getCenraceLevels(race_labels=["white", "black", "aian", "asian", "nhopi", "sor"]):
    """
    Returns all possible race combinations, each as a hyphenated string.
    :param race_labels: List of all races.
    :return: All possible race combinations.
    """
    return Cenrace(race_labels=race_labels).getRaceCategories()

def getCenraceNumberGroups(race_labels=["white", "black", "aian", "asian", "nhopi", "sor"]):
    """
    Return an object containing all race combinations and their indices grouped by length of combination.
    :param race_labels: List of all races.
    :return: CenraceGroup containing all race combinations organized by combination length.
    """
    categories = Cenrace(race_labels=race_labels).getRaceCategories()
    lengths = [len(x.split("-")) for x in categories]
    lengthset = list(set(lengths))
    
    groups = []
    for length in lengthset:
        label = "{} Race".format(length) if length == 1 else "{} Races".format(length)
        indices = [index for index,leng in enumerate(lengths) if leng == length]
        cats = np.array(categories)[indices].tolist()
        groups.append((label, indices, cats))
    
    group_names, group_indices, group_categories = [list(x) for x in zip(*groups)]
    
    group = CenraceGroup(group_names, group_indices, group_categories)
    
    return group


def getCenraceAloneOrInCombinationGroups(race_labels=["white", "black", "aian", "asian", "nhopi", "sor"]):
    """
    Returns an object containing all race combinations and their indices grouped by the inclusion of each race.
    :param race_labels: List of all races.
    :return: CenraceGroup containing all race combinations organized by inclusion of each race.
    """
    levels = getCenraceLevels(race_labels=race_labels)
    labels = []
    indices = []
    categories = []
    for race in race_labels:
        labels.append(f"{race} alone or in combination with one or more other races")
        items = [(i,x) for i,x in enumerate(levels) if race in x.split("-")]
        ind, cat = [list(x) for x in zip(*items)]
        indices.append(ind)
        categories.append(cat)
    
    group = CenraceGroup(labels, indices, categories)
    
    return group



class CenraceGroup():
    def __init__(self, group_names, group_indices, group_categories):
        self.group_names = group_names
        self.group_indices = group_indices
        self.group_categories = group_categories
    
    def getGroupLabels(self):
        return self.group_names
    
    def getIndexGroups(self):
        return self.group_indices
    
    def getCategories(self):
        return self.group_categories
