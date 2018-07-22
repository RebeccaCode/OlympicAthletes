class ComparisonOperators:
    equals = 'Equals'
    starts_with = 'Starts with'
    contains = 'Contains'
    less_than = 'Less than'
    greater_than = 'Greater than'


class SpecialValues:
    ignore = None
    not_available = 'NA'


class Criterion(object):

    def __init__(self, operator=None, value=None):
        self.operator = ComparisonOperators.equals if operator is None else operator
        self.value = SpecialValues.ignore if value is None else value

    def compare(self, value):
        if SpecialValues.ignore == self.value:
            return True
        elif self.operator == ComparisonOperators.equals:
            return self.value == value
        elif self.operator == ComparisonOperators.starts_with:
            return value.find(self.value) == 0
        elif self.operator == ComparisonOperators.contains:
            where_is = value.find(self.value)
            return where_is > -1
        elif self.operator == ComparisonOperators.less_than:
            if isinstance(self.value, str) and not str.isnumeric(self.value):
                return False
            else:
                return float(self.value) < float(value)
        elif self.operator == ComparisonOperators.greater_than:
            if isinstance(self.value, str) and not str.isnumeric(self.value):
                return False
            else:
                return float(self.value) > float(value)
        else:
            return False


