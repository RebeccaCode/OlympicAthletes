import unittest
from criterion import Criterion, ComparisonOperators, SpecialValues


class TestCriterion(unittest.TestCase):

    def test_ignore(self):
        criterion = Criterion(ComparisonOperators.equals, SpecialValues.ignore)

        test_value = 'apple'

        self.assertTrue(criterion.compare(test_value))

    def test_equals_numeric(self):
        criterion = Criterion(ComparisonOperators.equals, 9)

        test_value = criterion.value

        self.assertTrue(criterion.compare(test_value))
        self.assertFalse(criterion.compare(test_value*2))

    def test_equals_alpha(self):
        criterion = Criterion(ComparisonOperators.equals, 'a')

        test_value = criterion.value

        self.assertTrue(criterion.compare(test_value))
        self.assertFalse(criterion.compare(test_value.upper()))
        self.assertFalse(criterion.compare(SpecialValues.not_available))

    def test_equals_not_available(self):
        criterion = Criterion(ComparisonOperators.equals, SpecialValues.not_available)

        test_value = criterion.value

        self.assertTrue(criterion.compare(test_value))
        self.assertFalse(criterion.compare('|'))

    def test_starts_with(self):
        criterion = Criterion(ComparisonOperators.starts_with, 'An apple')

        test_value = 'An apple a day keeps the doctor away.'

        self.assertTrue(criterion.compare(test_value))
        self.assertFalse(criterion.compare('test_starts_with'))

    def test_contains(self):
        criterion = Criterion(ComparisonOperators.contains, 'apple a day')

        test_value = 'An apple a day keeps the doctor away.'

        self.assertTrue(criterion.compare(test_value))
        self.assertFalse(criterion.compare('test_starts_with'))

    def test_less_than_numeric(self):
        criterion = Criterion(ComparisonOperators.less_than, 9)

        test_value = criterion.value + 10

        self.assertTrue(criterion.compare(test_value))
        self.assertFalse(criterion.compare(criterion.value))

    def test_less_than_alpha(self):
        criterion = Criterion(ComparisonOperators.less_than, '9')

        test_value = '19'

        self.assertTrue(criterion.compare(test_value))
        self.assertFalse(criterion.compare(criterion.value))

    def test_greater_than_numeric(self):
        criterion = Criterion(ComparisonOperators.greater_than, 9)

        test_value = criterion.value - 10

        self.assertTrue(criterion.compare(test_value))
        self.assertFalse(criterion.compare(criterion.value))

    def test_greater_than_alpha(self):
        criterion = Criterion(ComparisonOperators.greater_than, '090')

        test_value = '10'

        self.assertTrue(criterion.compare(test_value))
        self.assertFalse(criterion.compare(criterion.value))
