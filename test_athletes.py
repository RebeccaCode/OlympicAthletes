import unittest
from athletes import Athletes
from criterion import Criterion, ComparisonOperators, SpecialValues


class TestAthletes(unittest.TestCase):

    athlete_5_1994_500m = \
        {'CompositeKey': '5|NED|1994|Speed Skating|Speed Skating Women\'s 500 metres',
         'ID': '5',
         'Name': 'Christine Jacoba Aaftink',
         'Sex': 'F',
         'Age': '27',
         'Height': '185',
         'Weight': '82',
         'Team': 'Netherlands',
         'NOC': 'NED',
         'Games': '1994 Winter',
         'Year': '1994',
         'Season': 'Winter',
         'City': 'Lillehammer',
         'Sport': 'Speed Skating',
         'Event': 'Speed Skating Women\'s 500 metres',
         'Medal': 'NA'}

    athlete_20_2002_gold = \
        {'CompositeKey': '20|NOR|2002|Alpine Skiing|Alpine Skiing Men\'s Super G',
         'ID': '20',
         'Name': 'Kjetil Andr Aamodt',
         'Sex': 'M',
         'Age': '30',
         'Height': '176',
         'Weight': '85',
         'Team': 'Norway',
         'NOC': 'NOR',
         'Games': '2002 Winter',
         'Year': '2002',
         'Season': 'Winter',
         'City': 'Salt Lake City',
         'Sport': 'Alpine Skiing',
         'Event': 'Alpine Skiing Men\'s Super G',
         'Medal': 'Gold'}

    athlete_2359_2008_gold = \
        {'CompositeKey': '2359|RUS|2008|Wrestling|Wrestling Men\'s Lightweight, Greco-Roman',
         'ID': '2359',
         'Name': 'Islam-Beka Said-Tsilimovich Albiyev',
         'Sex': 'M',
         'Age': '19',
         'Height': '165',
         'Weight': '66',
         'Team': 'Russia',
         'NOC': 'RUS',
         'Games': '2008 Summer',
         'Year': '2008',
         'Season': 'Summer',
         'City': 'Beijing',
         'Sport': 'Wrestling',
         'Event': 'Wrestling Men\'s Lightweight, Greco-Roman',
         'Medal': 'Gold'}

    test_athletes = [athlete_5_1994_500m, athlete_20_2002_gold, athlete_2359_2008_gold]

    def test_blank_athlete_criteria(self):
        result = Athletes().get_blank_map_criteria()
        self.assertIsInstance(result, dict)

        for key in self.athlete_5_1994_500m.keys():
            self.assertIsNone(result[key].value)

    def test_get_by_criterion(self):
        for test_athlete in self.test_athletes:
            query = {}

            name_criterion = Criterion(ComparisonOperators.equals, test_athlete['Name'])
            query['Name'] = name_criterion

            age_criterion = Criterion(ComparisonOperators.greater_than, 100)
            query['Age'] = age_criterion

            games_criterion = Criterion(ComparisonOperators.equals, test_athlete['Games'])
            query['Games'] = games_criterion

            event_criterion = Criterion(ComparisonOperators.contains, test_athlete['Event'][3:len(test_athlete['Event'])-1])
            query['Event'] = event_criterion

            for result in Athletes().get_by_map_criteria(query):
                print(result)
                self.assertIsInstance(result, dict)
                self.assertDictEqual(result, test_athlete)
