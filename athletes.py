import csv
from criterion import Criterion, ComparisonOperators

class Athletes():
    __default_file_name = 'data/athlete_events.csv'

    def __init__(self):
        self.blank_athlete_criteria = {'CompositeKey': Criterion(ComparisonOperators.equals),
                                       'ID': Criterion(ComparisonOperators.equals),
                                       'Name': Criterion(ComparisonOperators.equals),
                                       'Sex': Criterion(ComparisonOperators.equals),
                                       'Age': Criterion(ComparisonOperators.equals),
                                       'Height': Criterion(ComparisonOperators.equals),
                                       'Weight': Criterion(ComparisonOperators.equals),
                                       'Team': Criterion(ComparisonOperators.equals),
                                       'NOC': Criterion(ComparisonOperators.equals),
                                       'Games': Criterion(ComparisonOperators.equals),
                                       'Year': Criterion(ComparisonOperators.equals),
                                       'Season': Criterion(ComparisonOperators.equals),
                                       'City': Criterion(ComparisonOperators.equals),
                                       'Sport': Criterion(ComparisonOperators.equals),
                                       'Event': Criterion(ComparisonOperators.equals),
                                       'Medal': Criterion(ComparisonOperators.equals)}

    def __get_file_data(self, filename):
        with open(filename) as csv_file:
            reader = csv.reader(csv_file, delimiter=',', quotechar='"')

            # skip header record
            next(reader)

            for row in reader:
                composite_key = '{}|{}|{}|{}|{}'.format(row[0], row[7], row[9], row[12], row[13])

                item = {'CompositeKey': composite_key,
                        'ID': row[0],
                        'Name': row[1],
                        'Sex': row[2],
                        'Age': row[3],
                        'Height': row[4],
                        'Weight': row[5],
                        'Team': row[6],
                        'NOC': row[7],
                        'Games': row[8],
                        'Year': row[9],
                        'Season': row[10],
                        'City': row[11],
                        'Sport': row[12],
                        'Event': row[13],
                        'Medal': row[14]}
                yield item

    def get_blank_map_criteria(self):
        return self.blank_athlete_criteria

    def get_by_map_criteria(self, query):
        assert isinstance(query, dict)
        data = self.__get_file_data(self.__default_file_name)
        for record in data:
            matches = False
            for query_key in query.keys():
                #skip query keys with value None
                if query[query_key] is None:
                    continue

                if not query[query_key].compare(record[query_key]):
                    matches = False
                    break
                else:
                    matches = True

            if matches:
                yield record