import boto3
import csv
import io
import json
import time

max_records = 12
bucket_name = 'rebeccacode-repo-a1'
source_data_file_name = 'athlete_events.csv'


def lambda_handler(event, context):

    request_id = event['request_id']
    criteria = Criteria(request_id).get_criteria(request_id)
    result = Parser(request_id, criteria).parse()

    return result


class Criteria:
    def __init__(self, request_id):
        self.criteria_file_name = '{}-olympic-data-request.json'.format(request_id)

    def get_criteria(self, request_id):
        criteria_str = ''
        try:
            for data in S3ObjectOperators.yield_from_object(bucket_name, self.criteria_file_name):
                criteria_str = '{}{}'.format(criteria_str, data)
        except Exception as e:
            print(e)
            raise e

        criteria = eval(criteria_str)

        return criteria['criteria']  # only want to return the content of the JSON criteria object


class Parser:
    def __init__(self, request_id, criteria):
        self.criteria_file_name = '{}-olympic-data-request.json'.format(request_id)
        self.result_summary_file_name = '{}-olympic-data-request-result-summary.json'.format(request_id)
        self.result_set_file_name = '{}-olympic-data-request-result-set.json'.format(request_id)
        self.criteria = criteria

    def parse(self):
        s3_client = boto3.client('s3')

        result_set = {'result_set': []}
        result_summary = {'result_set_count': 0, 'result_set_date': '', 'result_set': ''}
        result = {'result_set_date': '', 'result_summary_url': '', 'result_set_url': ''}

        try:
            for record in self.__yield_matching_records():
                result_set['result_set'].append(record)

            s3_client.put_object(Bucket=bucket_name,
                                 Key=self.result_set_file_name,
                                 Body=json.dumps(result_set))

            result_summary['result_set_date'] = time.strftime('%Y/%m/%d %H:%M:%S', time.gmtime())
            result_summary['count'] = len(result_set)
            result_summary['result_set'] = s3_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': self.result_set_file_name
                }
            )

            s3_client.put_object(Bucket=bucket_name,
                                 Key=self.result_summary_file_name,
                                 Body=json.dumps(result_summary))

            result['result_set_date'] = result_summary['result_set_date']

            result['result_set_url'] = result_summary['result_set']

            result['result_summary_url'] = s3_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': self.result_summary_file_name
                }
            )
        except Exception as e:
            print(e)
            raise e

        return result

    def __yield_matching_records(self):
        for row in S3ObjectOperators.yield_csv_from_object(bucket_name, source_data_file_name):
            record = Parser.__athlete_from_csv_row(row)
            match = False
            for criterion in self.criteria:
                # skip query keys with value None
                if criterion['value'] is None:
                    continue

                match = Parser.__compare(criterion['operator'], record[criterion['attribute']], criterion['value'])

                if not match:
                    break

            if match:
                yield record

    @staticmethod
    def __athlete_from_csv_row(row):
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
        return item

    @staticmethod
    def __compare(criterion_operator, actual_value, criterion_value):
        if SpecialValues.ignore == criterion_value:
            return True
        elif ComparisonOperators.equals == criterion_operator:
            return actual_value == criterion_value
        elif ComparisonOperators.does_not_equal == criterion_operator:
            return actual_value != criterion_value
        elif ComparisonOperators.starts_with == criterion_operator:
            return actual_value.find(criterion_value) == 0
        elif ComparisonOperators.contains == criterion_operator:
            where_is = actual_value.find(criterion_value)
            return where_is > -1
        elif ComparisonOperators.less_than == criterion_operator:
            if isinstance(actual_value, str) and not str.isnumeric(actual_value):
                return False
            else:
                return float(actual_value) < float(criterion_value)
        elif ComparisonOperators.greater_than == criterion_operator:
            if isinstance(actual_value, str) and not str.isnumeric(actual_value):
                return False
            else:
                return float(actual_value) > float(criterion_value)
        else:
            return False


class S3ObjectOperators:

    @staticmethod
    def __get_body_for_reading( s3_client, bucket, key):
        body = s3_client.get_object(Bucket=bucket, Key=key)['Body']
        # these "implement" the io.IOBase interface
        setattr(body, 'readable', lambda: True)
        setattr(body, 'writable', lambda: False)
        setattr(body, 'seekable', lambda: False)
        setattr(body, 'closed', False)
        setattr(body, 'flush', lambda: 'noop')

        return body

    @staticmethod
    def yield_from_object(bucket, key):
        s3_client = boto3.client('s3')
        body = S3ObjectOperators.__get_body_for_reading(s3_client, bucket, key)
        with io.BufferedReader(body) as buf, io.TextIOWrapper(body, newline='') as buf_wrapper:
            # we have to yield from instead of return to keep all the buffers alive
            yield from buf_wrapper.read()

    @staticmethod
    def yield_csv_from_object(bucket, key):
        s3_client = boto3.client('s3')
        body = S3ObjectOperators.__get_body_for_reading(s3_client, bucket, key)
        with io.BufferedReader(body) as buf, io.TextIOWrapper(body, newline='') as buf_wrapper:
            # we have to yield from instead of return to keep all the buffers alive
            yield from csv.reader(buf_wrapper)


class ComparisonOperators:
    equals = 'Equals'
    does_not_equal = 'Does not equal'
    starts_with = 'Starts with'
    contains = 'Contains'
    less_than = 'Less than'
    greater_than = 'Greater than'


class SpecialValues:
    ignore = None
    not_available = 'NA'