import json
import urllib.parse
import boto3
import io
import csv

max_records = 3
bucket_name = 'rebeccacode-repo-a1'
file_name = 'athlete_events.csv'
s3c = boto3.client('s3')


def lambda_handler(event, context):
    result = {}
    result['count'] = 0
    result['message'] = ''
    result['records'] = []

    try:
        for row in csv_from_object(bucket_name, file_name):
            record = athlete_from_csv_row(row)
            match = False
            for criterion in event['criteria']:
                # skip query keys with value None
                if criterion['value'] is None:
                    continue

                criterion_attr = criterion['attribute']
                criterion_operator = criterion['operator']
                criterion_value = criterion['value']
                record_value = record[criterion_attr]

                match = compare(criterion_operator, record_value, criterion_value)

                if not match:
                    break

            if match:
                result['records'].append(record)
                result['count'] = len(result['records'])

            if result['count'] >= max_records:
                result['message'] = 'Truncating result set at {} records.'.format(max_records)
                break

    except Exception as e:
        print(e)
        raise e

    return result


def athlete_from_csv_row(row):
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


def csv_from_object(bucket, key):
    body = s3c.get_object(Bucket=bucket, Key=key)['Body']
    # these "implement" the io.IOBase interface
    setattr(body, 'readable', lambda: True)
    setattr(body, 'writable', lambda: False)
    setattr(body, 'seekable', lambda: False)
    setattr(body, 'closed', False)
    setattr(body, 'flush', lambda: 'noop')
    with io.BufferedReader(body) as buf, \
            io.TextIOWrapper(body, newline='') as buf_wrapper:
        # we have to yield from instead of return to keep all the buffers alive
        yield from csv.reader(buf_wrapper)


def compare(criterion_operator, actual_value, criterion_value):
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