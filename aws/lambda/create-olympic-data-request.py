import boto3
import json
import uuid

bucket_name = 'rebeccacode-repo-a1'


def lambda_handler(event, context):
    result = {'request_id': '', 'request_url': ''}

    try:
        s3_client = boto3.client('s3')

        request_id = new_id()
        file_name = '{}-olympic-data-request.json'.format(request_id)

        data = {'request_id': request_id, 'criteria': event['criteria']}

        s3_client.put_object(Bucket=bucket_name,
                             Key=file_name,
                             Body=json.dumps(data),
                             ContentEncoding='utf-8',
                             ContentType='application/json',
                             Metadata={'request_id': request_id})

        result['request_id'] = request_id
        result['request_url'] =  s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket_name,
                'Key': file_name
            }
        )

    except Exception as e:
        print(e)
        raise e

    return result


def new_id():
    return str(uuid.uuid4())