import boto3

bucket_name = 'rebeccacode-repo-a1'


def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    result_summary_file_name = '{}-olympic-data-request-result-summary.json'.format(event['request_id'])

    try:
        # make sure the object exists -- will throw an exception if it does not
        s3_client.get_object(Bucket=bucket_name, Key=result_summary_file_name)

        return s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket_name,
                'Key': result_summary_file_name
            }
        )

    except Exception as e:
        print(e)
        raise FileNotFoundError('Result summary file {} not found. Process might not be completed.'
                                .format(result_summary_file_name))
