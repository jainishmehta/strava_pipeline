import boto3
import json
from botocore.exceptions import ClientError
#TODO: Test with AWS
def aws_secret_rapid_api(secret_name: str, region_name: str = "us-east-1") -> str:
    secret_name = "MySecretName"
    region_name = "us-west-2"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print("The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
    if 'SecretString' in get_secret_value_response:
        secret_data = get_secret_value_response['SecretString']
    else:
        secret_data = get_secret_value_response['SecretBinary']
    secret_dict = json.loads(secret_data)
    return secret_dict.get("rapid-api-key") 