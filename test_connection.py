from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

def test_aws_credentials(conn_id):
    aws_hook = AwsBaseHook(conn_id)
    credentials = aws_hook.get_credentials()
    if credentials:
        print(f"AWS credentials '{conn_id}' test successful.")
    else:
        print(f"AWS credentials '{conn_id}' test failed.")

# Example usage
test_aws_credentials('aws_credentials')
