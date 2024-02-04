#!/bin/bash

airflow connections add aws_credentials --conn-uri 'aws://AKIAQAHBFEXISNVCPU77:NIhmbycdmhNr6G9STonTW7NBK6eOaIRUdK5n4Jbu@'
airflow connections add redshift --conn-uri 'redshift://awsuser:Rexona3113@default.000472196561.us-east-1.redshift-serverless.amazonaws.com:5439/dev'

airflow variables set s3_bucket gabi-udacity
airflow variables set s3_prefix data-pipelines