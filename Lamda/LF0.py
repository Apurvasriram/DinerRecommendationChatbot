import json
import boto3
import logging

def lambda_handler(event, context):
    client = boto3.client('lex-runtime')
    
    # print(event)
    response = client.post_text(
        botName='DiningConciergeBot',
        botAlias='DiningBot',
        userId='lf0',
        inputText=event['messages'][0]['unstructured']['text'])
    print(response)
    return {
        'statusCode': 200,
        # 'body': response,
        'messages' : [{"type": "unstructured","unstructured": {"text": response['message']}}],
        "headers": { 
            "Access-Control-Allow-Origin": "*" 
        }
    }