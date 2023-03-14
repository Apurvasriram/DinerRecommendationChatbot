import boto3
import json
import logging
from boto3.dynamodb.conditions import Key, Attr
# from botocore.vendored import requests
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def getSQSMsg():
    SQS = boto3.client("sqs")
    url = 'https://sqs.us-east-1.amazonaws.com/276240606154/DiningChatbot'
    response = SQS.receive_message(
        QueueUrl=url, 
        AttributeNames=['SentTimestamp'],
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    try:
        message = response['Messages'][0]
        if message is None:
            logger.debug("Empty message")
            return None
    except KeyError:
        logger.debug("No message in the queue")
        return None
    message = response['Messages'][0]
    SQS.delete_message(
            QueueUrl=url,
            ReceiptHandle=message['ReceiptHandle']
        )
    logger.debug('Received and deleted message: %s' % response)
    return message

def get_dynamo_data(dynno, table, value):
    response = table.get_item(Key={'Business_ID': value}, TableName='yelp-restaurants')

    name = response['Item']['Name']
    address_list = response['Item']['Address']
    return '{}, {}'.format(name, address_list)

def lambda_handler(event, context):
    
    """
        Query SQS to get the messages
        Store the relevant info, and pass it to the Elastic Search
    """
    
    table_name = 'yelp-restaurants'
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    message = getSQSMsg() #data will be a json object
    if message is None:
        logger.debug("No Cuisine or PhoneNum key found in message")
        return
    cuisine = message["MessageAttributes"]["Cuisine"]["StringValue"]
    location = message["MessageAttributes"]["Location"]["StringValue"]
    date = message["MessageAttributes"]["reservation_date"]["StringValue"]
    time = message["MessageAttributes"]["reservation_time"]["StringValue"]
    numOfPeople = message["MessageAttributes"]["numberOfPeople"]["StringValue"]
    phoneNumber = message["MessageAttributes"]["PhoneNumber"]["StringValue"]
    # phoneNumber = "+1" + phoneNumber
    
    
    if not cuisine or not phoneNumber:
        logger.debug("No Cuisine or PhoneNum key found in message")
        return
    
    """
        Query database based on elastic search results
        Store the relevant info, create the message and sns the info
    """
    
    es_query = "https://search-restaurants-bhto4krxmtf3ibaxjxz24qea3y.us-east-1.es.amazonaws.com/_search?q={cuisine}".format(
        cuisine=cuisine)
    
    print("es_query: ", es_query)
        
    esResponse = requests.get(es_query)
    print("esResponse: ", esResponse)
    # data = json.loads(esResponse.content.decode('utf-8'))
    query = {"from": 0, "size": 5, "query": {"match": {"Cuisine": message["MessageAttributes"]["Cuisine"]["StringValue"]}}}
    data = json.loads(requests.get(
        es_query,
        auth=("restaurents-demo", "Kushaal@26"),
        headers={"Content-Type": "application/json"}, data=json.dumps(query)).content.decode('utf-8'))

    print("data: ", data)
    number_of_records_found = int(data["hits"]["total"]["value"])
    print(f"Total Number of records in ES : {number_of_records_found}")
    hits = data["hits"]["hits"]
    print(f"Data are : {hits}")
    suggested_restaurants = []
    for hit in hits:
        id = hit['_source']['Business_ID']
        suggested_restaurant = get_dynamo_data(dynamodb, table, id)
        print(f"LOG:{suggested_restaurant}")
        suggested_restaurants.append(suggested_restaurant)
        details = suggested_restaurants
    print(f"Suggested Resturants are : {suggested_restaurants}")
    logging.info("reached")
    message_data = "Hello! Here are my {cuisine} restaurant suggestions in {location} for {numOfPeople} people, for {date} at {time}: \n\n".format(
            cuisine=cuisine,
            location=location,
            numOfPeople=numOfPeople,
            date=date,
            time=time,
        )

    message_data += "\n"

    for i, rest in enumerate(suggested_restaurants):
        message_data += str(i + 1) + ". " + rest + "\n"

    print("message_data: ",message_data)
    message_data += "Enjoy your meal!!"
    
    try:
        
        client = boto3.client("sns", region_name="us-east-1")
        client.publish(TopicArn='arn:aws:sns:us-east-1:276240606154:chatbot-sns', Message=message_data)
        
        
        # client = boto3.client('sns', region_name= 'us-east-1')
        # response = client.publish(
        #     PhoneNumber=phoneNumber,
        #     Message= message_data,
        #     MessageStructure='string'
        # )
    except KeyError:
        logger.debug("Error sending ")
    logger.debug("Message = '%s' Phone Number = %s" % (message_data, phoneNumber))
    
    return {
        'statusCode': 200,
        'body': json.dumps("LF2 running succesfully")
    }
    # return messageToSend