import boto3
from decimal import Decimal
import json
import os
from helper import AwsHelper, S3Helper, DynamoDBHelper
import datastore
import uuid
import time

import re
from pprint import pprint

kinesisClient=  boto3.client('kinesis')
KINESIS_RETRY_COUNT = 10
KINESIS_RETRY_WAIT_IN_SEC = 0.1


def callRekognition(bucketName, objectName, apiName, project, imgid):
    rekognition = AwsHelper().getClient('rekognition')

    maxLabels = int(os.environ['MAX_LABELS'])
    minConfidence = int(os.environ['MIN_CONFIDENCE'])

    response = rekognition.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucketName,
                'Name': objectName
            }
        },
        MaxLabels=maxLabels,
        MinConfidence=minConfidence,
    )

    responseTxt = rekognition.detect_text(
        Image={
            'S3Object': {
                'Bucket': bucketName,
                'Name': objectName
            }
        },
        Filters={
            'WordFilter': {
                'MinConfidence': 80
            }
        }
    )

    responseFaces= rekognition.detect_faces(
        Image={
            'S3Object': {
                'Bucket': bucketName,
                'Name': objectName
            }
        },
        Attributes=['ALL']
    )

    tc = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
    krecords =[]

    detect_labels = response['Labels']

    labels  =[]
    for label in detect_labels:
        # pprint(label)
        labels.append(label)

    texts = []
    detect_text = responseTxt['TextDetections']
    for text in detect_text:
        # pprint(text)
        texts.append(text)

    faces = []
    detect_faces = responseFaces['FaceDetails']
    for face in detect_faces:
        # pprint(face)
        faces.append(face)

    iid = str(uuid.uuid4())
    snsMessage = json.dumps(
        {'uuid': iid,'rekcreated': tc,'bucket': bucketName, 'key': objectName, 'project': project, 'imageid': imgid, 'labels': labels,'text': texts,'faces':faces})
    snsMessage = snsMessage + "\n"
    print(f"snsMessage = {snsMessage}")

    kresp =  kinesisClient.put_record(StreamName=os.environ['KIN_STREAM'],
                                      Data=snsMessage,
                                      PartitionKey="partitionkey")
    print(f"kinesis label response: {kresp}")


    # send_to_stream(krecords,KINESIS_RETRY_COUNT)

    return kresp

# see https://bit.ly/3EG0NhF
def send_to_stream(kinesis_records, retry_count):
    put_response = kinesisClient.put_records(
        Records=kinesis_records,
        StreamName=os.environ['KIN_STREAM']
    )
    print(f"kinesis put_response text: {put_response}")

    failed_count = put_response['FailedRecordCount']
    if failed_count > 0:
        if retry_count > 0:
            retry_kinesis_records = []
            for idx, record in enumerate(put_response['Records']):
                if 'ErrorCode' in record:
                    retry_kinesis_records.append(kinesis_records[idx])
            time.sleep(KINESIS_RETRY_WAIT_IN_SEC * (KINESIS_RETRY_COUNT - retry_count + 1))
            send_to_stream(retry_kinesis_records, retry_count - 1)
        else:
            print(f'Not able to put records after retries. Records = {put_response["Records"]}')

def processImage(itemId, bucketName, objectName, outputBucketName, itemsTableName, project, imgId):

    apiName = objectName.split("/")[0]

    response = callRekognition(bucketName, objectName, apiName, project, imgId)

    print("Generating output for ItemId: {}".format(itemId))
    print(response)

    outputPath = "sync/{}-analysis/{}/".format(objectName, itemId)
    opath = "{}response.json".format(outputPath)
    S3Helper.writeToS3(json.dumps(response), outputBucketName, opath)

    # opg = OutputGenerator(itemId, response, bucketName, objectName, detectForms, detectTables, ddb)
    # opg.run()

    print("ItemId: {}".format(itemId))

    ds = datastore.ItemStore(itemsTableName)
    ds.markItemComplete(itemId)

# --------------- Main handler ------------------

def processRequest(request):

    output = ""

    print("request: {}".format(request))

    bucketName = request['bucketName']
    objectName = request['objectName']
    itemId = request['itemId']
    outputBucket = request['outputBucket']
    itemsTable = request["itemsTable"]
    imgid = request["imgid"]
    project = request["project"]

    if (itemId and bucketName and objectName):
        print(f"ItemId: {itemId}, object: {bucketName}/{objectName}, project: {project}, imgid: {imgid}")
        # print("ItemId: {}, Object: {}/{}".format(itemId, bucketName, objectName))

        processImage(itemId, bucketName, objectName, outputBucket, itemsTable, project, imgid)

        output = "Item: {}, Object: {}/{} processed.".format(itemId, bucketName, objectName)
        print(output)

    return {
        'statusCode': 200,
        'body': output
    }

def lambda_handler(event, context):

    print("event: {}".format(event))
    message = json.loads(event['Records'][0]['body'])
    print("Message: {}".format(message))

    # scott proc
    s = json.dumps(message)
    pattern = re.compile('objectName": ".*\/(\d*)-(\d*)')
    for (imgId, project) in re.findall(pattern, s):
        print(f"imgId= {imgId}, project = {project}, bucket = {message['bucketName']}")
    # end scott proc

    request = {}
    request["itemId"] = message['itemId']
    request["bucketName"] = message['bucketName']
    request["objectName"] = message['objectName']
    request["outputBucket"] = os.environ['OUTPUT_BUCKET']
    request["itemsTable"] = os.environ['ITEMS_TABLE']
    request["kinesisStream"] = os.environ['KIN_STREAM']
    request["imgid"] = imgId
    request["project"] = project

    return processRequest(request)
