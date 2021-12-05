import boto3
from decimal import Decimal
import json
import os
from helper import AwsHelper, S3Helper, DynamoDBHelper
import datastore

import re
from pprint import pprint


def callRekognition(bucketName, objectName, apiName, project, imgid):
    rekognition = AwsHelper().getClient('rekognition')

    maxLabels = int(os.environ['MAX_LABELS'])
    minConfidence = int(os.environ['MIN_CONFIDENCE'])

    if (apiName == "labels"):
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
    elif (apiName == "text"):
        response = rekognition.detect_text(
            Image={
                'S3Object': {
                    'Bucket': bucketName,
                    'Name': objectName
                }
            }
        )
    elif (apiName == "faces"):
        response = rekognition.detect_faces(
            Image={
                'S3Object': {
                    'Bucket': bucketName,
                    'Name': objectName
                }
            }
        )
    elif (apiName == "moderation"):
        response = rekognition.detect_moderation_labels(
            Image={
                'S3Object': {
                    'Bucket': bucketName,
                    'Name': objectName
                }
            }
        )
    elif (apiName == "celebrities"):
        response = rekognition.recognize_celebrities(
            Image={
                'S3Object': {
                    'Bucket': bucketName,
                    'Name': objectName
                }
            }
        )
    else:
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


        # begin ss proc
        detect_labels = response['Labels']
        for label in detect_labels:
            pprint(label)

            snsMessage = json.dumps(
                {'bucket': bucketName, 'key': objectName, 'project': project, 'imageid': imgid, 'labels': label})
            snsMessage = snsMessage + "\n"
            print(f"snsMessage = {snsMessage}")

            snsClient = boto3.client('sns')
            snsTopicArn = os.environ['SNS_TOPIC_ARN']
            sndRole = os.environ['SNS_ROLE_ARN']
            snsResponse = snsClient.publish(
                TargetArn=snsTopicArn,
                Message=snsMessage,
                #   MessageStructure = 'json'
            )
            print(f"snsResponse = {snsResponse}")
        # end scott

        return response

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
        request["imgid"] = imgId
        request["project"] = project

        return processRequest(request)
