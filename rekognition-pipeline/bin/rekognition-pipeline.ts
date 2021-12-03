#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { RekognitionPipelineStack } from '../lib/rekognition-pipeline-stack';

const app = new cdk.App();
// new RekognitionPipelineStack(app, 'RekognitionPipelineStack');
new RekognitionPipelineStack(app, 'RekognitionPipelineStack',{
    tags:{
        who:'scotts',
        what:'test rekognition pipeline',
        githuburl: 'https://github.com/bdonkey/amazon-rekognition-serverless-large-scale-image-and-video-processing',
    }
});
