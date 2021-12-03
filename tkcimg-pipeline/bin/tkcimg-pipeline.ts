#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { TkcimgPipelineStack } from '../lib/tkcimg-pipeline-stack';

const app = new cdk.App();
new TkcimgPipelineStack(app, 'TkcimgPipelineStack');
// new TkcimgPipelineStack(app, 'TkcimgPipelineStack',{
//     tags:{
//         who:'scotts',
//         what:'Tkcimages pipeline stack for rekogniton',
//         githuburl: 'https://github.com/bdonkey/amazon-rekognition-serverless-large-scale-image-and-video-processing',
//     }
// });
