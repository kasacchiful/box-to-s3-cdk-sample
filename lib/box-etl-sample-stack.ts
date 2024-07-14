import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { ServicePrincipal, ManagedPolicy } from 'aws-cdk-lib/aws-iam';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { PythonFunction } from '@aws-cdk/aws-lambda-python-alpha';
import { PythonLayerVersion } from "@aws-cdk/aws-lambda-python-alpha";
import * as targets from 'aws-cdk-lib/aws-events-targets';
import { Rule, Schedule, RuleTargetInput } from 'aws-cdk-lib/aws-events';

export class BoxEtlSampleStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const inputBoxFolderId = '<Box Folder ID>';
    const boxParameterKey = '/box/sample/key_config';

    // IAM Role
    const lambdaRole = new cdk.aws_iam.Role(this, 'BoxToS3LambdaRole', {
      roleName: 'BoxToS3LambdaRole',
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
    });
    lambdaRole.addManagedPolicy(
      ManagedPolicy.fromAwsManagedPolicyName(
        "service-role/AWSLambdaBasicExecutionRole"
      )
    );
    lambdaRole.addManagedPolicy(
      ManagedPolicy.fromAwsManagedPolicyName(
        "AmazonSSMReadOnlyAccess"
      )
    );
    lambdaRole.addManagedPolicy(
      ManagedPolicy.fromAwsManagedPolicyName(
        "AmazonS3FullAccess"
      )
    );

    // S3 Bucket
    const bucket = new Bucket(this, 'FileBucket', {
      bucketName: "box-file-bucket-test"
    });
    
    // Lambda Layer
    const boxSdkLambdaLayer = new PythonLayerVersion(this, 'BoxToS3LambdaLayer', {
      layerVersionName: 'boxSdkLayer',
      entry: 'src/lambda/layer/box-sdk-layer',
      compatibleRuntimes: [cdk.aws_lambda.Runtime.PYTHON_3_12]
    });

    // Lambda Function
    const lambdaFunction = new PythonFunction(this, 'BoxToS3LambdaFunction', {
      functionName: 'boxToS3',
      runtime: cdk.aws_lambda.Runtime.PYTHON_3_12,
      entry: "src/lambda/handler",
      index: "box_to_s3.py",
      handler: "lambda_handler",
      role: lambdaRole,
      memorySize: 512,
      timeout: cdk.Duration.minutes(15),
      layers: [boxSdkLambdaLayer],
      environment: {
        BOX_PARAM_KEY: boxParameterKey,
        BUCKET_NAME: bucket.bucketName,
      },
    });

    // EventBridge Rule
    const ebrule = new Rule(this, 'boxFileDownloadExecRule', {
      // invoke function AM5:00(JST) everyday.
      schedule: Schedule.cron({minute: "0", hour: "20"}),
      targets: [
        new targets.LambdaFunction(lambdaFunction, {
          retryAttempts: 3,
          event: RuleTargetInput.fromObject({
            input_box_folder_id: inputBoxFolderId,  // Box Folder ID
            output_s3_url: `s3://${bucket.bucketName}/box/etl-sample/`  // S3 URL
          })
        })
      ]
    });
  }
}
