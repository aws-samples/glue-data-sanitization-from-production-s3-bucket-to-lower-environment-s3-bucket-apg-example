import { Stack, StackProps, Duration } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import cdk = require('aws-cdk-lib');
import s3 = require('aws-cdk-lib/aws-s3');
import glue = require('aws-cdk-lib/aws-glue');
import iam = require("aws-cdk-lib/aws-iam");
import kms = require("aws-cdk-lib/aws-kms");
import { NagSuppressions } from 'cdk-nag';
import { Effect } from 'aws-cdk-lib/aws-iam';

export class GlueDataSanitizationFromProductionS3BucketToLowerEnvironmentS3BucketStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Creates key for IAM setup according to AWS Doc https://aws.amazon.com/premiumsupport/knowledge-center/glue-not-writing-logs-cloudwatch/
    const productionInfrakmsKeyPolicy = new iam.PolicyDocument({
      statements: [
        // Policy for root account
        new iam.PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'kms:*'
          ],
          principals: [new iam.AccountRootPrincipal()],
          resources: ["*"],
        }),
        // Policy for cloudwatch to access https://docs.aws.amazon.com/glue/latest/dg/console-security-configurations.html
        new iam.PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            "kms:Encrypt*",
            "kms:Decrypt*",
            "kms:ReEncrypt*",
            "kms:GenerateDataKey*",
            "kms:Describe*"
          ],
          principals: [new iam.ServicePrincipal(`logs.${this.region}.amazonaws.com`)],
          resources: ["*"],
          conditions: {
            // Limit to glue logs
            ArnEquals: {
              "aws:SourceArn": [
                `arn:aws:logs:${this.region}:${this.account}:log-group:/aws-glue/*`
              ]
            }
          }
        }),
      ],
    });

    // KMS Key for production infrastructure encryption includes but not limited to bucket, cloudwatch
    const productionInfrakmsKey = new kms.Key(this, 'production-infra-key', {
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,       // Auto destroy if removed from stack
      pendingWindow: Duration.days(7),                // Delete after 7 day once delete is triggered
      policy: productionInfrakmsKeyPolicy             // Set custom policy allow Cloudwatch to use it for glue to write
    });


    // KMS Key for production data encryption includes but not limited to bucket, cloudwatch
    const productionDatakmsKey = new kms.Key(this, 'production-data-key', {
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,       // Auto destroy if removed from stack
      pendingWindow: Duration.days(7),                // Delete after 7 day once delete is triggered
    });


    // KMS Key for nonproduction data encryption includes but not limited to bucket
    const nonproductionDatakmsKey = new kms.Key(this, 'nonproduction-data-key', {
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,       // Auto destroy if removed from stack
      pendingWindow: Duration.days(7)                 // Delete after 7 day once delete is triggered
    });


    // Bucket internal-infrastructure-bucket
    // The S3 bucket to store  internal infrastructure bucket to store script/other infrastructure related info
    const internalInfrastructureBucket = new s3.Bucket(this, "internal-infrastructure-bucket", {
      enforceSSL: true,                                              // ForceSSL
      publicReadAccess: false,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,             // Block all public access by default (only ACL or IAM)
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: productionInfrakmsKey,
      autoDeleteObjects: true,                                        // NOT recommended for production code, this is just for easier clean up
      serverAccessLogsPrefix: "internal-infrastructure-bucket",
    });

    // Bucket internal-infrastructure-log-bucket
    // Store the log from glue
    const internalInfrastructureLogBucket = new s3.Bucket(this, "internal-infrastructure-log-bucket", {
      enforceSSL: true,                                              // ForceSSL
      publicReadAccess: false,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,             // Block all public access by default (only ACL or IAM)
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: productionInfrakmsKey,
      autoDeleteObjects: true,                                        // NOT recommended for production code, this is just for easier clean up
      serverAccessLogsPrefix: "internal-infrastructure-bucket",
    });


    // Bucket production-data-bucket
    // The S3 bucket to store production data and encrypted with production key
    const productionDataBucket = new s3.Bucket(this, "production-data-bucket", {
      enforceSSL: true,                                              // ForceSSL
      publicReadAccess: false,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,             // Block all public access by default (only ACL or IAM)
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: productionDatakmsKey,
      autoDeleteObjects: true,                                        // NOT recommended for production code, this is just for easier clean up
      serverAccessLogsPrefix: "production-data-bucket",
    });

    // Bucket production-data-bucket
    // The S3 bucket to store nonproduction data and encrypted with nonproduction key
    const nonproductionDataBucket = new s3.Bucket(this, "nonproduction-data-bucket", {
      enforceSSL: true,                                              // ForceSSL
      publicReadAccess: false,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,             // Block all public access by default (only ACL or IAM)
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: nonproductionDatakmsKey,
      autoDeleteObjects: true,                                        // NOT recommended for production code, this is just for easier clean up
      serverAccessLogsPrefix: "nonproduction-data-bucket",
    });

    const glueJobIAMRole = new iam.Role(this, "glue-job-iam-role", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com")
    });

    // Add minimum AWS Glue roles (from cloudwatch logs etc)
    // https://docs.aws.amazon.com/glue/latest/dg/create-service-policy.html

    // The reason to avoid managed policies are due to high admin previlieges (i.e. S3 admin on all resources) so restrict in this case to specific resource
    // to be closer to least previlieges. Below policies reduces some policy behind above doc (espeically on S3) to only allow access to specific bucket and no creation previlieges.
    // glueJobIAMRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'));

    // Require minimum roles for S3/EC2/IAM/CloudWatch metrics
    // Note additional S3/KMS will be specify in later section for specific resources
    // This generic one is minimum required for all resources for glue to work correctly

    // For S3 and IAM - no registion specification as they are global ARN resources
    glueJobIAMRole.addToPolicy(
      new iam.PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:ListAllMyBuckets",
          "s3:GetBucketAcl",
        ],
        resources: ["*"],
      }));

    glueJobIAMRole.addToPolicy(
      new iam.PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "iam:ListRolePolicies",
          "iam:GetRole",
          "iam:GetRolePolicy",
        ],
        resources: [`arn:aws:iam::${this.account}:*`],
      }));

    // For EC2/Glue/Cloudwatch
    // Glue need to be wildcard resource otherwise (not lock to account level)
    // Otherwise will received cannot retrieve security configuration error
    glueJobIAMRole.addToPolicy(
      new iam.PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "glue:*",
        ],
        resources: ["*",],
      }));

      glueJobIAMRole.addToPolicy(
        new iam.PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            "ec2:DescribeVpcEndpoints",
            "ec2:DescribeRouteTables",
            "ec2:CreateNetworkInterface",
            "ec2:DeleteNetworkInterface",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DescribeSecurityGroups",
            "ec2:DescribeSubnets",
            "ec2:DescribeVpcAttribute",
          ],
          resources: [`arn:aws:ec2:${this.region}:${this.account}:*`,],
        }));

        glueJobIAMRole.addToPolicy(
          new iam.PolicyStatement({
            effect: Effect.ALLOW,
            actions: [
              "cloudwatch:PutMetricData"
            ],
            resources: [`arn:aws:cloudwatch:${this.region}:${this.account}:*`,],
          }));
    
    // Enable cloudwatch log setup
    glueJobIAMRole.addToPolicy(
      new iam.PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:AssociateKmsKey"
        ],
        resources: [`arn:aws:logs:${this.region}:${this.account}:log-group:/aws-glue/*`],
      }));

    glueJobIAMRole.addToPolicy(
      new iam.PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "ec2:CreateTags",
          "ec2:DeleteTags"
        ],
        conditions: {
          "ForAllValues:StringEquals": {
            "aws:TagKeys": [
              "aws-glue-service-resource"
            ]
          }
        },
        resources: [
          `arn:aws:ec2:${this.region}:${this.account}:network-interface/*`,
          `arn:aws:ec2:${this.region}:${this.account}:security-group/*`,
          `arn:aws:ec2:${this.region}:${this.account}:instance/*`
        ],
      }));


    // Glue job will only have READ access on infrastructure bucket for script access
    internalInfrastructureBucket.grantRead(glueJobIAMRole);

    // Glue job will need read/write access to the log bucket
    internalInfrastructureLogBucket.grantReadWrite(glueJobIAMRole);

    // Glue job will only have READ access on production bucket
    productionDataBucket.grantRead(glueJobIAMRole);

    // Glue job will need READ/WRITE access to populate data into lower env bucket
    nonproductionDataBucket.grantReadWrite(glueJobIAMRole);

    // Grant key usage to the glue job
    productionInfrakmsKey.grantEncryptDecrypt(glueJobIAMRole);
    productionDatakmsKey.grantEncryptDecrypt(glueJobIAMRole);
    nonproductionDatakmsKey.grantEncryptDecrypt(glueJobIAMRole);

    // Enable adding suppressions to AwsSolutions-IAM5 to notify CDK-NAG that 
    // The wildcard S3 permission is genearted by CDK automatically to allow read/write of resources on S3 bucket according to minimum policy to each individual bucket
    NagSuppressions.addResourceSuppressions(
      glueJobIAMRole,
      [
        { id: 'AwsSolutions-IAM4', reason: 'The managed policy in question is AWSGlueServiceRole and is one of preffered way to setup according to AWS doc https://docs.aws.amazon.com/glue/latest/dg/create-service-policy.html.' },
        { id: 'AwsSolutions-IAM5', reason: 'The wildcard S3 permission is genearted by CDK automatically to allow read/write of resources on S3 bucket according to minimum policy to each individual bucket. The CloudWatch wildcard permission is required as log group will be created dynamically as part of AWS Glue job.' },
      ],
      true
    );

    // Glue job security policy for encryption
    const cfnSecurityConfiguration = new glue.CfnSecurityConfiguration(this, 'glue-security-configuration', {
      encryptionConfiguration: {
        cloudWatchEncryption: {
          cloudWatchEncryptionMode: "SSE-KMS",
          kmsKeyArn: productionInfrakmsKey.keyArn
        },
        jobBookmarksEncryption: {
          jobBookmarksEncryptionMode: "CSE-KMS",
          kmsKeyArn: productionInfrakmsKey.keyArn
        },
        s3Encryptions: [{
          s3EncryptionMode: "DISABLED",
        }],
      },
      name: 'glue-security-configuration',
    });

    // Glue security policy will need to depend on the creation of above setup
    cfnSecurityConfiguration.addDependsOn(productionInfrakmsKey.node.defaultChild as kms.CfnKey);
    cfnSecurityConfiguration.addDependsOn(productionDatakmsKey.node.defaultChild as kms.CfnKey);
    cfnSecurityConfiguration.addDependsOn(nonproductionDatakmsKey.node.defaultChild as kms.CfnKey);

    // Glue workflow to govern the overall process
    const glueWorkflow = new glue.CfnWorkflow(this, "glue-etl-workflow");

    // Glue job
    const glueJobName = "glue-etl-job";
    const glueJob = new glue.CfnJob(this, glueJobName, {
      role: glueJobIAMRole.roleArn,
      name: glueJobName,
      command: {
        name: "glueetl",
        pythonVersion: "3",
        scriptLocation: "s3://" + internalInfrastructureBucket.bucketName + "/scripts/glue-etl-job-production-to-lower-env-data-sanitilzation.py"
      },
      securityConfiguration: cfnSecurityConfiguration.name,
      // // Pass the arguments for the bucket names to the script
      defaultArguments: {
        // Disable bookmark for easier debugging to reuse data
        "--job-bookmark-option": "job-bookmark-disable",
        "--job-language": "python",
        "--enable-metrics": "",
        "--spark-event-logs-path": "s3://" + internalInfrastructureLogBucket.bucketName + "/glue-etl-job/logs/",
        "--enable-continuous-cloudwatch-log": "true",
        "--SOURCE_BUCKETNAME": productionDataBucket.bucketName,
        "--TARGET_BUCKETNAME": nonproductionDataBucket.bucketName
      },
      glueVersion: "3.0",
      // Force 1 run at a time for now
      executionProperty: {
        maxConcurrentRuns: 1,
      },
      // Below are the capacity setting for the job and optional
      // Specify 2DPU (1 DPU = 4 vCPUs X 16 GB) 
      // As the workertype is "standard", maxCapacity and numberofworkers need to be specified
      // https://docs.aws.amazon.com/glue/latest/dg/add-job.html
      // Even though the doc mentioned to specify Maximum capacity in "Standard" type, but it will result error if specify maxCapcity, so remove it
      maxRetries: 2,
      timeout: 120,
      numberOfWorkers: 2,
      workerType: "Standard",
    });

    // Glue job will need to depend on the creation of above setup
    glueJob.addDependsOn(cfnSecurityConfiguration);
    glueJob.addDependsOn(internalInfrastructureLogBucket.node.defaultChild as s3.CfnBucket);
    glueJob.addDependsOn(internalInfrastructureBucket.node.defaultChild as s3.CfnBucket);
    glueJob.addDependsOn(productionDataBucket.node.defaultChild as s3.CfnBucket);
    glueJob.addDependsOn(internalInfrastructureBucket.node.defaultChild as s3.CfnBucket);

    // Glue Job Trigger
    const glueJobTrigger = new glue.CfnTrigger(this, "glue-etl-job-trigger", {
      workflowName: glueWorkflow.name,
      // Trigger on demand
      type: "ON_DEMAND",
      actions: [
        {
          jobName: glueJobName
        }
      ],
      startOnCreation: false, // As it is on demand job, so need to set to false, otherwise cloudformation will fail
    });

    glueJobTrigger.addDependsOn(glueWorkflow);
    glueJobTrigger.addDependsOn(glueJob);

    // Prinout the infrastructure job to copy the S3 asset
    new cdk.CfnOutput(this, 'InfrastructureBucketName', { value: internalInfrastructureBucket.bucketName });
    new cdk.CfnOutput(this, 'ProductionDataBucketName', { value: productionDataBucket.bucketName });
    new cdk.CfnOutput(this, 'NonproductionDataBucket', { value: nonproductionDataBucket.bucketName });
  }
}
