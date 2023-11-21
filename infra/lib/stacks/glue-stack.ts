import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as glueAlpha from '@aws-cdk/aws-glue-alpha';

export class GlueStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const ns = this.node.tryGetContext('ns') as string;

    const queue = new sqs.Queue(this, 'Queue');

    const bucket = new s3.Bucket(this, 'Bucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });
    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SqsDestination(queue)
    );

    const database = new glueAlpha.Database(this, 'Database');

    const role = new iam.Role(this, 'GlueS3Role', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromManagedPolicyArn(
          this,
          'GlueS3RolePolicy',
          'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        ),
        iam.ManagedPolicy.fromManagedPolicyArn(
          this,
          'GlueSQSRolePolicy',
          'arn:aws:iam::aws:policy/AmazonSQSFullAccess'
        ),
      ],
    });
    bucket.grantReadWrite(role);

    const csvClassifier = new glue.CfnClassifier(this, 'CSVClassifier', {
      csvClassifier: {
        name: `${ns.toLowerCase()}-csv-classifier`,
        containsHeader: 'PRESENT',
        header: ['movieId', 'title', 'genres'],
        delimiter: ',',
        quoteSymbol: '"',
      },
    });
    new glue.CfnCrawler(this, 'S3Crawler', {
      name: `${ns}-s3-crawler`,
      role: role.roleArn,
      databaseName: database.databaseName,
      tablePrefix: `${ns.toLowerCase()}-`,
      schemaChangePolicy: {
        updateBehavior: 'UPDATE_IN_DATABASE',
        deleteBehavior: 'DEPRECATE_IN_DATABASE',
      },
      targets: {
        s3Targets: [
          {
            path: bucket.s3UrlForObject('/input/'),
            eventQueueArn: queue.queueArn,
          },
        ],
      },
      recrawlPolicy: {
        recrawlBehavior: 'CRAWL_EVENT_MODE',
      },
      classifiers: [csvClassifier.ref],
    });
  }
}
