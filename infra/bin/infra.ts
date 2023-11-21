#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { GlueStack } from '../lib/stacks/glue-stack';
import { Config } from '../config/loader';

const ns = Config.app.ns;
const app = new cdk.App({
  context: { ns },
});

new GlueStack(app, `${ns}GlueStack`, {});

const tags = cdk.Tags.of(app);
tags.add('namespace', Config.app.ns);
tags.add('stage', Config.app.stage);

app.synth();
