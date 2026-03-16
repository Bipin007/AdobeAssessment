#!/usr/bin/env python3
"""
CDK app entrypoint. Deploy with: cdk deploy
"""
import aws_cdk as cdk
from search_keyword_stack import SearchKeywordPipelineStack

app = cdk.App()
SearchKeywordPipelineStack(app, "SearchKeywordPipelineStack")
app.synth()
