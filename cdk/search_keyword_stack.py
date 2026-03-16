"""
CDK stack: S3 buckets, Glue job, Step Functions state machine, and S3 upload trigger.
"""
from pathlib import Path
import aws_cdk as cdk
from aws_cdk import (
    aws_sns as sns,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


class SearchKeywordPipelineStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- S3 ---
        input_bucket = s3.Bucket(
            self,
            "InputBucket",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            event_bridge_enabled=True,
        )
        output_bucket = s3.Bucket(
            self,
            "OutputBucket",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        success_topic = sns.Topic(
            self,
            "PipelineSuccessTopic",
            display_name="Search Keyword Pipeline Success",
        )
        failure_topic = sns.Topic(
            self,
            "PipelineFailureTopic",
            display_name="Search Keyword Pipeline Failure",
        )

        # --- Glue job script (upload from glue/scripts/) ---
        script_path = Path(__file__).resolve().parent.parent / "glue" / "scripts" / "search_keyword_performance.py"
        if not script_path.exists():
            raise FileNotFoundError(f"Glue script not found: {script_path}")

        glue_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
            ],
        )
        input_bucket.grant_read(glue_role)
        output_bucket.grant_read_write(glue_role)

        from aws_cdk import aws_s3_assets
        script_file_asset = aws_s3_assets.Asset(self, "GlueScriptFile", path=str(script_path))
        script_file_asset.grant_read(glue_role)

        glue_job = glue.CfnJob(
            self,
            "SearchKeywordGlueJob",
            name="search-keyword-performance-job",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{script_file_asset.s3_bucket_name}/{script_file_asset.s3_object_key}",
                python_version="3",
            ),
            default_arguments={
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
            },
            glue_version="5.1",
            worker_type="G.1X",
            number_of_workers=10,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
        )

        # Step Functions supports both:
        # 1. manual execution input: { "input_path": "s3://...", "output_path": "s3://..." }
        # 2. S3 Object Created events from the input bucket
        manual_input = sfn.Pass(
            self,
            "UseManualExecutionInput",
            parameters={
                "input_path.$": "$.input_path",
                "output_path.$": "$.output_path",
            },
        )
        upload_input = sfn.Pass(
            self,
            "BuildPathsFromS3Event",
            parameters={
                "input_path.$": "States.Format('s3://{}/{}', $.detail.bucket.name, $.detail.object.key)",
                "output_path": f"s3://{output_bucket.bucket_name}/output/",
            },
        )
        resolve_paths = sfn.Choice(self, "ResolveExecutionPaths")
        resolve_paths.when(sfn.Condition.is_present("$.input_path"), manual_input)
        resolve_paths.otherwise(upload_input)

        start_glue = tasks.GlueStartJobRun(
            self,
            "StartGlueJob",
            glue_job_name=glue_job.name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--input_path": sfn.JsonPath.string_at("$.input_path"),
                "--output_path": sfn.JsonPath.string_at("$.output_path"),
            }),
            result_path="$.glue_result",
        )
        notify_success = tasks.SnsPublish(
            self,
            "NotifyPipelineSuccess",
            topic=success_topic,
            subject="Search keyword pipeline succeeded",
            message=sfn.TaskInput.from_object({
                "status": "SUCCEEDED",
                "input_path": sfn.JsonPath.string_at("$.input_path"),
                "output_path": sfn.JsonPath.string_at("$.output_path"),
                "glue_job_name": sfn.JsonPath.string_at("$.glue_result.JobName"),
                "glue_job_run_id": sfn.JsonPath.string_at("$.glue_result.Id"),
                "note": "Glue finished successfully. Check the output bucket for the dated tab-delimited file.",
            }),
            result_path=sfn.JsonPath.DISCARD,
        )
        notify_failure = tasks.SnsPublish(
            self,
            "NotifyPipelineFailure",
            topic=failure_topic,
            subject="Search keyword pipeline failed",
            message=sfn.TaskInput.from_object({
                "status": "FAILED",
                "input_path": sfn.JsonPath.string_at("$.input_path"),
                "output_path": sfn.JsonPath.string_at("$.output_path"),
                "error": sfn.JsonPath.string_at("$.error_info.Error"),
                "cause": sfn.JsonPath.string_at("$.error_info.Cause"),
                "note": "Review the Step Functions execution history and the Glue job logs in CloudWatch.",
            }),
            result_path=sfn.JsonPath.DISCARD,
        )
        failed = sfn.Fail(
            self,
            "PipelineFailed",
            cause="Glue job execution failed",
            error="SearchKeywordPipelineFailed",
        )

        start_glue.add_catch(
            notify_failure.next(failed),
            errors=["States.ALL"],
            result_path="$.error_info",
        )

        definition = resolve_paths.afterwards().next(start_glue).next(notify_success)
        state_machine = sfn.StateMachine(
            self,
            "SearchKeywordStateMachine",
            state_machine_name="search-keyword-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
        )

        upload_rule = events.Rule(
            self,
            "StartPipelineOnInputUpload",
            description="Start the search keyword pipeline when a new input file is uploaded.",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {
                        "name": [input_bucket.bucket_name],
                    },
                    "object": {
                        "key": [{"prefix": "input/"}],
                    },
                },
            ),
        )
        upload_rule.add_target(events_targets.SfnStateMachine(state_machine))

        # Outputs
        cdk.CfnOutput(self, "InputBucketName", value=input_bucket.bucket_name, description="S3 input bucket")
        cdk.CfnOutput(self, "OutputBucketName", value=output_bucket.bucket_name, description="S3 output bucket")
        cdk.CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn, description="Step Functions ARN")
        cdk.CfnOutput(self, "SuccessTopicArn", value=success_topic.topic_arn, description="SNS topic for successful pipeline runs")
        cdk.CfnOutput(self, "FailureTopicArn", value=failure_topic.topic_arn, description="SNS topic for failed pipeline runs")
