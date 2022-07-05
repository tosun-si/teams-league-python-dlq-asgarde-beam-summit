from datetime import datetime

import apache_beam as beam
from apache_beam import PCollection
from asgarde.failure import Failure

from team_league.application.team_league_options import TeamLeagueNamesOptions
from team_league.infrastructure.io.bigquery.failure_table_fields import FailureTableFields
from team_league.infrastructure.io.failure_helper import get_failure_error


class FailureBigqueryWriteTransform(beam.PTransform):
    """
    This Transform class allows to sink failures to Bigquery
    """

    def __init__(self,
                 pipeline_options: TeamLeagueNamesOptions):
        super().__init__()
        self.pipeline_options = pipeline_options

    def expand(self, inputs_failures: PCollection[Failure]):
        return (inputs_failures
                | 'Map to failure table fields' >> beam.Map(self.to_failure_table_fields)
                | "Sink failures to Bigquery" >> beam.io.WriteToBigQuery(
                    project=self.pipeline_options.project_id,
                    dataset=self.pipeline_options.failure_output_dataset,
                    table=self.pipeline_options.failure_output_table,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                )

    def to_failure_table_fields(self, failure: Failure):
        return {
            FailureTableFields.FEATURE_NAME.value: self.pipeline_options.failure_feature_name,
            FailureTableFields.JOB_NAME.value: self.pipeline_options.job_type,
            FailureTableFields.PIPELINE_STEP.value: failure.pipeline_step,
            FailureTableFields.INPUT_ELEMENT.value: failure.input_element,
            FailureTableFields.EXCEPTION_TYPE.value: type(failure.exception).__name__,
            FailureTableFields.STACK_TRACE.value: get_failure_error(failure),
            FailureTableFields.COMPONENT_TYPE.value: 'DATAFLOW',
            FailureTableFields.DWH_CREATION_DATE.value: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
