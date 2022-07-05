import logging

import apache_beam as beam
from apache_beam import PCollection
from asgarde.failure import Failure

from team_league.infrastructure.io.failure_helper import get_failure_error


class FailureCloudLoggingWriteTransform(beam.PTransform):
    """
    This Transform class allows to log failures to cloud logging with a specific format.
    Then these logs can be intercepted by an alerting policy.
    Via this policy, emails are sent to some email groups
    """

    def __init__(self):
        super().__init__()

    def expand(self, inputs_failures: PCollection[Failure]):
        return (inputs_failures
                | 'Map to failure log info' >> beam.Map(self.to_failure_log_info)
                | "Logs Failure to cloud logging" >> beam.Map(self.log_failure))

    def to_failure_log_info(self, failure: Failure):
        input_element_info = f'InputElement : {failure.input_element}'
        stack_trace_info = f'StackTrace : {get_failure_error(failure)}'

        return f'{input_element_info} \n {stack_trace_info}'

    def log_failure(self, failure_log_info: str):
        logging.error(failure_log_info)

        return failure_log_info
