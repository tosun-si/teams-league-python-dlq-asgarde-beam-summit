from apache_beam import PTransform

from team_league.domain_ptransform.failure_log_io_connector import FailureLogIOConnector
from team_league.infrastructure.io.cloud_logging.failure_cloud_logging_write_transform import \
    FailureCloudLoggingWriteTransform


class FailureCloudLoggingIOAdapter(FailureLogIOConnector):

    def __init__(self,
                 write_transform: FailureCloudLoggingWriteTransform) -> None:
        super().__init__()
        self.write_transform = write_transform

    def write_failure_log(self) -> PTransform:
        return self.write_transform
