from apache_beam import PTransform

from team_league.domain_ptransform.failure_database_io_connector import FailureDatabaseIOConnector
from team_league.infrastructure.io.bigquery.failure_bigquery_write_transform import FailureBigqueryWriteTransform


class FailureBigqueryIOAdapter(FailureDatabaseIOConnector):

    def __init__(self,
                 write_transform: FailureBigqueryWriteTransform) -> None:
        super().__init__()
        self.write_transform = write_transform

    def write_failure(self) -> PTransform:
        return self.write_transform
