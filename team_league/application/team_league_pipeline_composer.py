from abc import ABCMeta, abstractmethod

from apache_beam import Pipeline

from team_league.application.team_league_options import TeamLeagueNamesOptions
from team_league.domain_transform.failure_database_io_connector import FailureDatabaseIOConnector
from team_league.domain_transform.failure_log_io_connector import FailureLogIOConnector
from team_league.domain_transform.team_stats_database_io_connector import TeamStatsDatabaseIOConnector
from team_league.domain_transform.team_stats_inmemory_io_connector import TeamStatsInMemoryIOConnector
from team_league.domain_transform.team_stats_transform import TeamStatsTransform


class PipelineComposer(metaclass=ABCMeta):

    @abstractmethod
    def compose(self) -> Pipeline:
        pass


class TeamLeaguePipelineComposer:

    def __init__(self,
                 pipeline_options: TeamLeagueNamesOptions,
                 team_stats_inmemory_io_connector: TeamStatsInMemoryIOConnector,
                 team_stats_database_io_connector: TeamStatsDatabaseIOConnector,
                 failure_database_io_connector: FailureDatabaseIOConnector,
                 failure_log_io_connector: FailureLogIOConnector) -> None:
        super().__init__()
        self.pipeline_options = pipeline_options
        self.team_stats_inmemory_io_connector = team_stats_inmemory_io_connector
        self.team_stats_database_io_connector = team_stats_database_io_connector
        self.failure_database_io_connector = failure_database_io_connector
        self.failure_log_io_connector = failure_log_io_connector

    def compose(self, pipeline: Pipeline) -> Pipeline:
        result_outputs, result_failures = (
                pipeline
                | 'Read in memory team stats' >> self.team_stats_inmemory_io_connector.read_team_stats()
                | 'Team stats domain transform' >> TeamStatsTransform()
        )

        result_outputs | 'Write team stats to db' >> self.team_stats_database_io_connector.write_team_stats()

        result_failures | 'Transform log failures to Cloud logging' >> self.failure_log_io_connector.write_failure_log()
        result_failures | 'Transform sink failures to Bigquery' >> self.failure_database_io_connector.write_failure()

        return pipeline
