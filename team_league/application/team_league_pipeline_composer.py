from abc import ABCMeta, abstractmethod
from typing import Dict

from apache_beam import Pipeline, PCollection

from team_league.application.pipeline_conf import PipelineConf
from team_league.domain_ptransform.failure_database_io_connector import FailureDatabaseIOConnector
from team_league.domain_ptransform.failure_log_io_connector import FailureLogIOConnector
from team_league.domain_ptransform.team_stats_database_io_connector import TeamStatsDatabaseIOConnector
from team_league.domain_ptransform.team_stats_file_io_connector import TeamStatsFileIOConnector
from team_league.domain_ptransform.team_stats_inmemory_io_connector import TeamStatsInMemoryIOConnector
from team_league.domain_ptransform.team_stats_transform import TeamStatsTransform


class PipelineComposer(metaclass=ABCMeta):

    @abstractmethod
    def compose(self) -> Pipeline:
        pass


class TeamLeaguePipelineComposer:

    def __init__(self,
                 pipeline_conf: PipelineConf,
                 team_stats_inmemory_io_connector: TeamStatsInMemoryIOConnector,
                 team_stats_database_io_connector: TeamStatsDatabaseIOConnector,
                 team_stats_file_io_connector: TeamStatsFileIOConnector,
                 failure_database_io_connector: FailureDatabaseIOConnector,
                 failure_log_io_connector: FailureLogIOConnector) -> None:
        super().__init__()
        self.pipeline_conf = pipeline_conf
        self.team_stats_inmemory_io_connector = team_stats_inmemory_io_connector
        self.team_stats_database_io_connector = team_stats_database_io_connector
        self.team_stats_file_io_connector = team_stats_file_io_connector
        self.failure_database_io_connector = failure_database_io_connector
        self.failure_log_io_connector = failure_log_io_connector

    def compose(self, pipeline: Pipeline) -> Pipeline:
        slogans_side_inputs: PCollection[Dict] = (
                pipeline
                | 'Read team slogans' >> self.team_stats_file_io_connector.read_team_slogans())

        result_outputs, result_failures = (
                pipeline
                | 'Read in memory team stats' >> self.team_stats_inmemory_io_connector.read_team_stats()
                | 'Team stats domain transform' >> TeamStatsTransform(slogans_side_inputs)
        )

        result_outputs | 'Write team stats to db' >> self.team_stats_database_io_connector.write_team_stats()

        result_failures | 'Transform log failures to Cloud logging' >> self.failure_log_io_connector.write_failure_log()
        result_failures | 'Transform sink failures to Bigquery' >> self.failure_database_io_connector.write_failure()

        return pipeline
