from dependency_injector import containers, providers

from team_league.application.team_league_pipeline_composer import TeamLeaguePipelineComposer
from team_league.infrastructure.io.bigquery.failure_bigquery_io_adapter import FailureBigqueryIOAdapter
from team_league.infrastructure.io.bigquery.failure_bigquery_write_transform import FailureBigqueryWriteTransform
from team_league.infrastructure.io.bigquery.team_stats_bigquery_io_adapter import TeamStatsBigqueryIOAdapter
from team_league.infrastructure.io.bigquery.team_stats_bigquery_write_transform import TeamStatsBigqueryWriteTransform
from team_league.infrastructure.io.cloud_logging.failure_cloud_logging_io_adapter import FailureCloudLoggingIOAdapter
from team_league.infrastructure.io.cloud_logging.failure_cloud_logging_write_transform import \
    FailureCloudLoggingWriteTransform
from team_league.infrastructure.io.jsonfile.team_slogan_jsonfile_read_transform import TeamSloganJsonFileReadTransform
from team_league.infrastructure.io.jsonfile.team_stats_jsonfile_io_adapter import TeamStatsJsonFileIOAdapter
from team_league.infrastructure.io.mock.team_stats_mock_io_adapter import TeamStatsMockIOAdapter
from team_league.infrastructure.io.mock.team_stats_mock_read_transform import TeamStatsMockReadTransform


class IOTransforms(containers.DeclarativeContainer):
    config = providers.Configuration()

    read_teams_stats_inmemory_transform = providers.Singleton(TeamStatsMockReadTransform)
    read_teams_slogan_file_transform = providers.Singleton(TeamSloganJsonFileReadTransform,
                                                           pipeline_conf=config)

    write_team_stats_database_transform = providers.Singleton(TeamStatsBigqueryWriteTransform,
                                                              pipeline_conf=config)
    write_failure_database_transform = providers.Singleton(FailureBigqueryWriteTransform,
                                                           pipeline_conf=config)
    write_failure_log_transform = providers.Singleton(FailureCloudLoggingWriteTransform)


class Adapters(containers.DeclarativeContainer):
    io_transforms = providers.DependenciesContainer()

    team_stats_inmemory_io_connector = providers.Singleton(
        TeamStatsMockIOAdapter,
        read_transform=io_transforms.read_teams_stats_inmemory_transform)

    team_stats_database_io_connector = providers.Singleton(
        TeamStatsBigqueryIOAdapter,
        write_transform=io_transforms.write_team_stats_database_transform)

    team_stats_file_io_connector = providers.Singleton(
        TeamStatsJsonFileIOAdapter,
        read_transform=io_transforms.read_teams_slogan_file_transform)

    failure_database_io_connector = providers.Singleton(
        FailureBigqueryIOAdapter,
        write_transform=io_transforms.write_failure_database_transform)

    failure_log_io_connector = providers.Singleton(
        FailureCloudLoggingIOAdapter,
        write_transform=io_transforms.write_failure_log_transform)


class Pipeline(containers.DeclarativeContainer):
    config = providers.Configuration()

    adapters = providers.DependenciesContainer()

    compose_pipeline = providers.Factory(
        TeamLeaguePipelineComposer,
        pipeline_conf=config,
        team_stats_inmemory_io_connector=adapters.team_stats_inmemory_io_connector,
        team_stats_database_io_connector=adapters.team_stats_database_io_connector,
        team_stats_file_io_connector=adapters.team_stats_file_io_connector,
        failure_database_io_connector=adapters.failure_database_io_connector,
        failure_log_io_connector=adapters.failure_log_io_connector
    )
