from dataclasses import dataclass


@dataclass
class PipelineConf:
    project_id: str
    input_file_slogans: str
    team_league_dataset: str
    team_stats_table: str
    job_type: str
    failure_output_dataset: str
    failure_output_table: str
    failure_feature_name: str

    @staticmethod
    def to_pipeline_conf(option):
        return PipelineConf(
            project_id=option.project_id,
            input_file_slogans=option.input_file_slogans,
            team_league_dataset=option.team_league_dataset,
            team_stats_table=option.team_stats_table,
            job_type=option.job_type,
            failure_output_dataset=option.failure_output_dataset,
            failure_output_table=option.failure_output_table,
            failure_feature_name=option.failure_feature_name
        )
