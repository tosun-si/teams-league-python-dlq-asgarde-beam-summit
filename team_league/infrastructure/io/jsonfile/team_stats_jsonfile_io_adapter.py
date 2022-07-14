from apache_beam import PTransform

from team_league.domain_ptransform.team_stats_file_io_connector import TeamStatsFileIOConnector
from team_league.infrastructure.io.jsonfile.team_slogan_jsonfile_read_transform import TeamSloganJsonFileReadTransform


class TeamStatsJsonFileIOAdapter(TeamStatsFileIOConnector):

    def __init__(self,
                 read_transform: TeamSloganJsonFileReadTransform) -> None:
        super().__init__()
        self.read_transform = read_transform

    def read_team_slogans(self) -> PTransform:
        return self.read_transform
