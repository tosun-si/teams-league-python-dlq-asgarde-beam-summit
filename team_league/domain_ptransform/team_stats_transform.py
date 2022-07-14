from typing import Tuple, Dict

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.pvalue import AsSingleton
from asgarde.collection_composer import CollectionComposer
from asgarde.failure import Failure

from team_league.domain.team_stats import TeamStats
from team_league.domain.team_stats_raw import TeamStatsRaw


class TeamStatsTransform(beam.PTransform):
    def __init__(self, slogans_side_inputs: PCollection[Dict]):
        super().__init__()
        self.slogans_side_inputs = slogans_side_inputs

    def expand(self, inputs: PCollection[TeamStatsRaw]) -> \
            Tuple[PCollection[TeamStats], PCollection[Failure]]:
        result = (CollectionComposer.of(inputs)
                  .map("Validate raw fields", lambda t_raw: t_raw.validate_fields())
                  .map("Compute team stats", TeamStats.compute_team_stats)
                  .map("Add slogan to team stats",
                       self.add_slogan_to_stats,
                       slogans=AsSingleton(self.slogans_side_inputs),
                       setup_action=lambda: '######### Start add slogan to stats actions')
                  )

        return result.outputs, result.failures

    def add_slogan_to_stats(self, team_stats: TeamStats, slogans: Dict) -> TeamStats:
        return team_stats.add_slogan_to_stats(slogans)
