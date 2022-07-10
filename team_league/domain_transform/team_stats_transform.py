from typing import Tuple

import apache_beam as beam
from apache_beam import PCollection
from asgarde.collection_composer import CollectionComposer
from asgarde.failure import Failure

from team_league.domain.team_stats import TeamStats
from team_league.domain.team_stats_raw import TeamStatsRaw


class TeamStatsTransform(beam.PTransform):
    def __init__(self):
        super().__init__()

    def expand(self, inputs: PCollection[TeamStatsRaw]) -> \
            Tuple[PCollection[TeamStats], PCollection[Failure]]:
        result = (CollectionComposer.of(inputs)
                  .map("Validate raw fields", lambda t_raw: t_raw.validate_fields())
                  .map("Compute team stats", TeamStats.compute_team_stats)
                  .map("Add slogan to team stats", lambda t_stats: t_stats.add_slogan_to_stats()))

        return result.outputs, result.failures
