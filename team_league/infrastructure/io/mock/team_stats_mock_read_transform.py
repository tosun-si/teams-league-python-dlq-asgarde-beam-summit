from typing import List

import apache_beam as beam
from apache_beam import PTransform
from apache_beam.pvalue import PBegin

from team_league.domain.team_scorer_raw import TeamScorerRaw
from team_league.domain.team_stats_raw import TeamStatsRaw

psg_scorers: List[TeamScorerRaw] = [
    TeamScorerRaw(scorerFirstName="Kylian", scorerLastName="Mbappe", goalsNumber=15, gamesNumber=13),
    TeamScorerRaw(scorerFirstName="Sa Silva", scorerLastName="Neymar", goalsNumber=11, gamesNumber=12),
    TeamScorerRaw(scorerFirstName="Angel", scorerLastName="Di Maria", goalsNumber=7, gamesNumber=13),
    TeamScorerRaw(scorerFirstName="Lionel", scorerLastName="Messi", goalsNumber=12, gamesNumber=13),
    TeamScorerRaw(scorerFirstName="Marco", scorerLastName="Verrati", goalsNumber=3, gamesNumber=13)
]

real_scorers: List[TeamScorerRaw] = [
    TeamScorerRaw(scorerFirstName="Karim", scorerLastName="Benzema", goalsNumber=14, gamesNumber=13),
    TeamScorerRaw(scorerFirstName="Junior", scorerLastName="Vinicius", goalsNumber=9, gamesNumber=12),
    TeamScorerRaw(scorerFirstName="Luca", scorerLastName="Modric", goalsNumber=5, gamesNumber=11),
    TeamScorerRaw(scorerFirstName="Silva", scorerLastName="Rodrygo", goalsNumber=7, gamesNumber=13),
    TeamScorerRaw(scorerFirstName="Marco", scorerLastName="Asensio", goalsNumber=6, gamesNumber=13)
]

team_stats: List[TeamStatsRaw] = [
    TeamStatsRaw(teamName="PSG", teamScore=30, scorers=psg_scorers),
    TeamStatsRaw(teamName="Real", teamScore=25, scorers=real_scorers)
]


class TeamStatsMockReadTransform(PTransform):

    def __init__(self):
        super().__init__()

    def expand(self, inputs: PBegin):
        return inputs | 'Read mocked teams stats raw' >> beam.Create(team_stats)
