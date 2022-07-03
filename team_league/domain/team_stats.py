from __future__ import annotations

from dataclasses import dataclass, replace
from typing import List

from team_league.domain.team_score_stats import TeamScoreStats
from team_league.domain.team_scorer_raw import TeamScorerRaw
from team_league.domain.team_stats_raw import TeamStatsRaw

TEAM_SLOGANS = {
    "PSG": "Paris est magique",
    "Real": "Hala Madrid"
}


@dataclass
class TeamStats:
    teamName: str
    teamScore: int
    teamSlogan: str
    scoreStats: TeamScoreStats

    @staticmethod
    def computeTeamStats(team_stats_raw: TeamStatsRaw) -> TeamStats:
        team_scorers: List[TeamScorerRaw] = team_stats_raw.scorers
        top_scorer: TeamScorerRaw = max(team_scorers, key=lambda team_scorer: team_scorer.goalsNumber)

        total_score_team: int = sum(map(lambda t: t.goalsNumber, team_scorers))

        scoreStats: TeamScoreStats = TeamScoreStats(
            topScorerFirstName=top_scorer.scorerFirstName,
            topScorerLastName=top_scorer.scorerLastName,
            topScorerGoalsNumber=top_scorer.goalsNumber,
            topScorerGamesNumber=top_scorer.gamesNumber,
            totalScoreNumber=total_score_team)

        return TeamStats(
            teamName=team_stats_raw.teamName,
            teamScore=team_stats_raw.teamScore,
            teamSlogan='',
            scoreStats=scoreStats
        )

    def add_slogan_to_stats(self) -> TeamStats:
        team_slogan: str = TEAM_SLOGANS.get(self.teamName)

        if team_slogan is None:
            raise AttributeError(f'No slogan for team : {team_slogan}')

        return replace(self, teamSlogan=team_slogan)
