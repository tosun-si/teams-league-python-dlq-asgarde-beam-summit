from dataclasses import dataclass


@dataclass
class TeamScoreStats:
    topScorerFirstName: str
    topScorerLastName: str
    topScorerGoalsNumber: int
    topScorerGamesNumber: int
    totalScoreNumber: int
