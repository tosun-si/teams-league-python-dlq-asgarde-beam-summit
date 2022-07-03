from dataclasses import dataclass


@dataclass
class TeamScorerRaw:
    scorerFirstName: str
    scorerLastName: str
    goalsNumber: int
    gamesNumber: int
