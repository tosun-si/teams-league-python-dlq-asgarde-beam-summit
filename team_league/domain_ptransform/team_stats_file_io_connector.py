from abc import abstractmethod, ABCMeta

from apache_beam import PTransform


class TeamStatsFileIOConnector(metaclass=ABCMeta):

    @abstractmethod
    def read_team_slogans(self) -> PTransform:
        pass
