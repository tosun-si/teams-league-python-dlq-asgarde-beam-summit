from abc import abstractmethod, ABCMeta

from apache_beam import PTransform


class FailureDatabaseIOConnector(metaclass=ABCMeta):

    @abstractmethod
    def write_failure(self) -> PTransform:
        pass
