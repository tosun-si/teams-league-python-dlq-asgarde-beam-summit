from abc import abstractmethod, ABCMeta

from apache_beam import PTransform


class FailureLogIOConnector(metaclass=ABCMeta):

    @abstractmethod
    def write_failure_log(self) -> PTransform:
        pass
