from abc import ABC, abstractmethod

class Notifier(ABC):

    @abstractmethod
    def send_notification(self, channel, message):
        pass
