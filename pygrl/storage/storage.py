from abc import ABC, abstractmethod
from typing import Any


class Storage(ABC):
    """
    Abstract class for storage.

    Methods:
    --------
    get(key: str) -> Any
        Gets the value associated with the key.

    set(key: str, value: Any) -> None
        Sets the value associated with the key.
    """

    @abstractmethod
    def get(self, key: str) -> Any:
        """
        Gets the value associated with the key.

        Parameters
        ----------
        key : str
            The key to get the value for.

        Returns
        -------
        Any
            The value associated with the key.
        """
        pass

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        """
        Sets the value associated with the key.

        Parameters
        ----------
        key : str
            The key to set the value for.
        value : Any
            The value to set.
        """
        pass

    @abstractmethod
    def drop(self, key: str):
        """
        Drops the value associated with the key.

        Parameters
        ----------
        key : str
            The key to drop the value for.
        """
        pass

    @abstractmethod
    def clear(self):
        """
        Clears all the values in the storage.
        """
        pass

    @abstractmethod
    def keys(self) -> list:
        """
        Returns all the keys in the storage.
        """
        pass
