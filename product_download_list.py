from download_state import DownloadState
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    wait,
    as_completed,
)


class ProductDownloadList(list):
    """List of ProductDownload objects

    Derived from list type
    Implements additional methods for keeping track of multiple ProductDownloads
    """

    def __str__(self):
        return (
            f"Scheduled: {len(self.get_scheduled())}\n"
            f"Downloading: {len(self.get_active())}\n"
            f"Downloaded: {len(self.get_downloaded())}\n"
            f"Extracting: {len(self.get_extracting())}\n"
            f"Extracted: {len(self.get_extracted())}\n"
            f"Total: {len(self)}\n"
        )

    def find(self, future):
        """Search for ProductDownload by future

        Parameters
        ----------
        future : concurrent.futures.Future object

        Returns
        -------
        ProductDownload object with matching future object
        """
        for download in self:
            if download.future == future:
                return download

    def wait_for_completed(self):
        """Wait until first download completes
        """
        wait(self.get_active_futures(), return_when="FIRST_COMPLETED")

    def get(self, state):
        """Return all elements with matching state

        Parameters
        ----------
        state : DownloadState
        """
        return [elem for elem in self if elem.state == state]

    def get_scheduled(self):
        """Return all scheduled elements
        """
        return self.get(DownloadState.SCHEDULED)

    def get_active(self):
        """Return all elements that are currently being downloaded
        """
        return self.get(DownloadState.DL_ACTIVE)

    def get_active_futures(self):
        """Return future object to all currently active downloads
        """
        return [active.future for active in self.get_active()]

    def get_downloaded(self):
        """Return all elements that have been downloaded but not yet extracted
        """
        return self.get(DownloadState.DL_DONE)

    def get_extracting(self):
        """Return all elements that are currently being extracted
        """
        return self.get(DownloadState.EXTRACT_ACTIVE)

    def get_extracted(self):
        """Return all elements that have been extracted
        """
        return self.get(DownloadState.EXTRACT_DONE)

    def get_failed(self):
        """Return all elements that have failed
        """
        return self.get(DownloadState.FAILED)

    def all_downloaded(self):
        """Return true if all scheduled downloads have been completed
        """
        return (
            len(self.get_downloaded())
            + len(self.get_extracting())
            + len(self.get_extracted())
            + len(self.get_failed())
        ) == len(self)

    def size(self):
        """Return sum of downloads in MB
        """
        return sum(elem.size for elem in self if elem.size)