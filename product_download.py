from download_state import DownloadState
import logging
from time import perf_counter, sleep


def byte_to_MB(size):
    """Convert byte to MegaByte
    """
    return size / 1048576


def MB_per_sec(tic, toc, size):
    """Given start/stop timestamps and a filesize in MB return a MB/s value
    """
    return size / (toc - tic)


class ProductDownload(object):
    """Object for representing a product download

    Originally, this was done inside the SentinelAPIManager class
    using a number of dicts mapping a future object to its respective
    state, UUID, UTM tile etc.
    In order to reduce bloat in the aformentioned class I moved all of
    required functionality for keeping track of multiple downloads
    into ProductDownload and ProductDownloadList
    """

    def __init__(self, uuid, index, utm=None):
        self.uuid = uuid  # Product UUID
        self.utm = utm  # Product MGRS tile
        self.index = index  # Download index
        self.state = DownloadState.SCHEDULED  # Download state

        self.mirror = None  # best mirror available or None
        self.future = None  # future object or None
        self.zip_path = None  # path to zip file or None
        self.safe_path = None  # path to extracted SAFE folder or None
        self._start_time = None  # time of download start
        self._stop_time = None  # time of download stop
        self.size = None  # file size of zip file
        self.speed = None  # download speed
        self.odata = None  # OData for completed download

    def __str__(self):
        return f"UUID: {self.uuid}\nMirror: {self.mirror}\nState: {self.state}"

    def register(self, future):
        """Attach a future object, change state to active and attach done callback
        """
        self.future = future
        self._start_time = perf_counter()
        self.state = DownloadState.DL_ACTIVE
        self.future.add_done_callback(self._done_callback)

    def _done_callback(self, future):
        """Update state to DL_DONE, calculate download speed, assign OData response

        Invoked when a download is completed
        """
        self._stop_time = perf_counter()
        try:
            self.odata = future.result()
        except Exception as err:
            self.state = DownloadState.FAILED
            self.size = 0
            self.speed = 0
            self.zip_path = ""
            # manually acquire module logger
            logger = logging.getLogger("single-mirror")
            logger.info(
                "[%d/%d] UUID %s | Download failed",
                self.index[0],
                self.index[1],
                self.uuid,
            )
            logger.error(str(err))
            return

        self.size = byte_to_MB(self.odata["size"])
        self.speed = MB_per_sec(self._start_time, self._stop_time, self.size)
        self.zip_path = self.odata["path"]
        self.state = DownloadState.DL_DONE

    def _unzip_callback(self, future):
        """Update ProductDownload state to EXTRACT_DONE

        Invoked when unzip is completed
        """
        try:
            self.safe_path = future.result()
        except Exception as err:
            self.state = DownloadState.FAILED
            # manually acquire module logger
            logger = logging.getLogger("single-mirror")
            logger.info(
                "[%d/%d] UUID %s | Unzip failed",
                self.index[0],
                self.index[1],
                self.uuid,
            )
            logger.error(str(err))
            return
        self.state = DownloadState.EXTRACT_DONE