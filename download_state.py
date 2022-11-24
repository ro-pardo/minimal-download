from enum import Enum


class DownloadState(Enum):
    """Represent state of a product download
    """

    FAILED = -1  # Download failed
    SCHEDULED = 0  # Scheduled for download
    DL_ACTIVE = 1  # Download active
    DL_DONE = 2  # Download finished
    EXTRACT_ACTIVE = 3  # Extraction active
    EXTRACT_DONE = 4  # Extraction finished