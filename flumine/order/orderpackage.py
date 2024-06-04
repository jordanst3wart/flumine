from enum import Enum


class OrderPackageType(Enum):
    PLACE = "Place"
    CANCEL = "Cancel"
    REPLACE = "Replace"
    UPDATE = "Update"
