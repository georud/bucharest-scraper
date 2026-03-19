from enum import Enum


class Platform(str, Enum):
    BOOKING = "booking"
    AIRBNB = "airbnb"


class PropertyType(str, Enum):
    HOTEL = "hotel"
    APARTMENT = "apartment"
    HOSTEL = "hostel"
    GUESTHOUSE = "guesthouse"
    VILLA = "villa"
    RESORT = "resort"
    BNB = "bnb"
    OTHER = "other"


class ScrapeStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    NEEDS_REFINEMENT = "needs_refinement"
