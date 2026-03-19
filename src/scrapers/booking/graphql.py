from __future__ import annotations

import json
from datetime import date, timedelta


def build_search_query(
    dest_id: str,
    dest_type: str,
    checkin: date,
    checkout: date,
    adults: int,
    rooms: int,
    ne_lat: float,
    ne_lng: float,
    sw_lat: float,
    sw_lng: float,
    offset: int = 0,
    rows_per_page: int = 100,
) -> dict:
    """Build the GraphQL request body for FullSearch / searchQueries."""
    return {
        "operationName": "FullSearch",
        "variables": {
            "input": {
                "acidpiN": True,
                "boundingBox": {
                    "neLatitude": ne_lat,
                    "neLongitude": ne_lng,
                    "swLatitude": sw_lat,
                    "swLongitude": sw_lng,
                },
                "dates": {
                    "checkin": checkin.isoformat(),
                    "checkout": checkout.isoformat(),
                },
                "destination": {
                    "id": dest_id,
                    "type": dest_type,
                },
                "filters": {},
                "nbAdults": adults,
                "nbChildren": 0,
                "nbRooms": rooms,
                "pagination": {
                    "offset": offset,
                    "rowsPerPage": rows_per_page,
                },
                "sorters": {
                    "selectedSorter": None,
                    "referenceGeoId": None,
                },
            },
        },
        "query": FULL_SEARCH_QUERY,
    }


def build_map_markers_query(
    ne_lat: float,
    ne_lng: float,
    sw_lat: float,
    sw_lng: float,
    dest_id: str,
    dest_type: str,
    adults: int,
    rooms: int,
    checkin: date | None = None,
    checkout: date | None = None,
) -> dict:
    """Build the GraphQL request body for MapMarkersDesktop."""
    input_vars: dict = {
        "boundingBox": {
            "neLatitude": ne_lat,
            "neLongitude": ne_lng,
            "swLatitude": sw_lat,
            "swLongitude": sw_lng,
        },
        "destination": {
            "id": dest_id,
            "type": dest_type,
        },
        "nbAdults": adults,
        "nbChildren": 0,
        "nbRooms": rooms,
    }
    if checkin and checkout:
        input_vars["dates"] = {
            "checkin": checkin.isoformat(),
            "checkout": checkout.isoformat(),
        }
    return {
        "operationName": "MapMarkersDesktop",
        "variables": {"input": input_vars},
        "query": MAP_MARKERS_QUERY,
    }


def get_dates(checkin_offset: int, checkout_offset: int) -> tuple[date, date]:
    """Calculate check-in and check-out dates from today."""
    today = date.today()
    return today + timedelta(days=checkin_offset), today + timedelta(days=checkout_offset)


# Minimal GraphQL query for search results
FULL_SEARCH_QUERY = """
query FullSearch($input: SearchQueryInput!) {
  searchQueries {
    search(input: $input) {
      results {
        ... on SearchResultProperty {
          id: idDetail
          name
          basicPropertyData {
            id
            starRating {
              value
            }
            location {
              address
              city
              countryCode
            }
            photos {
              main {
                highResJpegUrl {
                  relativeUrl
                }
              }
            }
          }
          displayName {
            text
          }
          blocks {
            finalPrice {
              amount
              currency
            }
          }
          location {
            displayLocation
          }
          reviews {
            totalScore
            reviewsCount
          }
          geoDistanceFromSearch {
            label
          }
        }
      }
      pagination {
        nbResultsTotal
      }
      mapBoundingBox {
        neLatitude
        neLongitude
        swLatitude
        swLongitude
      }
      breadcrumbs {
        name
      }
    }
  }
}
"""

# Lighter query for map markers (coordinates + basic info only)
MAP_MARKERS_QUERY = """
query MapMarkersDesktop($input: MapMarkerInput!) {
  mapMarkers(input: $input) {
    ... on MapPropertyMarker {
      propertyId
      name
      coordinate {
        latitude
        longitude
      }
      priceInfo {
        amount
        currency
      }
      reviewScore
      reviewCount
      starRating
      propertyType
    }
    ... on MapClusterMarker {
      coordinate {
        latitude
        longitude
      }
      propertyCount
    }
  }
}
"""
