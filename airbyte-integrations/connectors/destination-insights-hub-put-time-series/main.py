#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import sys

from destination_insights_hub_put_time_series import DestinationInsightsHubPutTimeSeries

if __name__ == "__main__":
    DestinationInsightsHubPutTimeSeries().run(sys.argv[1:])
