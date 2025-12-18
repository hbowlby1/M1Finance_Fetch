import time
import pandas as pd
import requests
import json


class FetchCSV:
    def __init__(self, session, segmentID: str, otherAccountID: str):
        self.session = session
        self.segmentID = segmentID
        self.otherAccountID = otherAccountID

    def fetchTaxLotsCSVs(self):
        df_open = self._fetch_lot_type("OPEN")
        df_closed = self._fetch_lot_type("CLOSED")
        return df_open, df_closed

    def _fetch_lot_type(self, lot_type: str):
        try:
            current_time_ms = str(int(time.time() * 1000))
            url = "https://lens.m1.com/graphql"

            query_string = """query AccountTaxLots($id: ID!, $lotType: LotTypeEnum!, $first: Int, $after: String) {
              node(id: $id) {
                ... on Account {
                  number
                  taxLots(lotType: $lotType, first: $first, after: $after) {
                    pageInfo {
                      hasNextPage
                      endCursor
                      __typename
                    }
                    edges {
                      node {
                        symbol
                        cusip
                        acquisitionDate
                        quantity
                        costBasis
                        shortLongTermHolding
                        unrealizedGainLoss
                        closeDate
                        shortTermRealizedGainLoss
                        longTermRealizedGainLoss
                        washSaleIndicator
                        id
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                  __typename
                }
                __typename
              }
            }"""

            HEADERS = {
                "Host": "lens.m1.com",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.5",
                "Referer": "https://dashboard.m1.com/",
                "content-type": "application/json",
                "x-client-id": "m1-web/10.0.170",
                "x-client-sentinel": current_time_ms,
                "x-segment-id": self.segmentID,
                "x-apollo-operation-name": "AccountTaxLots",  # UPDATED: Must match the query op name
                "Origin": "https://dashboard.m1.com",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
                "authorization": f"Bearer {self.session.access_token}",
            }
            PAYLOAD = {
                "operationName": "AccountTaxLots",
                "variables": {
                    "id": self.otherAccountID,
                    "lotType": lot_type,
                    "first": 2000,
                },
                "query": query_string,
            }

            # Initial request
            response = self.session.post(url, json=PAYLOAD, headers=HEADERS)
            response.raise_for_status()

            # Parse JSON
            json_data = response.json()
            # Check for GraphQL errors
            if "errors" in json_data:
                print(f"GraphQL errors for {lot_type}: {json_data['errors']}")
                return None

            ### fetch the data and convert to CSV DataFrame ###
            data_list = []

            # Check for pagination
            tax_lots = json_data.get("data", {}).get("node", {}).get("taxLots")
            if not tax_lots:
                print(f"No {lot_type.lower()} tax lots data found in response.")
                return pd.DataFrame()  # Return empty DataFrame

            page_info = tax_lots.get("pageInfo", {})
            nextPage = page_info.get("hasNextPage", False)

            if not nextPage:
                edges = tax_lots.get("edges", [])
                data_list.extend(edges)
            else:
                while nextPage:
                    edges = tax_lots.get("edges", [])
                    data_list.extend(edges)
                    endCursor = page_info.get("endCursor")
                    if not endCursor:
                        break
                    PAYLOAD["variables"]["after"] = endCursor
                    try:
                        response = self.session.post(url, json=PAYLOAD, headers=HEADERS)
                        response.raise_for_status()
                        json_data = response.json()
                        if "errors" in json_data:
                            print(f"GraphQL errors in {lot_type} pagination: {json_data['errors']}")
                            break
                        tax_lots = json_data.get("data", {}).get("node", {}).get("taxLots")
                        if not tax_lots:
                            break
                        page_info = tax_lots.get("pageInfo", {})
                        nextPage = page_info.get("hasNextPage", False)
                    except requests.exceptions.RequestException as e:
                        print(f"Request failed during {lot_type} pagination: {e}")
                        break
                    except ValueError as e:
                        print(f"Failed to parse JSON during {lot_type} pagination: {e}")
                        break

            # Convert to DataFrame
            if not data_list:
                print(f"No {lot_type.lower()} data to convert to DataFrame.")
                return pd.DataFrame()

            records = []
            for edge in data_list:
                node = edge.get("node")
                if node:
                    records.append(node)

            df = pd.DataFrame.from_records(records)
            print(f"Successfully fetched {lot_type.lower()} tax lots data.")
            return df

        except requests.exceptions.RequestException as e:
            print(f"Request failed for {lot_type}: {e}")
            return None
        except ValueError as e:
            print(f"Failed to parse JSON response for {lot_type}: {e}")
            return None
        except KeyError as e:
            print(f"Missing expected key in {lot_type} response: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred for {lot_type}: {e}")
            return None