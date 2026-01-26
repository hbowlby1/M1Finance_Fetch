import time
import pandas as pd
import requests
import json
import logging

logger = logging.getLogger(__name__)


class FetchCSV:
    def __init__(self, session, segmentID: str, otherAccountID: str):
        self.session = session
        self.segmentID = segmentID
        self.otherAccountID = otherAccountID

    def _get_headers(self, operation_name="AccountTaxLots"):
        current_time_ms = str(int(time.time() * 1000))
        return {
            "Host": "lens.m1.com",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://dashboard.m1.com/",
            "content-type": "application/json",
            "x-client-id": "m1-web/10.0.170",
            "x-client-sentinel": current_time_ms,
            "x-segment-id": self.segmentID,
            "x-apollo-operation-name": operation_name,
            "Origin": "https://dashboard.m1.com",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "authorization": f"Bearer {self.session.access_token}",
        }

    def fetchTaxLotsCSVs(self):
        '''
        Docstring for fetchTaxLotsCSVs
        this will fetch both open and closed tax lots from the graphQL and
        convert them to CSV dataframes
        
        :param self: Description
        '''
        df_open = self._fetch_lot_type("OPEN")
        df_closed = self._fetch_lot_type("CLOSED")
        return df_open, df_closed

    def _fetch_lot_type(self, lot_type: str):
        try:
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
            headers = self._get_headers("AccountTaxLots")
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
            response = self.session.post(url, json=PAYLOAD, headers=headers)
            response.raise_for_status()

            # Parse JSON
            json_data = response.json()
            # Check for GraphQL errors
            if "errors" in json_data:
                logger.error("GraphQL errors for %s: %s", lot_type, json_data["errors"])
                return None

            ### fetch the data and convert to CSV DataFrame ###
            data_list = []

            # Check for pagination
            tax_lots = json_data.get("data", {}).get("node", {}).get("taxLots")
            if not tax_lots:
                logger.warning("No %s tax lots data found in response.", lot_type.lower())
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
                        response = self.session.post(url, json=PAYLOAD, headers=headers)
                        response.raise_for_status()
                        json_data = response.json()
                        if "errors" in json_data:
                            logger.error("GraphQL errors in %s pagination: %s", lot_type, json_data["errors"])
                            break
                        tax_lots = json_data.get("data", {}).get("node", {}).get("taxLots")
                        if not tax_lots:
                            break
                        page_info = tax_lots.get("pageInfo", {})
                        nextPage = page_info.get("hasNextPage", False)
                    except requests.exceptions.RequestException:
                        logger.exception("Request failed during %s pagination.", lot_type)
                        break
                    except ValueError:
                        logger.exception("Failed to parse JSON during %s pagination.", lot_type)
                        break

            # Convert to DataFrame
            if not data_list:
                logger.warning("No %s data to convert to DataFrame.", lot_type.lower())
                return pd.DataFrame()

            records = []
            for edge in data_list:
                node = edge.get("node")
                if node:
                    records.append(node)

            df = pd.DataFrame.from_records(records)
            logger.info("Successfully fetched %s tax lots data.", lot_type.lower())
            return df

        except requests.exceptions.RequestException:
            logger.exception("Request failed for %s.", lot_type)
            return None
        except ValueError:
            logger.exception("Failed to parse JSON response for %s.", lot_type)
            return None
        except KeyError:
            logger.exception("Missing expected key in %s response.", lot_type)
            return None
        except Exception:
            logger.exception("An unexpected error occurred for %s.", lot_type)
            return None
        
    def fetchHoldingsCSV(self):
        '''
        Docstring for fetchHoldingsCSV
        this will fetch the holdings from the graphQL and
        convert it to a CSV dataframe
        
        :param self: Description
        '''
        try:
            url = "https://lens.m1.com/graphql"

            query_string = """query InvestmentsTablePagination($accountId: ID!, $first: Int!, $after: String, $positionsSort: [PositionSortOptionInput!]!) {
              account: node(id: $accountId) {
                ... on Account {
                  __typename
                  id
                  originator
                  ...BorrowAccount
                  balance {
                    investments {
                      hasPositions
                      ...Investments
                      __typename
                    }
                    __typename
                  }
                }
                __typename
              }
            }

            fragment BorrowAccount on Account {
              borrowAccount {
                hasCreditBorrowed
                creditBorrowed
                status {
                  excessMarginEquity
                  requiredMarginEquity
                  marginEquity
                  __typename
                }
                __typename
              }
              __typename
            }

            fragment Investments on Investments {
              positions(first: $first, after: $after, sort: $positionsSort) {
                pageInfo {
                  hasNextPage
                  endCursor
                  __typename
                }
                total
                edges {
                  cursor
                  node {
                    ...Investment
                    __typename
                  }
                  __typename
                }
                __typename
              }
              totalValue {
                isPartial
                value
                __typename
              }
              totalUnrealizedGain {
                gain
                gainPercent
                tooltip {
                  ...AppTooltip
                  __typename
                }
                __typename
              }
              totalCost {
                cost
                isPartial
                tooltip {
                  ...AppTooltip
                  __typename
                }
                __typename
              }
              __typename
            }

            fragment Investment on Position {
              id
              cost {
                averageSharePrice
                cost
                __typename
              }
              marginability {
                maintenanceEquityRequirementPercent
                __typename
              }
              positionSecurity {
                descriptor
                security {
                  __typename
                  id
                  profile {
                    logoUrl
                    __typename
                  }
                  type
                  ... on Security {
                    symbol
                    __typename
                  }
                  ...SliceableCell
                }
                symbol
                __typename
              }
              quantity
              unrealizedGain {
                gain
                gainPercent
                __typename
              }
              value {
                value
                __typename
              }
              __typename
            }

            fragment SliceableCell on Sliceable {
              __typename
              id
              isActive
              name
              ... on Security {
                symbol
                status
                __typename
              }
              ... on SystemPie {
                systemPieStatus: status
                __typename
              }
              ...SliceableLogo
            }

            fragment SliceableLogo on Sliceable {
              __typename
              name
              ... on Security {
                symbol
                profile {
                  logoUrl
                  __typename
                }
                __typename
              }
              ... on SystemPie {
                key
                logoUrl
                categorizationDetails {
                  logoUrl
                  name
                  key
                  __typename
                }
                __typename
              }
              ... on UserPie {
                portfolioLinks {
                  id
                  isRootSlice
                  __typename
                }
                __typename
              }
            }

            fragment AppTooltip on AppTooltip {
              header
              body
              link {
                ...AppLink
                __typename
              }
              icon {
                ...AppImage
                __typename
              }
              __typename
            }

            fragment AppLink on AppLink {
              articleId
              internalPath
              title
              url
              analyticsEvent {
                ...AnalyticsEvent
                __typename
              }
              kind
              size
              underline
              font
              fontWeight
              __typename
            }

            fragment AnalyticsEvent on AppAnalyticsEvent {
              name
              valueParameter
              customParameters {
                name
                value
                __typename
              }
              customBoolParameters {
                name
                value
                __typename
              }
              customNumberParameters {
                name
                value
                __typename
              }
              __typename
            }

            fragment AppImage on AppImage {
              type
              names
              color
              lightTheme {
                scale1xUrl
                scale2xUrl
                scale3xUrl
                __typename
              }
              darkTheme {
                scale1xUrl
                scale2xUrl
                scale3xUrl
                __typename
              }
              __typename
            }"""

            headers = self._get_headers("InvestmentsTablePagination")
            PAYLOAD = {
                "operationName": "InvestmentsTablePagination",
                "variables": {
                    "accountId": self.otherAccountID,
                    "first": 100,
                    "positionsSort": [{"direction": "DESC", "type": "VALUE"}]
                },
                "query": query_string
            }

            # Initial request
            response = self.session.post(url, json=PAYLOAD, headers=headers)
            response.raise_for_status()

            # Parse JSON
            json_data = response.json()
            # Check for GraphQL errors
            if "errors" in json_data:
                logger.error("GraphQL errors for holdings: %s", json_data["errors"])
                return None

            ### fetch the data and convert to CSV DataFrame ###
            data_list = []

            # Check for pagination
            investments = json_data.get("data", {}).get("account", {}).get("balance", {}).get("investments")
            if not investments:
                logger.warning("No holdings data found in response.")
                return pd.DataFrame()  # Return empty DataFrame

            positions = investments.get("positions")
            if not positions:
                logger.warning("No positions data found in response.")
                return pd.DataFrame()

            page_info = positions.get("pageInfo", {})
            nextPage = page_info.get("hasNextPage", False)

            if not nextPage:
                edges = positions.get("edges", [])
                data_list.extend(edges)
            else:
                while nextPage:
                    edges = positions.get("edges", [])
                    data_list.extend(edges)
                    endCursor = page_info.get("endCursor")
                    if not endCursor:
                        break
                    PAYLOAD["variables"]["after"] = endCursor
                    try:
                        response = self.session.post(url, json=PAYLOAD, headers=headers)
                        response.raise_for_status()
                        json_data = response.json()
                        if "errors" in json_data:
                            logger.error("GraphQL errors in holdings pagination: %s", json_data["errors"])
                            break
                        investments = json_data.get("data", {}).get("account", {}).get("balance", {}).get("investments")
                        if not investments:
                            break
                        positions = investments.get("positions")
                        if not positions:
                            break
                        page_info = positions.get("pageInfo", {})
                        nextPage = page_info.get("hasNextPage", False)
                    except requests.exceptions.RequestException:
                        logger.exception("Request failed during holdings pagination.")
                        break
                    except ValueError:
                        logger.exception("Failed to parse JSON during holdings pagination.")
                        break

            # Convert to DataFrame
            if not data_list:
                logger.warning("No holdings data to convert to DataFrame.")
                return pd.DataFrame()

            records = []
            for edge in data_list:
                node = edge.get("node")
                if node:
                    # Flatten the nested structure for CSV
                    positionSecurity = node.get("positionSecurity") or {}
                    cost = node.get("cost") or {}
                    value = node.get("value") or {}
                    unrealizedGain = node.get("unrealizedGain") or {}
                    marginability = node.get("marginability") or {}
                    record = {
                        "symbol": positionSecurity.get("symbol"),
                        "descriptor": positionSecurity.get("descriptor"),
                        "quantity": node.get("quantity"),
                        "average_share_price": cost.get("averageSharePrice"),
                        "total_cost": cost.get("cost"),
                        "current_value": value.get("value"),
                        "unrealized_gain": unrealizedGain.get("gain"),
                        "unrealized_gain_percent": unrealizedGain.get("gainPercent"),
                        "maintenance_margin_percent": marginability.get("maintenanceEquityRequirementPercent")
                    }
                    records.append(record)

            df = pd.DataFrame.from_records(records)
            logger.info("Successfully fetched holdings data.")
            return df

        except requests.exceptions.RequestException:
            logger.exception("Request failed for holdings.")
            return None
        except ValueError:
            logger.exception("Failed to parse JSON response for holdings.")
            return None
        except KeyError:
            logger.exception("Missing expected key in holdings response.")
            return None
        except Exception:
            logger.exception("An unexpected error occurred for holdings.")
            return None
