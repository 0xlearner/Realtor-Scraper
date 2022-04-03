import scrapy
import csv
import json
from datetime import datetime


def parse_date(dt):
    dateobj = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z")
    return dateobj.strftime("%d-%m-%Y")


class RealtorScraperSpider(scrapy.Spider):
    name = "realtor-exp"

    base_url = "https://www.realtor.com/api/v1/hulk?client_id=rdc-x&schema=vesta"

    headers = {
        "authority": "www.realtor.com",
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "max-age=0",
        "content-type": "application/json",
        "origin": "https://www.realtor.com",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sec-gpc": "1",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36",
    }

    json_data = {
        "query": "",
        "variables": {},
        "callfrom": "PDP",
        "isClient": True,
    }

    def start_requests(self):
        prop_id = []
        with open("items_realtor_property_3.csv", "r") as file:
            data = csv.DictReader(file)
            for col in data:
                prop_id.append(col["id"])

        # for id in prop_id:
        #     print(id)
        #     if "4541373160" in prop_id:
        #         prop_id.remove("4541373160")
        id = "4541373160"
        self.json_data[
            "query"
        ] = f'{{\n    home(property_id: "{id}") {{\n      advertisers {{\n        team_name\n        address {{\n          city\n          country\n          line\n          postal_code\n          state\n          state_code\n        }}\n        builder {{\n          fulfillment_id\n        }}\n        broker {{\n          accent_color\n          designations\n          fulfillment_id\n          name\n          logo\n        }}\n        email\n        fulfillment_id\n        href\n        mls_set\n        name\n        nrds_id\n        office {{\n          address {{\n            city\n            coordinate {{\n              lat\n              lon\n            }}\n            country\n            line\n            postal_code\n            state\n            state_code\n          }}\n          application_url\n          email\n          lead_email {{\n            to\n            cc\n          }}\n          fulfillment_id\n          hours\n          href\n          mls_set\n          out_of_community\n          name\n          phones {{\n            ext\n            number\n            primary\n            trackable\n            type\n          }}\n          photo {{\n            href\n          }}\n          slogan\n        }}\n        phones {{\n          ext\n          number\n          primary\n          trackable\n          type\n        }}\n        photo {{\n          href\n        }}\n        slogan\n        type\n      }}\n      buyers {{\n        address {{\n          city\n          country\n          line\n          postal_code\n          state\n          state_code\n        }}\n        broker {{\n          accent_color\n          designations\n          fulfillment_id\n          name\n          logo\n        }}\n        email\n        fulfillment_id\n        href\n        mls_set\n        name\n        nrds_id\n        office {{\n          address {{\n            city\n            coordinate {{\n              lat\n              lon\n            }}\n            country\n            line\n            postal_code\n            state\n            state_code\n          }}\n          application_url\n          email\n          lead_email {{\n            to\n            cc\n          }}\n          fulfillment_id\n          hours\n          href\n          mls_set\n          out_of_community\n          name\n          phones {{\n            ext\n            number\n            primary\n            trackable\n            type\n          }}\n          photo {{\n            href\n          }}\n          slogan\n        }}\n        phones {{\n          ext\n          number\n          primary\n          trackable\n          type\n        }}\n        photo {{\n          href\n        }}\n        slogan\n        type\n      }}\n      community {{\n        permalink\n      }}\n      estimates {{\n        current_values(source: "corelogic")\n        @include_if(field: "status", operator: in, value: "sold,off_market,other") {{\n          estimate\n          estimate_high\n          estimate_low\n          date\n          source {{\n            type\n            name\n          }}\n        }}\n      }}\n      days_on_market\n      description {{\n        baths\n        baths_3qtr\n        baths_full\n        baths_full_calc\n        baths_half\n        baths_max\n        baths_min\n        baths_partial_calc\n        baths_total\n        beds\n        beds_max\n        beds_min\n        construction\n        cooling\n        exterior\n        fireplace\n        garage\n        garage_max\n        garage_min\n        garage_type\n        heating\n        logo {{\n          href\n        }}\n        lot_sqft\n        name\n        pool\n        roofing\n        rooms\n        sqft\n        sqft_max\n        sqft_min\n        stories\n        styles\n        sub_type\n        text\n        type\n        units\n        year_built\n        year_renovated\n        zoning\n      }}\n      details {{\n        category\n        parent_category\n        text\n      }}\n      flags {{\n        is_coming_soon\n        is_contingent\n        is_deal_available\n        is_for_rent\n        is_foreclosure\n        is_garage_present\n        is_new_construction\n        is_pending\n        is_price_excludes_land\n        is_senior_community\n        is_short_sale\n        is_subdivision\n      }}\n      href\n      last_sold_date\n      last_sold_price\n      list_date\n      list_price\n      listing_id\n      local {{\n        flood {{\n          firststreet_url\n          fsid\n          flood_factor_score\n          flood_factor_severity\n          environmental_risk\n          trend_direction\n          fema_zone\n          insurance_quotes{{\n            provider_url\n            provider_name\n            provider_logo\n            expires\n            price\n            home_coverage\n            contents_coverage\n            disclaimer\n          }}\n        }}\n        noise {{\n          score\n        }}\n      }}\n      location {{\n        address {{\n          city\n          coordinate {{\n            lat\n            lon\n          }}\n          country\n          line\n          postal_code\n          state\n          state_code\n          street_direction\n          street_name\n          street_number\n          street_post_direction\n          street_suffix\n          unit\n          validation_code\n        }}\n        county {{\n          fips_code\n          name\n          state_code\n        }}\n        neighborhoods {{\n          city\n          id\n          level\n          name\n          state_code\n          slug_id\n        }}\n        search_areas {{\n          city\n          state_code\n        }}\n      }}\n      nearby_schools {{\n        schools {{\n          coordinate {{\n            lat\n            lon\n          }}\n          distance_in_miles\n          district {{\n            id\n            name\n          }}\n          education_levels\n          funding_type\n          grades\n          greatschools_id\n          id\n          name\n          nces_code\n          parent_rating\n          rating\n          review_count\n          slug_id\n          student_count\n        }}\n      }}\n      photo_count\n      photos {{\n        title\n        description\n        href\n        type\n      }}\n      primary_photo {{\n        href\n      }}\n      property_history {{\n        date\n        event_name\n        price\n        price_sqft\n        source_listing_id\n        source_name\n        listing @include_if(field: "status", operator: in, value: "sold,off_market,other") {{\n          list_price\n          last_status_change_date\n          last_update_date\n          status\n          list_date\n          listing_id\n          suppression_flags\n          photos {{\n            href\n          }}\n          description {{\n            text\n          }}\n          advertisers {{\n            fulfillment_id\n            nrds_id\n            name\n            email\n            href\n            slogan\n            office {{\n              fulfillment_id\n              name\n              email\n              href\n              slogan\n              out_of_community\n              application_url\n              mls_set\n            }}\n            broker {{\n              fulfillment_id\n              name\n              accent_color\n              logo\n            }}\n            type\n            mls_set\n          }}\n          buyers {{\n            fulfillment_id\n            nrds_id\n            name\n            email\n            href\n            slogan\n            type\n            mls_set\n            address {{\n              line\n              city\n              postal_code\n              state_code\n              state\n              country\n              coordinate {{\n                lat\n                lon\n              }}\n            }}\n            office {{\n              fulfillment_id\n              name\n              email\n              href\n              slogan\n              hours\n              out_of_community\n              application_url\n              mls_set\n              address {{\n                line\n                city\n                postal_code\n                state_code\n                state\n                country\n              }}\n              phones {{\n                number\n                type\n                primary\n                trackable\n                ext\n              }}\n              county {{\n                name\n              }}\n            }}\n            phones {{\n              number\n              type\n              primary\n              trackable\n              ext\n            }}\n            broker {{\n              fulfillment_id\n              name\n              accent_color\n              logo\n            }}\n          }}\n          source {{\n            id\n            agents {{\n              agent_id\n              agent_name\n              office_id\n              office_name\n              office_phone\n              type\n            }}\n          }}\n        }}\n      }}\n      property_id\n      provider_url {{\n        href\n        level\n        type\n      }}\n      source {{\n        agents {{\n          agent_id\n          agent_name\n          id\n          office_id\n          office_name\n          office_phone\n          type\n        }}\n        disclaimer {{\n          href\n          logo {{\n            href\n            height\n            width\n          }}\n          text\n        }}\n        id\n        plan_id\n        listing_id\n        name\n        raw {{\n          status\n          style\n          tax_amount\n        }}\n        type\n        community_id\n      }}\n      status\n      suppression_flags\n      tags\n      tax_history {{\n        assessment {{\n          building\n          land\n          total\n        }}\n        market {{\n          building\n          land\n          total\n        }}\n        tax\n        year\n      }}\n    }}\n  }}'

        yield scrapy.Request(
            url=self.base_url,
            method="POST",
            dont_filter=True,
            headers=self.headers,
            body=json.dumps(self.json_data),
            callback=self.parse_details,
        )

    def parse_details(self, response):
        item = {}
        prop_detail = response.json()

        try:
            prop_history = prop_detail["data"]["home"]["property_history"]

            if prop_history != None:
                history_date = []
                for idx, prop in enumerate(prop_history):
                    history_date.append(prop["date"][idx])
                print(history_date)
                print(len(history_date))

                # for idx, dt in enumerate(history_date):
                #     item[f"property_history_date_{idx}"] = dt

                # history_event = [e["event_name"] for e in prop_history]
                # print(len(history_event))

                # for idx, evnt in enumerate(history_event):
                #     item[f"property_history_event_{idx}"] = evnt

                # history_price = [pri["price"] for pri in prop_history]
                # print(len(history_price))

                # for idx, price in enumerate(history_price):
                #     item[f"property_history_price_{idx}"] = price

                # history_price_sqft = [
                #     pri_sqft["price_sqft"] for pri_sqft in prop_history
                # ]
                # print(len(history_price_sqft))

                # for idx, price_sqft in enumerate(history_price_sqft):
                #     item[f"property_history_price_sqft_{idx}"] = price_sqft

                # history_source_listing_id = [
                #     sld["source_listing_id"] for sld in prop_history
                # ]
                # print(len(history_source_listing_id))

                # for idx, source_listing_id in enumerate(history_source_listing_id):
                #     item[
                #         f"property_history_source_listing_id_{idx}"
                #     ] = source_listing_id

                # history_source_name = [sn["source_name"] for sn in prop_history]
                # print(len(history_source_name))

                # for idx, source_name in enumerate(history_source_name):
                #     item[f"property_history_source_name_{idx}"] = source_name

                # if prop_history[0]["listing"] != None:
                #     history_listing_list_price = [
                #         l["listing"]["list_price"] for l in prop_history
                #     ]
                #     print(len(history_listing_list_price))
                #     history_listing_list_change_date = [
                #         parse_date(l["listing"]["last_status_change_date"])
                #         for l in prop_history
                #     ]
                #     print(len(history_listing_list_change_date))
                #     history_listing_list_update_date = [
                #         parse_date(l["listing"]["last_update_date"])
                #         for l in prop_history
                #     ]
                #     print(len(history_listing_list_update_date))
                #     history_listing_list_status = [
                #         l["listing"]["status"] for l in prop_history
                #     ]
                #     print(len(history_listing_list_status))
                #     history_listing_list_date = [
                #         parse_date(l["listing"]["list_date"]) for l in prop_history
                #     ]
                #     print(len(history_listing_list_date))

                # for idx, listing_price in enumerate(history_listing_list_price):
                #     item[f"property_history_listing_price_{idx}"] = listing_price

                # for idx, listing_change_date in enumerate(
                #     history_listing_list_change_date
                # ):
                #     item[
                #         f"property_history_listing_change_date_{idx}"
                #     ] = listing_change_date

                # for idx, listing_update_date in enumerate(
                #     history_listing_list_update_date
                # ):
                #     item[
                #         f"property_history_listing_update_date_{idx}"
                #     ] = listing_update_date

                # for idx, listing_status in enumerate(history_listing_list_status):
                #     item[f"property_history_listing_status_{idx}"] = listing_status

                # for idx, listing_date in enumerate(history_listing_list_date):
                #     item[f"property_history_listing_date_{idx}"] = listing_date

        except:
            pass

        yield item
