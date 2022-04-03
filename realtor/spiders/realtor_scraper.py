import scrapy
import csv
import json
from datetime import datetime


def parse_date(dt):
    dateobj = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S%z")
    return dateobj.strftime("%d-%m-%Y")


class RealtorScraperSpider(scrapy.Spider):
    name = "realtor-scraper"

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

        for id in prop_id:
            print(id)
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

        item["property_id"] = prop_detail["data"]["home"]["property_id"]
        item["property_link"] = prop_detail["data"]["home"]["href"]
        item["city"] = prop_detail["data"]["home"]["location"]["address"]["city"]
        item["lat"] = ""
        item["lon"] = ""
        item["address"] = prop_detail["data"]["home"]["location"]["address"]["line"]
        item["postcode"] = prop_detail["data"]["home"]["location"]["address"][
            "postal_code"
        ]
        item["state"] = prop_detail["data"]["home"]["location"]["address"]["state"]
        item["state_code"] = prop_detail["data"]["home"]["location"]["address"][
            "state_code"
        ]
        item["street_name"] = prop_detail["data"]["home"]["location"]["address"][
            "street_name"
        ]
        item["street_num"] = prop_detail["data"]["home"]["location"]["address"][
            "street_number"
        ]
        item["street_suffix"] = prop_detail["data"]["home"]["location"]["address"][
            "street_suffix"
        ]
        item["listing_status"] = prop_detail["data"]["home"]["status"]
        item["homestyle"] = ""
        item["price"] = prop_detail["data"]["home"]["list_price"]
        item["listing_date"] = ""
        item["last_sold_price"] = prop_detail["data"]["home"]["last_sold_price"]
        item["flood_factor_score"] = ""
        item["flood_factor_severity"] = ""
        item["listing_raw_status"] = ""
        try:
            item["last_sold_date"] = prop_detail["data"]["home"]["last_sold_date"]

        except:
            item["last_sold_date"] = ""
        item["environmental_risk"] = ""
        item["fema_zone"] = ""
        item["noise_score"] = ""
        item["baths"] = prop_detail["data"]["home"]["description"]["baths"]
        item["baths_3qtr"] = prop_detail["data"]["home"]["description"]["baths_3qtr"]
        item["baths_full"] = prop_detail["data"]["home"]["description"]["baths_full"]
        item["baths_full_calc"] = prop_detail["data"]["home"]["description"][
            "baths_full_calc"
        ]
        item["baths_half"] = prop_detail["data"]["home"]["description"]["baths_half"]
        item["baths_max"] = prop_detail["data"]["home"]["description"]["baths_max"]
        item["baths_min"] = prop_detail["data"]["home"]["description"]["baths_min"]
        item["baths_partial_calc"] = prop_detail["data"]["home"]["description"][
            "baths_partial_calc"
        ]
        item["baths_total"] = prop_detail["data"]["home"]["description"]["baths_total"]
        item["beds"] = prop_detail["data"]["home"]["description"]["beds"]
        item["beds_max"] = prop_detail["data"]["home"]["description"]["beds_max"]
        item["beds_min"] = prop_detail["data"]["home"]["description"]["beds_min"]
        item["construction"] = prop_detail["data"]["home"]["description"][
            "construction"
        ]
        item["cooling"] = prop_detail["data"]["home"]["description"]["cooling"]
        item["exterior"] = prop_detail["data"]["home"]["description"]["exterior"]
        item["fireplace"] = prop_detail["data"]["home"]["description"]["fireplace"]
        item["garage"] = prop_detail["data"]["home"]["description"]["garage"]
        item["garage_max"] = prop_detail["data"]["home"]["description"]["garage_max"]
        item["garage_min"] = prop_detail["data"]["home"]["description"]["garage_min"]
        item["garage_type"] = prop_detail["data"]["home"]["description"]["garage_type"]
        item["heating"] = prop_detail["data"]["home"]["description"]["heating"]
        item["lot_sqft"] = prop_detail["data"]["home"]["description"]["lot_sqft"]
        item["pool"] = prop_detail["data"]["home"]["description"]["pool"]
        item["rooms"] = prop_detail["data"]["home"]["description"]["rooms"]
        item["sqft"] = prop_detail["data"]["home"]["description"]["sqft"]
        item["sqft_max"] = prop_detail["data"]["home"]["description"]["sqft_max"]
        item["sqft_min"] = prop_detail["data"]["home"]["description"]["sqft_min"]
        item["stories"] = prop_detail["data"]["home"]["description"]["stories"]
        item["type"] = prop_detail["data"]["home"]["description"]["type"]
        item["year_built"] = prop_detail["data"]["home"]["description"]["year_built"]
        item["year_renovated"] = prop_detail["data"]["home"]["description"][
            "year_renovated"
        ]
        item["community_features"] = ""
        item["unit_features"] = ""
        item["bedrooms"] = ""
        item["total_rooms"] = ""
        item["basement_description"] = ""
        item["appliances"] = ""
        item["heating_feature"] = ""
        item["cooling_feature"] = ""
        item["bathrooms"] = ""
        item["interior"] = ""
        item["exterior_lot_features"] = ""
        item["lot_size_acres"] = ""
        item["lot_size_square_feet"] = ""
        item["parking_feature"] = ""
        item["asscociation"] = ""
        item["asscociation_fee"] = ""
        item["asscociation_frequency"] = ""
        item["asscociation_includes"] = ""
        item["calculated_total_monthly_association_fees"] = ""
        item["school_info"] = ""
        item["source_listing_status"] = ""
        item["county"] = ""
        item["cross_street"] = ""
        item["source_property_type"] = ""
        item["property_subtype"] = ""
        item["parcel_number"] = ""
        item["total_sqft_living"] = ""
        item["construction_material"] = ""
        item["foundation_details"] = ""
        item["levels"] = ""
        item["property_age"] = ""
        item["roof_type"] = ""
        item["sewer"] = ""
        item["water_source"] = ""
        item["tags"] = prop_detail["data"]["home"]["tags"]
        details = prop_detail["data"]["home"]["details"]
        try:
            prop_history = prop_detail["data"]["home"]["property_history"]

            if prop_history != None:
                for history in prop_history:
                    history_date = [p["date"] for p in prop_history]

                    for idx, dt in enumerate(history_date):
                        item[f"property_history_date_{idx}"] = dt

                    history_event = [e["event_name"] for e in prop_history]

                    for idx, evnt in enumerate(history_event):
                        item[f"property_history_event_{idx}"] = evnt

                    history_price = [pri["price"] for pri in prop_history]

                    for idx, price in enumerate(history_price):
                        item[f"property_history_price_{idx}"] = price

                    history_price_sqft = [
                        pri_sqft["price_sqft"] for pri_sqft in prop_history
                    ]

                    for idx, price_sqft in enumerate(history_price_sqft):
                        item[f"property_history_price_sqft_{idx}"] = price_sqft

                    history_source_listing_id = [
                        sld["source_listing_id"] for sld in prop_history
                    ]

                    for idx, source_listing_id in enumerate(history_source_listing_id):
                        item[
                            f"property_history_source_listing_id_{idx}"
                        ] = source_listing_id

                    history_source_name = [sn["source_name"] for sn in prop_history]

                    for idx, source_name in enumerate(history_source_name):
                        item[f"property_history_source_name_{idx}"] = source_name

                    if history["listing"] != None:
                        history_listing_list_price = [
                            l["listing"]["list_price"] for l in prop_history
                        ]
                        history_listing_list_change_date = [
                            parse_date(l["listing"]["last_status_change_date"])
                            for l in prop_history
                        ]
                        history_listing_list_update_date = [
                            parse_date(l["listing"]["last_update_date"])
                            for l in prop_history
                        ]
                        history_listing_list_status = [
                            l["listing"]["status"] for l in prop_history
                        ]
                        history_listing_list_date = [
                            parse_date(l["listing"]["list_date"]) for l in prop_history
                        ]

                    for idx, listing_price in enumerate(history_listing_list_price):
                        item[f"property_history_listing_price_{idx}"] = listing_price

                    for idx, listing_change_date in enumerate(
                        history_listing_list_change_date
                    ):
                        item[
                            f"property_history_listing_change_date_{idx}"
                        ] = listing_change_date

                    for idx, listing_update_date in enumerate(
                        history_listing_list_update_date
                    ):
                        item[
                            f"property_history_listing_update_date_{idx}"
                        ] = listing_update_date

                    for idx, listing_status in enumerate(history_listing_list_status):
                        item[f"property_history_listing_status_{idx}"] = listing_status

                    for idx, listing_date in enumerate(history_listing_list_date):
                        item[f"property_history_listing_date_{idx}"] = listing_date

        except:
            pass
        try:
            prop_tax = prop_detail["data"]["home"]["tax_history"]
            if prop_tax != None:
                tax_building_assessment = [
                    tab["assessment"]["building"] for tab in prop_tax
                ]

                for idx, tab in enumerate(tax_building_assessment):
                    item[f"property_history_tax_building_assessment_{idx}"] = tab

                tax_landing_assessment = [tal["assessment"]["land"] for tal in prop_tax]

                for idx, tla in enumerate(tax_landing_assessment):
                    item[f"property_history_tax_landing_assessment_{idx}"] = tla

                tax_total_assessment = [tat["assessment"]["total"] for tat in prop_tax]

                for idx, tta in enumerate(tax_total_assessment):
                    item[f"property_history_tax_total_assessment_{idx}"] = tta

                tax_building_market = [tmb["market"]["building"] for tmb in prop_tax]

                for idx, tbm in enumerate(tax_building_market):
                    item[f"property_history_tax_building_market_{idx}"] = tbm

                tax_land_market = [tml["market"]["land"] for tml in prop_tax]

                for idx, tlm in enumerate(tax_land_market):
                    item[f"property_history_tax_land_market_{idx}"] = tlm

                tax_total_market = [tmt["market"]["total"] for tmt in prop_tax]

                for idx, ttm in enumerate(tax_total_market):
                    item[f"property_history_tax_total_market_{idx}"] = ttm

                tax = [tx["tax"] for tx in prop_tax]

                for idx, tx in enumerate(tax):
                    item[f"property_history_tax_{idx}"] = tx

                tax_year = [txy["year"] for txy in prop_tax]

                for idx, txy in enumerate(tax_year):
                    item[f"property_history_tax_year_{idx}"] = txy
        except:
            pass
        try:
            item["broker_email"] = prop_detail["data"]["home"]["advertisers"][0][
                "email"
            ]
            item["broker_name"] = prop_detail["data"]["home"]["advertisers"][0]["name"]
            item["broker_city"] = prop_detail["data"]["home"]["advertisers"][0][
                "office"
            ]["address"]["city"]

            item["broker_country"] = prop_detail["data"]["home"]["advertisers"][0][
                "office"
            ]["address"]["country"]
            item["broker_line"] = prop_detail["data"]["home"]["advertisers"][0][
                "office"
            ]["address"]["line"]
            item["broker_state_code"] = prop_detail["data"]["home"]["advertisers"][0][
                "office"
            ]["address"]["state_code"]
            item["broker_office_name"] = prop_detail["data"]["home"]["advertisers"][0][
                "office"
            ]["name"]
            item["broker_phone_1"] = prop_detail["data"]["home"]["advertisers"][0][
                "office"
            ]["phones"][0]["number"]
            item["broker_phone_2"] = prop_detail["data"]["home"]["advertisers"][0][
                "phones"
            ][0]["number"]
            item["broker_phone_type"] = prop_detail["data"]["home"]["advertisers"][0][
                "office"
            ]["phones"][0]["type"]
        except:
            item["broker_email"] = ""
            item["broker_name"] = ""
            item["broker_city"] = ""
            item["broker_country"] = ""
            item["broker_line"] = ""
            item["broker_state_code"] = ""
            item["broker_office_name"] = ""
            item["broker_phone_1"] = ""
            item["broker_phone_2"] = ""
            item["broker_phone_type"] = ""
        try:
            for detail in details:
                if detail["category"] == "Appliances":
                    item["appliances"] = detail["text"]

                if detail["category"] == "Heating and Cooling":
                    for txt in detail["text"]:
                        if "Cooling Features: " in txt:
                            item["cooling_feature"] = txt.replace(
                                "Cooling Features: ", ""
                            )
                        if "Heating Features: " in txt:
                            item["heating_feature"] = txt.replace(
                                "Heating Features: ", ""
                            )

                if detail["category"] == "Unit Features":
                    item["unit_features"] = prop_detail["data"]["home"]["details"][1][
                        "text"
                    ]

                if detail["category"] == "Other Rooms":
                    for txt in detail["text"]:
                        if "Total Rooms: " in txt:
                            item["total_rooms"] = txt.replace("Total Rooms: ", "")
                        if "Basement Description: " in txt:
                            item["basement_description"] = txt.replace(
                                "Basement Description: ", ""
                            )

                if detail["category"] == "Bathrooms":
                    for idx, txt in enumerate(detail["text"]):
                        detail["text"] = txt.replace("Full Bathrooms: ", "")
                    item["bathrooms"] = detail["text"][0]

                if detail["category"] == "Interior Features":
                    for idx, txt in enumerate(detail["text"]):
                        detail["text"] = txt.replace("Window Features: ", "")
                    item["interior"] = detail["text"]

                if detail["category"] == "Exterior and Lot Features":
                    item["exterior_lot_features"] = detail["text"]

                if detail["category"] == "Land Info":
                    for txt in detail["text"]:
                        if "Lot Size Acres: " in txt:
                            item["lot_size_acres"] = txt.replace("Lot Size Acres: ", "")
                        if "Lot Size Square Feet: " in txt:
                            item["lot_size_square_feet"] = txt.replace(
                                "Lot Size Square Feet: ", ""
                            )

                if detail["category"] == "Garage and Parking":
                    for idx, txt in enumerate(detail["text"]):
                        detail["text"] = txt.replace("Parking Features: ", "")
                    item["parking_feature"] = detail["text"]

                if detail["category"] == "Homeowners Association":
                    for idx, txt in enumerate(detail["text"]):
                        detail["text"][idx] = (
                            txt.replace("Association: ", "")
                            .replace("Association Fee: ", "")
                            .replace("Association Fee Frequency: ", "")
                            .replace("Association Fee Includes: ", "")
                            .replace("Calculated Total Monthly Association Fees: ", "")
                        )
                    try:
                        item["asscociation"] = detail["text"][0]
                        item["asscociation_fee"] = detail["text"][1]
                        item["asscociation_frequency"] = detail["text"][2]
                        item["asscociation_includes"] = detail["text"][3]
                        item["calculated_total_monthly_association_fees"] = detail[
                            "text"
                        ][4]
                    except:
                        item["asscociation_fee"] = ""
                        item["asscociation_frequency"] = ""
                        item["asscociation_includes"] = ""

                if detail["category"] == "School Information":
                    for txt in detail["text"]:
                        if "High School District: " in txt:
                            item["school_info"] = txt.replace(
                                "High School District: ", ""
                            )

                if detail["category"] == "Other Property Info":
                    for txt in detail["text"]:
                        if "Source Listing Status: " in txt:
                            item["source_listing_status"] = txt.replace(
                                "Source Listing Status: ", ""
                            )
                        if "County: " in txt:
                            item["county"] = txt.replace("County: ", "")
                        if "Cross Street: " in txt:
                            item["cross_street"] = txt.replace("Cross Street: ", "")
                        if "Source Property Type: " in txt:
                            item["source_property_type"] = txt.replace(
                                "Source Property Type: ", ""
                            )
                        if "Property Subtype: " in txt:
                            item["property_subtype"] = txt.replace(
                                "Property Subtype: ", ""
                            )
                        if "Parcel Number: " in txt:
                            item["parcel_number"] = txt.replace("Parcel Number: ", "")

                if detail["category"] == "Building and Construction":
                    for txt in detail["text"]:
                        if "Total Square Feet Living: " in txt:
                            item["total_sqft_living"] = txt.replace(
                                "Total Square Feet Living: ", ""
                            )
                        if "Construction Materials: " in txt:
                            item["construction_material"] = txt.replace(
                                "Construction Materials: ", ""
                            )
                        if "Foundation Details: " in txt:
                            item["foundation_details"] = txt.replace(
                                "Foundation Details: ", ""
                            )
                        if "Levels: " in txt:
                            item["levels"] = txt.replace("Levels: ", "")
                        if "Property Age: " in txt:
                            item["property_age"] = txt.replace("Property Age: ", "")
                        if "Roof: " in txt:
                            item["roof_type"] = txt.replace("Roof: ", "")
                if detail["category"] == "Utilities":
                    for txt in detail["text"]:
                        if "Sewer: " in txt:
                            item["sewer"] = txt.replace("Sewer: ", "")
                        if "Water Source: " in txt:
                            item["water_source"] = txt.replace("Water Source: ", "")
                if detail["category"] == "Bedrooms":
                    for txt in detail["text"]:
                        if "Bedrooms: " in txt:
                            item["bedrooms"] = txt.replace("Bedrooms: ", "")
                if detail["category"] == "Community Features":
                    for txt in detail["text"]:
                        item["community_features"] = txt

                if detail["category"] == "Unit Features":
                    for txt in detail["text"]:
                        item["unit_features"] = txt
        except:
            pass

        try:
            item["listing_raw_status"] = prop_detail["data"]["home"]["source"]["raw"][
                "status"
            ]
            item["homestyle"] = prop_detail["data"]["home"]["source"]["raw"]["style"]
            item["listing_date"] = parse_date(prop_detail["data"]["home"]["list_date"])

            item["flood_factor_score"] = prop_detail["data"]["home"]["local"]["flood"][
                "flood_factor_score"
            ]
            item["flood_factor_severity"] = prop_detail["data"]["home"]["local"][
                "flood"
            ]["flood_factor_severity"]
            item["lat"] = prop_detail["data"]["home"]["location"]["address"][
                "coordinate"
            ]["lat"]
            item["lon"] = prop_detail["data"]["home"]["location"]["address"][
                "coordinate"
            ]["lon"]
            item["environmental_risk"] = prop_detail["data"]["home"]["local"]["flood"][
                "environmental_risk"
            ]
            item["fema_zone"] = prop_detail["data"]["home"]["local"]["flood"][
                "fema_zone"
            ][0]
            item["noise_score"] = prop_detail["data"]["home"]["local"]["noise"]["score"]
        except:
            pass

        yield item
