import asyncio
import codecs
import re
import json
import uuid
import logging

import aiohttp
import datetime
import urllib.parse

from bs4 import BeautifulSoup, Tag
from typing import Any, Union
from datetime import datetime
from fake_useragent import UserAgent

from submodule_integrations.models.integration import Integration
from submodule_integrations.utils.errors import (
    IntegrationAuthError,
    IntegrationAPIError,
)


class AppFolioIntegration(Integration):
    def __init__(self, user_agent: str = UserAgent().random):
        super().__init__("appfolio")
        self.network_requester = None
        self.user_agent = user_agent
        self.headers = None
        self.token = None
        self.domain = None
        self.url = None
        self.cookie_string = None

    async def initialize(self, domain: str = None, network_requester=None, tokens: dict | str = None):
        self.domain = domain
        self.url = f"https://{self.domain}"
        self.network_requester = network_requester
        self.headers = {
            "Host": self.domain,
            "User-Agent": self.user_agent,
        }

        if isinstance(tokens, dict):
            cookie_str = self._cookie_dict_to_string(tokens)
            self.token = cookie_str
            self.headers["Cookie"] = cookie_str
            self.cookie_string = cookie_str

        if isinstance(tokens, str):
            cookie_str = tokens
            self.token = cookie_str
            self.headers["Cookie"] = cookie_str
            self.cookie_string = cookie_str

    @staticmethod
    def _cookie_dict_to_string(cookie_dict: dict) -> str:
        return "; ".join([f"{key}={value}" for key, value in cookie_dict.items()])

    async def _make_request(self, method: str, url: str, **kwargs) -> str:
        """
        Helper method to handle network requests using either custom requester or aiohttp.
        Prefers automatic redirects but falls back to manual handling if needed.
        """
        if self.network_requester:
            response = await self.network_requester.request(
                method, url, process_response=self._handle_response, **kwargs
            )
            return response

        max_redirects = kwargs.pop("max_redirects", 5)

        async with aiohttp.ClientSession() as session:
            # First try with automatic redirects
            try:
                async with session.request(
                        method, url, allow_redirects=True, **kwargs
                ) as response:
                    if response.status == 200:
                        return await self._handle_response(response)

                    # If we still get a redirect status, fall back to manual handling
                    if response.status in (301, 302, 303, 307, 308):
                        print("Automatic redirect failed, handling manually")
                        return await self._handle_manual_redirect(
                            session, method, url, max_redirects, **kwargs
                        )

                    return await self._handle_response(response)

            except aiohttp.ClientError as e:
                print(
                    f"Automatic redirect failed with error: {e}, attempting manual redirect"
                )
                return await self._handle_manual_redirect(
                    session, method, url, max_redirects, **kwargs
                )

    async def _handle_manual_redirect(
            self, session, method: str, url: str, max_redirects: int, **kwargs
    ) -> str:
        """Handle redirects manually when automatic redirects fail"""
        redirect_count = 0
        current_url = url
        current_method = method

        while redirect_count < max_redirects:
            async with session.request(
                    current_method, current_url, allow_redirects=False, **kwargs
            ) as response:
                if response.status in (301, 302, 303, 307, 308):
                    redirect_count += 1
                    next_url = response.headers.get("Location")

                    if not next_url:
                        raise IntegrationAPIError(
                            self.integration_name,
                            f"Received redirect status {response.status} but no Location header",
                        )

                    # Handle relative URLs
                    if next_url.startswith("/"):
                        parsed_url = urllib.parse.urlparse(current_url)
                        next_url = (
                            f"{parsed_url.scheme}://{parsed_url.netloc}{next_url}"
                        )

                    print(
                        f"Following manual redirect {redirect_count}/{max_redirects}: {next_url}"
                    )
                    current_url = next_url

                    # For 303, always use GET for the redirect
                    if response.status == 303:
                        current_method = "GET"

                    continue

                return await self._handle_response(response)

        raise IntegrationAPIError(
            self.integration_name,
            f"Too many redirects (max: {max_redirects})",
        )

    async def _handle_response(
            self, response: aiohttp.ClientResponse
    ) -> Union[str, Any]:
        if response.status == 200 or response.ok:
            return await response.text()

        status_code = response.status
        # do things with fail status codes
        if 400 <= status_code < 500:
            if status_code == 403 and self.network_requester:
                await self.network_requester.proxy_handler.set_new_proxy()
                return IntegrationAPIError(
                    integration_name=self.integration_name,
                    status_code=403,
                    message="Retry request",
                    error_code="retry_request",
                )

            if self.token is None:
                raise IntegrationAuthError(
                    message="No access token. [Credentials might not exist/be valid]",
                    status_code=400,
                )
            # potential auth caused
            reason = response.reason
            raise IntegrationAuthError(
                message=f"AppFolio: {status_code} - {reason}",
                status_code=status_code,
            )
        else:
            raise IntegrationAPIError(
                self.integration_name,
                f"AppFolio: {status_code} - {response.headers}",
                status_code,
            )

    @staticmethod
    def _get_state_code(status: str):
        codes = {
            "Open": "Open",
            "New": "0",
            "New by Appfolio": "10",
            "Assigned": "9",
            "Assigned by Appfolio": "11",
            "Scheduled": "3",
            "Waiting": "6",
            "Estimate Requested": "1",
            "Estimated": "2",
            "Work Done": "8",
            "Ready to Bill": "12",
            "Completed": "4",
            "Completed No Need To Bill": "7",
            "Canceled": "5",
        }
        return codes.get(status)

    @staticmethod
    def _format_date(date_str: str) -> str:
        """
        Convert a date string from YYYY-MM-DD format to RFC 2822 format with GMT timezone

        Args:
            date_str: Date string in YYYY-MM-DD format (e.g. '2025-01-01')

        Returns:
            Date string in format 'Wed, 01 Jan 2025 00:00:00 GMT'
        """
        try:
            # Parse the input date
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            # Format it according to RFC 2822 with GMT timezone
            return date_obj.strftime("%a, %d %b %Y %H:%M:%S GMT")
        except ValueError as e:
            raise ValueError(
                f"Invalid date format. Please use YYYY-MM-DD format. Error: {e}"
            )

    @staticmethod
    def denormalize_response(response_json: dict) -> list:
        """
        Takes a JSON response with 'data' and 'included' sections and merges the included data
        into the main data objects based on their relationships.

        Args:
            response_json: Dictionary containing 'data' and 'included' keys

        Returns:
            List of denormalized data objects with included data merged in
        """
        # Create a lookup dictionary for included items
        included_lookup = {}
        for item in response_json.get("included", []):
            key = (item["type"], item["id"])
            included_lookup[key] = item

        def resolve_relationship(relationship):
            """Helper function to resolve a single relationship"""
            if not relationship or "data" not in relationship:
                return None

            rel_data = relationship["data"]
            if not rel_data:  # Handle null relationships
                return None

            # Handle both single items and arrays
            if isinstance(rel_data, list):
                return [
                    included_lookup.get((item["type"], item["id"])) for item in rel_data
                ]
            else:
                key = (rel_data["type"], rel_data["id"])
                return included_lookup.get(key)

        def process_data_item(item):
            """Process a single data item and its relationships"""
            result = {
                "id": item["id"],
                "type": item["type"],
                # 'link': item.get('links', {}).get('page'),
                **item.get("attributes", {}),
                **item.get("links", {}),
            }

            # Process each relationship
            relationships = item.get("relationships", {})
            for rel_name, rel_data in relationships.items():
                resolved = resolve_relationship(rel_data)
                if resolved:
                    # If it's a list of relationships, get their attributes
                    if isinstance(resolved, list):
                        result[rel_name] = [
                            {
                                **r.get("attributes", {}),
                                "id": r["id"],
                                "type": r["type"],
                            }
                            for r in resolved
                            if r
                        ]
                    else:
                        # For single relationships, merge attributes directly
                        result[rel_name] = {
                            **resolved.get("attributes", {}),
                            "id": resolved["id"],
                            "type": resolved["type"],
                        }

                        # Special handling for nested relationships (like property.address)
                        nested_relationships = resolved.get("relationships", {})
                        for nested_name, nested_rel in nested_relationships.items():
                            nested_resolved = resolve_relationship(nested_rel)
                            if nested_resolved:
                                result[f"{rel_name}_{nested_name}"] = {
                                    **nested_resolved.get("attributes", {}),
                                    "id": nested_resolved["id"],
                                    "type": nested_resolved["type"],
                                }

            return result

        # Process all data items
        return [process_data_item(item) for item in response_json["data"]]

    async def fetch_emails(self, occupancy_id: str, tenant_id: str):
        url = f"{self.url}/occupancies/{occupancy_id}/selected_tenant/{tenant_id}"
        headers = self.headers.copy()
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.5",
            "priority": "u=1, i",
            "cookie": self.cookie_string,
        }
        html_content = await self._make_request(
            "GET",
            url,
            headers=headers,
            max_line_size=8190 * 15,
            max_field_size=8190 * 15,
        )

        if "Occupancy not found." in html_content:
            raise IntegrationAPIError(
                self.integration_name,
                f"Occupancy not found. [Occupancy ID: {occupancy_id}, Tenant ID: {tenant_id}]",
            )
        # Robust email regex pattern (case-insensitive).
        email_regex = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")

        soup = BeautifulSoup(html_content, "html.parser")

        # Search for email addresses in elements with class "js-email-mail-to".
        email_element = soup.find("a", class_="js-email-mail-to")
        if email_element and email_element.has_attr("href"):
            href = email_element["href"]
            if href.lower().startswith("mailto:"):
                candidate = href[7:]  # Remove "mailto:" prefix.
                if email_regex.fullmatch(candidate):
                    return {"email": candidate}

        # Fallback: search the text content for an email address.

        return {"email": ""}

    async def fetch_all_tenants(self, page: int = 1):
        url = f"{self.url}/occupancies"
        params = {"page": page, "sort[by]": "name", "sort[order]": "asc"}

        headers = {
            "x-requested-with": "XMLHttpRequest",
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.5",
            "priority": "u=1, i",
            "cookie": self.cookie_string,
        }

        response = await self._make_request("GET", url, params=params, headers=headers)
        data = json.loads(response)
        # Parse header row from the HTML
        thead_html = data.get("thead_row", "")
        soup = BeautifulSoup(thead_html, "html.parser")
        headers = [th.get_text(strip=True) for th in soup.find_all("th")]

        table = []
        for row in data.get("body_row_data", []):
            # Get each cell's HTML value, extract text, and pad if missing
            row_cells = row.get("data", [])
            row_values = [
                BeautifulSoup(cell.get("value", ""), "html.parser").get_text(strip=True)
                for cell in row_cells
            ]
            if len(row_values) < len(headers):
                row_values.extend([""] * (len(headers) - len(row_values)))
            row_dict = dict(zip(headers, row_values))

            # Rename "Unit" key to "Unit Name", if present
            if "Unit" in row_dict:
                row_dict["Unit Name"] = row_dict.pop("Unit")

            # Extract occupancy and selected tenant IDs from the "Name" cell (first column)
            name_cell_html = row_cells[0].get("value", "") if row_cells else ""
            name_soup = BeautifulSoup(name_cell_html, "html.parser")
            a_tag = name_soup.find("a")
            occupancy_id, selected_tenant_id = None, None
            if a_tag and a_tag.has_attr("href"):
                href = a_tag["href"]
                parts = href.strip("/").split("/")
                # Expected URL pattern: /occupancies/<occupancy_id>/selected_tenant/<tenant_id>
                if (
                        len(parts) >= 4
                        and parts[0] == "occupancies"
                        and parts[2] == "selected_tenant"
                ):
                    occupancy_id = parts[1]
                    selected_tenant_id = parts[3]
            row_dict["Occupancy ID"] = occupancy_id
            row_dict["Selected Tenant ID"] = selected_tenant_id

            table.append(row_dict)
        # Return the resulting table
        return table

    @staticmethod
    def _parse_move_ins(data):
        """
        Parse the HTML table data from the JSON string.

        Args:
            data (dict): JSON string containing HTML table data

        Returns:
            list: List of dictionaries with parsed tenant information
        """
        body_rows = data.get("body_row_data", [])
        tenants = []

        id_pattern = re.compile(r"web_flow_id=(\d+)")

        for row in body_rows:
            tenant_info = {}
            row_data = row.get("data", [])

            # Extract tenant name
            if len(row_data) > 0 and row_data[0].get("value"):
                name_html = row_data[0].get("value", "")
                href_match = re.search(r'href="([^"]+)"', name_html)
                if href_match:
                    tenant_info["tenant_url"] = href_match.group(1)
                    id_match = id_pattern.search(tenant_info["tenant_url"])
                    if id_match:
                        tenant_info["tenant_id"] = id_match.group(1)

                tenant_info["tenant_name"] = re.sub(r"<[^>]+>", "", name_html)

            # Extract property-unit
            if len(row_data) > 1 and row_data[1].get("value"):
                property_html = row_data[1].get("value", "")
                href_match = re.search(r'href="([^"]+)"', property_html)
                if href_match:
                    tenant_info["property_url"] = href_match.group(1)

                property_text = re.sub(r"<[^>]+>", "", property_html)
                tenant_info["property_unit"] = property_text

                # Extract property name and unit separately if possible
                if " - " in property_text:
                    parts = property_text.split(" - ", 1)
                    tenant_info["property_name"] = parts[0].strip()
                    tenant_info["unit"] = parts[1].strip()
                else:
                    tenant_info["property_name"] = property_text
                    tenant_info["unit"] = ""
            else:
                tenant_info["property_unit"] = ""
                tenant_info["property_name"] = ""
                tenant_info["unit"] = ""

            # Extract move-in date
            if len(row_data) > 2 and row_data[2].get("value"):
                date_html = row_data[2].get("value", "")
                tenant_info["move_in_date"] = re.sub(r"<[^>]+>", "", date_html)

            tenants.append(tenant_info)

        return tenants

    async def _get_move_out_data(self):
        url = f"{self.url}/dashboard/move_outs_data"
        params = {
            "sort[by]": "move_out_flow_date",
            "sort[order]": "asc",
        }
        headers = self.headers.copy()
        headers["x-requested-with"] = "XMLHttpRequest"
        headers["accept"] = "application/json, text/javascript, */*; q=0.01"

        page = 1
        tenants = []
        most_recent = []
        while True:
            params["page"] = page
            response = await self._make_request(
                "GET", url, headers=headers, params=params
            )
            data = json.loads(response)
            outs = self._parse_move_outs(data)
            if len(outs) == 0 or outs == most_recent:
                break

            tenants.extend(outs)
            most_recent = outs
            page += 1

        return tenants

    @staticmethod
    def _merge_moves_data(movein_tenants, moveout_tenants):
        """
        Merge parsed move-in and move-out tenant data into a comprehensive list.

        Args:
            movein_tenants (list): List of dictionaries with parsed move-in tenant data
            moveout_tenants (list): List of dictionaries with parsed move-out tenant data

        Returns:
            list: List of dictionaries with merged tenant information
        """
        # Create a merged list starting with all move-in tenants
        merged_tenants = []

        # Dictionary to store moveout tenants by name for easier lookup
        moveout_by_name = {
            tenant["tenant_name"].lower(): tenant for tenant in moveout_tenants
        }

        # First, process all move-in tenants
        for movein in movein_tenants:
            # Extract property name and unit separately if not already done
            property_unit = movein.get("property_unit", "")
            property_name = movein.get("property_name", "")
            unit = movein.get("unit", "")

            if not property_name and " - " in property_unit:
                parts = property_unit.split(" - ", 1)
                property_name = parts[0].strip()
                unit = parts[1].strip()

            tenant_data = {
                "tenant_id": movein.get("tenant_id", ""),
                "tenant_name": movein.get("tenant_name", ""),
                "property_unit": property_unit,
                "property_name": property_name,
                "unit": unit,
                "move_in_date": movein.get("move_in_date", ""),
                "move_out_date": "",
                "moveout_type": "",
                "moveout_id": "",
                "is_overdue": False,
            }

            # Check if this tenant has a matching move-out record by name
            moveout = moveout_by_name.get(movein["tenant_name"].lower())
            if moveout:
                tenant_data["move_out_date"] = moveout.get("move_out_date", "")
                tenant_data["moveout_type"] = moveout.get("moveout_type", "")
                tenant_data["moveout_id"] = moveout.get("moveout_id", "")
                tenant_data["is_overdue"] = moveout.get("is_overdue", False)

            merged_tenants.append(tenant_data)

        # Now add any moveout tenants that don't have a matching move-in record
        processed_names = {tenant["tenant_name"].lower() for tenant in merged_tenants}

        for moveout in moveout_tenants:
            if moveout["tenant_name"].lower() not in processed_names:
                # Extract property name and unit separately if not already done
                property_unit = moveout.get("property_unit", "")
                property_name = moveout.get("property_name", "")
                unit = moveout.get("unit", "")

                if not property_name and " - " in property_unit:
                    parts = property_unit.split(" - ", 1)
                    property_name = parts[0].strip()
                    unit = parts[1].strip()

                tenant_data = {
                    "tenant_id": "",  # No matching move-in ID
                    "tenant_name": moveout.get("tenant_name", ""),
                    "property_unit": property_unit,
                    "property_name": property_name,
                    "unit": unit,
                    "move_in_date": "",  # No move-in date
                    "move_out_date": moveout.get("move_out_date", ""),
                    "moveout_type": moveout.get("moveout_type", ""),
                    "moveout_id": moveout.get("moveout_id", ""),
                    "is_overdue": moveout.get("is_overdue", False),
                }
                merged_tenants.append(tenant_data)

        return merged_tenants

    async def fetch_tenancy_move_data(self):
        move_ins = await self._get_move_in_data()
        move_outs = await self._get_move_out_data()

        result = self._merge_moves_data(
            movein_tenants=move_ins, moveout_tenants=move_outs
        )
        return result

    @staticmethod
    def _parse_move_outs(data):
        """
        Parse the HTML move-out table data from the JSON string.

        Args:
            data (dict): JSON string containing HTML table data

        Returns:
            list: List of dictionaries with parsed tenant move-out information
        """
        body_rows = data.get("body_row_data", [])
        moveouts = []
        id_pattern = re.compile(r"/move_outs/(\d+)")

        for row in body_rows:
            moveout_info = {}
            row_data = row.get("data", [])

            # Extract tenant name
            if len(row_data) > 0 and row_data[0].get("value"):
                name_html = row_data[0].get("value", "")
                href_match = re.search(r'href="([^"]+)"', name_html)
                if href_match:
                    moveout_info["tenant_url"] = href_match.group(1)
                    id_match = id_pattern.search(moveout_info["tenant_url"])
                    if id_match:
                        moveout_info["moveout_id"] = id_match.group(1)

                moveout_info["tenant_name"] = re.sub(r"<[^>]+>", "", name_html)

            # Extract move out type
            if len(row_data) > 1 and row_data[1].get("value"):
                type_html = row_data[1].get("value", "")
                moveout_info["moveout_type"] = re.sub(r"<[^>]+>", "", type_html)

            # Extract property-unit
            if len(row_data) > 2 and row_data[2].get("value"):
                property_html = row_data[2].get("value", "")
                href_match = re.search(r'href="([^"]+)"', property_html)
                if href_match:
                    moveout_info["property_url"] = href_match.group(1)

                property_text = re.sub(r"<[^>]+>", "", property_html)
                moveout_info["property_unit"] = property_text

                # Extract property name and unit separately if possible
                if " - " in property_text:
                    parts = property_text.split(" - ", 1)
                    moveout_info["property_name"] = parts[0].strip()
                    moveout_info["unit"] = parts[1].strip()
                else:
                    moveout_info["property_name"] = property_text
                    moveout_info["unit"] = ""

            # Extract move-out date
            if len(row_data) > 3 and row_data[3].get("value"):
                date_html = row_data[3].get("value", "")
                moveout_info["is_overdue"] = "text-danger" in date_html
                moveout_info["move_out_date"] = re.sub(r"<[^>]+>", "", date_html)

            moveouts.append(moveout_info)

        return moveouts

    async def fetch_units(self, property_url: str):
        params = {"items_per_page": 50}
        url = f"{property_url}/units"

        headers = {
            "x-requested-with": "XMLHttpRequest",
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.5",
            "priority": "u=1, i",
            "cookie": self.cookie_string,
        }
        response = await self._make_request("GET", url, headers=headers, params=params)
        # TODO: on the UI, "Property not found." shows up
        if "Property not found." in response:
            raise IntegrationAuthError(
                f"Property not found: {property_url}",
                platform="appfolio",
                session_id=self.session_id,
            )
        # Parse the JSON response
        data = json.loads(response)
        # Extract headers from the header HTML
        thead_html = data["thead_row"]

        soup = BeautifulSoup(thead_html, "html.parser")
        headers = [th.get_text(strip=True) for th in soup.find_all("th")]

        # Parse each row into a dictionary
        rows = []
        for row in data["body_row_data"]:
            # Clean each cell's HTML and get text values
            row_values = [
                BeautifulSoup(cell["value"], "html.parser").get_text(strip=True)
                for cell in row["data"]
            ]
            row_dict = dict(zip(headers, row_values))

            # Rename the "Unit" key to "Unit Name"
            if "Unit" in row_dict:
                row_dict["Unit Name"] = row_dict.pop("Unit")

            # Extract the unit URL from row_data_attributes if available
            unit_url = None
            for attr in row.get("row_data_attributes", []):
                if attr.get("key") == "href":
                    unit_url = attr.get("value")
                    break
            row_dict["Unit URL"] = unit_url

            # Split the unit URL to extract the unit_id (and potentially other parts)
            if unit_url:
                # Remove leading/trailing slashes and split the URL
                parts = unit_url.strip("/").split("/")
                # Expecting the format: ['properties', '1584', 'units', '5027']
                if len(parts) >= 4:
                    row_dict["Unit ID"] = parts[3]  # '5027'

            # Extract the display name from the first cell (unit) of the row
            unit_cell_html = row["data"][0]["value"]
            unit_soup = BeautifulSoup(unit_cell_html, "html.parser")
            # If needed, you can extract the display text separately,
            # but the value is already assigned to "Unit Name"

            # Extract the occupant (tenant) ID from the Tenant cell, if available
            tenant_cell_html = row["data"][2]["value"]
            tenant_soup = BeautifulSoup(tenant_cell_html, "html.parser")
            a_tag = tenant_soup.find("a")
            occupant_id = None
            if a_tag and a_tag.has_attr("href"):
                href = a_tag["href"]
                if "/occupancies/" in href:
                    occupant_id = href.split("/occupancies/")[-1]
            row_dict["Occupant ID"] = occupant_id

            # Split the Lease Start/End value into separate fields
            lease_value = row_dict.get("Lease Start/End", "").strip()
            if lease_value and lease_value != "N/A" and " - " in lease_value:
                lease_start, lease_end = [
                    x.strip() for x in lease_value.split(" - ", 1)
                ]
            else:
                lease_start, lease_end = None, None
            row_dict["Lease Start"] = lease_start
            row_dict["Lease End"] = lease_end
            # Remove the original combined lease key
            row_dict.pop("Lease Start/End", None)

            rows.append(row_dict)

        # Return the final list of dictionaries
        return rows

    async def fetch_work_orders(self, status: str, start_date: str, page_number: int | None = None):
        print(f"fetching work orders for status: {status}, start_date: {start_date}" + (f", page: {page_number}" if page_number is not None else ", all pages"))
        params = {
            "page[size]": "10",
            # "page[number]": "1",
            "filter[created_at__gteq]": self._format_date(start_date),
            "sort": "-created_at",
            # Fields parameters
            "fields[work_orders]": "id,created_at,scheduled_start,scheduled_end,display_number,instructions,remarks,status,updated_at",
            "fields[occupancies]": "id,name",
            "fields[units]": "property_and_unit_name,name",
            "fields[properties]": "display_name,property_type,name_and_address",
            "fields[addresses]": "address1,address2,city,postal_code,state",
            "fields[users]": "name",
            "fields[vendors]": "name",
            "fields[work_order_categories]": "name",
            "fields[work_order_assigned_users]": "accepted",
            "fields[companies]": "name",
            "fields[service_requests]": "id,request_type",
            "fields[work_order_activities]": "comments,details,occurred_at",
            # Stats parameter
            "stats[work_orders]": "send_surveys_automatically",
            # Include parameter
            "include": "occupancy,unit,work_order_assigned_users.user,work_order_category,vendor,vendor_company,service_request,property,property.address",
        }
        filter_status_code = self._get_state_code(status)
        if filter_status_code:
            params.update({"filter[status_code]": filter_status_code})

        headers = self.headers.copy()
        headers["X-Api-Client"] = "/maintenance/service_requests/work_orders"
        headers["Accept-Version"] = "v2"
        headers["Accept"] = "application/vnd.api+json"

        url = f"{self.url}/api/work_orders"
        raw_wo_list = []

        if page_number is not None:
            # Fetch a specific page
            print(f"Requesting page {page_number} for work orders...")
            params.update({"page[number]": str(page_number)})
            response_text = await self._make_request(
                "GET", url, headers=headers, params=params
            )
            try:
                response_data = json.loads(response_text)
            except json.decoder.JSONDecodeError:
                print(f"Failed to parse JSON response for page {page_number}: {response_text[:200]}...")
                response_data = {"data": [], "included": []}

            denorm_response = self.denormalize_response(response_data)
            if len(denorm_response) > 0:
                raw_wo_list.extend(denorm_response)
        else:
            # Fetch all pages
            current_page_index = 1
            while True:
                print(f"Requesting page {current_page_index} for work orders (fetching all)...")
                params.update({"page[number]": str(current_page_index)})
                response_text = await self._make_request(
                    "GET", url, headers=headers, params=params
                )
                try:
                    response_data = json.loads(response_text)
                except json.decoder.JSONDecodeError:
                    print(f"Failed to parse JSON response for page {current_page_index}: {response_text[:200]}...")
                    break

                denorm_response = self.denormalize_response(response_data)
                if len(denorm_response) > 0:
                    raw_wo_list.extend(denorm_response)
                else:
                    break

                current_page_index += 1
        
        print(f"Fetched {len(raw_wo_list)} raw work order entries" + (f" from page {page_number}" if page_number is not None else " from all pages"))
        work_orders = []
        for i, order in enumerate(raw_wo_list):
            parsed_order = await self._parse_work_order_page(url=order.get("page"))
            print(f"parsed work order {order.get('page')}")
            if order.get("vendor_company"):
                order.pop("vendor_company")
            if order.get("remarks"):
                order.pop("remarks")
            if order.get("work_order_assigned_users"):
                order.pop("work_order_assigned_users")

            order.update(parsed_order)
            work_orders.append(order)
        print(f"fetched {len(work_orders)} work orders for {status} and {start_date}")
        return work_orders

    async def _parse_work_order_page(self, url: str):
        headers = self.headers.copy()
        headers["Accept"] = (
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8"
        )

        # include extra parameters to support massive header sizes
        response = await self._make_request(
            "GET",
            url,
            headers=headers,
            max_line_size=8190 * 15,
            max_field_size=8190 * 15,
        )
        soup = self._create_soup(response)

        work_order_data = {}
        service_id = self._extract_service_request_id(url)

        # get created by
        created_by_element = soup.select_one("span.js-service-request-header-created-by")
        if created_by_element:
            work_order_data["created_by"] = created_by_element.text.strip()

        # get job description
        description_element = soup.select_one("div.js-work-order-description")
        if description_element:
            description = description_element.text.strip()
            work_order_data["description"] = description

        # get property, owner and resident info
        property_card_element = soup.select_one("div.js-property-contact-card")
        if property_card_element:
            property_card_data = property_card_element.select_one(
                "div.js-contact-card-details"
            )
            card_data_spans = property_card_data.select("span")
            property_data = "\n".join(span.text.strip() for span in card_data_spans)
            work_order_data["property"] = property_data.strip().replace("-5\n", "")

        owner_card_element = soup.select_one("div.js-owner-contact-card")
        if owner_card_element:
            # Initialize with all expected keys
            owner_data = {"name": None, "phones": [], "email": None, "notes": None}
            
            owner_name_span = owner_card_element.select_one("span.contact-card__name")
            if owner_name_span:
                owner_data["name"] = owner_name_span.text.strip()

            owner_contact_element = owner_card_element.select_one(
                "div.js-contact-card-details"
            )
            
            # Ensure owner_contact_element exists before trying to extract text or parse it
            if owner_contact_element: 
                contact_text = self._extract_text_from_div(owner_contact_element)
                
                # Attempt to extract email from mailto links first
                email_link = owner_contact_element.select_one('a[href^="mailto:"]')
                if email_link:
                    owner_data["email"] = email_link.text.strip()
                elif contact_text: # Fallback to regex if not in mailto link and contact_text exists
                    email_regex = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
                    email_match = email_regex.search(contact_text)
                    if email_match:
                        owner_data["email"] = email_match.group(0)

                # Attempt to extract phone numbers from tel links first
                tel_links = owner_contact_element.select('a[href^="tel:"]')
                for link in tel_links:
                    phone = link.text.strip()
                    if phone and phone not in owner_data["phones"]: # Check if phone already added
                        owner_data["phones"].append(phone)
                
                # Fallback for phones from plain text if contact_text exists
                if contact_text:
                    phone_prefixes = ["phone:", "mobile:", "work phone:", "home phone:", "tel:"]
                    contact_lines = contact_text.split('\\n')
                    temp_phones_from_text = []
                    idx = 0
                    while idx < len(contact_lines):
                        current_line_stripped = contact_lines[idx].strip()
                        for prefix in phone_prefixes:
                            if current_line_stripped.lower().startswith(prefix.lower()):
                                num_part = current_line_stripped[len(prefix):].strip()
                                if num_part:
                                    temp_phones_from_text.append(num_part)
                                elif (idx + 1 < len(contact_lines)):
                                    next_line_stripped = contact_lines[idx+1].strip()
                                    all_known_labels = phone_prefixes + ["email:", "e-mail:", "owner notes:"]
                                    is_next_line_a_label = any(next_line_stripped.lower().startswith(lbl.lower()) for lbl in all_known_labels)
                                    if next_line_stripped and not is_next_line_a_label:
                                        temp_phones_from_text.append(next_line_stripped)
                                        idx += 1 # Consume next line
                                break # Found a phone prefix
                        idx += 1
                    
                    for phone_str in temp_phones_from_text:
                        # Basic validation: check for minimum digits and reject if email/notes marker present
                        # Keep only digits and '+' for count check
                        digits_only = ''.join(filter(lambda char: char.isdigit() or char == '+', phone_str))
                        if len(digits_only) >= 7 and '@' not in phone_str and 'notes:' not in phone_str.lower() and phone_str not in owner_data["phones"]:
                            # Further cleanup: remove potential labels mixed in the string if needed, though regex might be better
                            # For now, just check length and common invalid patterns
                            owner_data["phones"].append(phone_str.strip())

                    # Extract Owner Notes specifically
                    notes_title_div = owner_contact_element.find("div", class_="service-request-card-extra-title", string="Owner Notes:")
                    if notes_title_div:
                        notes_container_div = notes_title_div.find_next_sibling("div", class_="js-maintenance-notes")
                        if notes_container_div:
                            note_p = notes_container_div.find('p')
                            note_text = (note_p.text.strip() if note_p else notes_container_div.text.strip())
                            if note_text and note_text.lower() != 'none':
                                owner_data["notes"] = note_text
            # else: owner_contact_element is None, owner_data will retain initial None/empty list values

            work_order_data["owner"] = owner_data

        resident_card_element = soup.select_one("div.js-tenant-contact-card")
        if resident_card_element:
            # Initialize resident_data with expected keys
            resident_data = {"name": None, "email": None, "phones": [], "notes": None, "data": None} 
            resident_name_span = resident_card_element.select_one(
                "span.contact-card__name"
            )
            if resident_name_span:
                resident_data["name"] = resident_name_span.text.strip()

            extra_div_element = resident_card_element.select_one(
                "div.js-contact-card-details"
            )
            
            if extra_div_element:
                extra_text = self._extract_text_from_div(extra_div_element)
                # Store the raw data block which might contain other info like pets etc.
                resident_data["data"] = extra_text 

                # Attempt to extract email from mailto links first
                email_link = extra_div_element.select_one('a[href^="mailto:"]')
                if email_link:
                    resident_data["email"] = email_link.text.strip()
                elif extra_text: # Fallback to regex if not in mailto link and extra_text exists
                    email_regex = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
                    email_match = email_regex.search(extra_text)
                    if email_match:
                        resident_data["email"] = email_match.group(0)
                
                # Attempt to extract phone numbers from tel links
                tel_links = extra_div_element.select('a[href^="tel:"]')
                for link in tel_links:
                    phone = link.text.strip()
                    if phone and phone not in resident_data["phones"]:
                         resident_data["phones"].append(phone)
                
                # TODO: Add fallback logic for phones from extra_text if needed (similar to owner parsing)
                
                # TODO: Extract Resident Notes if a specific structure exists for them

            work_order_data["resident"] = resident_data

            # Extract Vendor Info
            vendor_card = soup.select_one(".js-vendor-contact-card")
            vendor_data = {"name": None, "phones": [], "fax": [], "email": None, "address": None}
            if vendor_card:
                name_elem = vendor_card.select_one("span.contact-card__name")
                if name_elem:
                    vendor_data["name"] = name_elem.text.strip()

                details_div = vendor_card.select_one("div.js-contact-card-details")
                if details_div:
                    # Extract Phones and Fax using links and context
                    contact_info_spans = details_div.select("span.js-contact-card-contact-info")
                    for span in contact_info_spans:
                        tel_link = span.select_one('a[href^="tel:"]')
                        if tel_link:
                            number = tel_link.text.strip()
                            # Check text content *before* the link within the span for context
                            span_text_content = span.decode_contents() # Get HTML content to check text before link
                            link_html = str(tel_link)
                            text_before_link = span_text_content.split(link_html)[0].lower()

                            if "fax:" in text_before_link:
                                if number and number not in vendor_data["fax"]:
                                    vendor_data["fax"].append(number)
                            elif "phone:" in text_before_link: # Assume phone if not fax and prefix exists
                                if number and number not in vendor_data["phones"]:
                                     vendor_data["phones"].append(number)
                            else:
                                # If no clear prefix, maybe add to phone as default? Or skip?
                                # Let's be conservative and only add if prefix is clear for now.
                                logging.debug(f"Skipping number {number} in vendor card due to unclear prefix: {span.text}")


                    # Extract Email
                    email_link = details_div.select_one('a[href^="mailto:"]')
                    if email_link:
                        vendor_data["email"] = email_link.text.strip()

                    # Extract Address
                    address_elem = details_div.select_one("span.js-contact-card-address")
                    if address_elem:
                         # Use get_text with separator to handle <br> tags
                         vendor_data["address"] = address_elem.get_text(separator="\\n", strip=True)

            work_order_data["vendor"] = vendor_data

        # get priority information
        priority_element = soup.find("span", text="Priority:")
        if priority_element:
            priority_element_value = soup.select_one(
                "span.js-service-request-header-priority"
            )
            if priority_element_value:
                priority = priority_element_value.text.strip()
                work_order_data["priority"] = priority

        # get actions information
        actions_element = soup.select_one("div.js-activity-log")
        if actions_element:
            actions = []
            activity_rows = actions_element.select("div.js-activity-log-row")
            for row in activity_rows:
                activity_text = self._extract_text_from_div(row)
                actions.append(activity_text)

            work_order_data["actions"] = actions

        # get vendor instructions
        vendor_instructions_element = soup.select_one(
            "div.js-work-order-vendor-instructions"
        )
        if vendor_instructions_element:
            instructions = self._extract_text_from_div(vendor_instructions_element)
            work_order_data["vendor_instructions"] = instructions

        # get work order notes
        notes_element = soup.select_one("div#notes")
        if notes_element:
            notes_card_element = notes_element.select_one("div.card-body")
            if notes_card_element:
                notes = await self._fetch_notes(service_id=service_id)
                work_order_data["notes"] = notes

        # get attachments
        attachments_element = soup.select_one("div.js-work-order-body__attachments")
        if attachments_element:
            attachments = await self._fetch_attachments(service_id=service_id)
            work_order_data["attachments"] = attachments

        # get task assignee
        assignee_element = soup.select_one("div.js-assigned-to")
        if assignee_element:
            assignees = []
            assigned_to_elements = assignee_element.select("span.js-assignee-name")
            for each in assigned_to_elements:
                assigned_to_name = each.text.strip()
                assignees.append(assigned_to_name)
            work_order_data["assigned_to"] = assignees

        return work_order_data

    async def _fetch_notes(self, service_id: str):
        params = {
            "add_notes_for_id": f"{service_id}",
            "add_notes_for_type": "Maintenance::ServiceRequestDecorator",
            "show_all": "true",
            "show_notes_for_id": f"{service_id}",
            "show_notes_for_type": "Maintenance::ServiceRequestDecorator",
        }
        url = f"{self.url}/notes"
        headers = self.headers.copy()
        headers["Accept"] = (
            "*/*;q=0.5, text/javascript, application/javascript, application/ecmascript, application/x-ecmascript"
        )

        response = await self._make_request("GET", url, headers=headers, params=params)
        # regex matching was not consistent
        start_idx = response.find(".html(")
        if start_idx == -1:
            return None

        # Find the first quote after .html(
        content_start = response.find('"', start_idx)
        if content_start == -1:
            return None

        # Find the last quote before the closing parenthesis
        content_end = response.rfind('"')
        if content_end == -1 or content_end <= content_start:
            return None

        # Extract the content between quotes
        content = response[content_start + 1: content_end]
        # Replace escaped characters
        content = content.replace('\\"', '"')  # Unescape quotes
        content = content.replace("\\n", "\n")  # Handle newlines
        content = content.replace("\\/", "/")  # Handle forward slashes
        content = content.strip()

        soup = self._create_soup(content)
        notes_block = soup.select_one("section.js-notes-block")
        notes_list = notes_block.select("div.js-block-show")

        notes = []
        for item in notes_list:
            note = self._extract_text_from_div(item)
            if note == "":
                continue

            note = note.replace("\nEdit\nDelete", "")
            note = note.replace("\nshow full note\ncollapse note", "")
            notes.append(note)

        return notes

    async def _fetch_attachments(self, service_id: str):
        url = f"{self.url}/api/work_orders?filter[service_request][id]={service_id}&fields[service_requests]=id&fields[work_orders]=remarks,display_number&fields[attachments]=name,preview_url,created_at,size&include=visible_attachments"
        headers = self.headers.copy()
        headers["Accept"] = "application/vnd.api+json"
        headers["Accept-Version"] = "v2"

        response = await self._make_request("GET", url, headers=headers)
        try:
            response = json.loads(response)
        except json.decoder.JSONDecodeError:
            print("failed to decode response for fetching attachments")
            return None

        included: list = response["included"]
        attachments = []
        for included_item in included:
            if included_item.get("type") == "attachments":
                attached = included_item.get("attributes")
                attachments.append(attached)

        return attachments

    async def fetch_vacancies(self):
        url = f"{self.url}/vacancies"
        params = {
            "filters[properties_ids]": "",
            "filters[bedrooms]": "",
            "filters[min_rent]": "",
            "filters[max_rent]": "",
            "filters[available_from]": "",
            "filters[available_to]": "",
            "filters[cats]": "",
            "filters[dogs]": "",
            "filters[sort_by]": "websitePostingVisible",
        }
        headers = self.headers.copy()
        headers["Accept"] = "application/json; q=0.01"
        response = await self._make_request(
            "GET", url=url, headers=headers, params=params
        )
        try:
            response = json.loads(response)
            results_html = response.get("results_html")
        except json.decoder.JSONDecodeError:
            print(f"not json response: {response[:300]}")
            results_html = response

        if results_html is None:
            return None

        soup = self._create_soup(results_html)

        vacancy_cards = soup.select("div.js-listable-card")
        vacancies = []
        vacancy_tasks = []
        for vacancy_item in vacancy_cards:
            task = asyncio.create_task(self._parse_vacancy_task(vacancy_item))
            vacancy_tasks.append(task)

        for future in asyncio.as_completed(vacancy_tasks):
            vacancy = await future
            if future is not None:
                vacancies.append(vacancy)

        return vacancies

    async def _parse_vacancy_task(self, vacancy_item: Tag):
        try:
            headers = self.headers.copy()
            headers["Accept"] = "*/*"

            parsed = self._parse_vacancy_card(card=vacancy_item)
            property_url = parsed.get("link")
            if "campaigns" in property_url:
                # the response for this is js and has html elements for the modal; the actual url is in here
                campaign_resp = await self._make_request(
                    "GET",
                    url=property_url,
                    headers=headers,
                    max_line_size=8190 * 15,
                    max_field_size=8190 * 15,
                )
                d_start = campaign_resp.find("campaign_unit_type_link")
                c_data = campaign_resp[d_start: d_start + 150]
                pattern = r'href=[\'"]([^\'"]*)[\'"]'
                match = re.search(pattern, c_data)

                if match:
                    property_url = match.group(1)
                else:
                    escaped_pattern = r'href=\\[\'"]([^\\\'\\"]*)\\[\'"]'
                    match = re.search(escaped_pattern, c_data)
                    if match:
                        property_url = match.group(1)
                    else:
                        property_url = None

                if property_url:
                    property_url = self.url + property_url
                    parsed["link"] = property_url

            if property_url is None:
                return parsed

            property_page = await self._make_request(
                method="GET",
                url=property_url,
                headers=headers,
                max_line_size=8190 * 15,
                max_field_size=8190 * 15,
            )
            page_soup = self._create_soup(property_page)
            page_data = self._parse_vacancy_page(soup=page_soup)

            parsed.update(page_data)
            return parsed
        except Exception as e:
            print(f"failed to parse vacancy: {e.with_traceback(None)}")
            return None

    @staticmethod
    def _parse_vacancy_page(soup: BeautifulSoup) -> dict[str, Any]:
        data = {}
        unit_data = {}
        property_data = {}
        campaign_unit_data = {}

        unit_desc_elem = soup.select_one("div.unit-name-and-address")
        if unit_desc_elem is not None:
            unit_type_elem = unit_desc_elem.select_one(
                "div.js-unit_template_key_value_datapair"
            )
            if unit_type_elem is not None:
                unit_type = unit_type_elem.select_one("div.datapair__value")
                unit_data["type"] = unit_type.text.strip()

        property_desc_elem = soup.select_one("div.property-name-and-address")
        if property_desc_elem is not None:
            property_type_elem = property_desc_elem.select_one(
                "div#property_type_value"
            )
            if property_type_elem is not None:
                property_data["type"] = property_type_elem.text.strip()

            county_elem = property_desc_elem.select_one(
                "div.js-marketing-property-county"
            )
            if county_elem is not None:
                property_data["county"] = county_elem.text.strip()

        unit_info_elem = soup.select_one("div#unit_information_show")
        if unit_info_elem is not None:
            info_pairs = unit_info_elem.select("div.datapair")
            unit_info = AppFolioIntegration._parse_data_pairs(info_pairs)

            unit_data["general"] = unit_info

        property_info_elem = soup.select_one("div#property_information_show")
        if property_info_elem is not None:
            info_pairs = property_info_elem.select("div.datapair")
            property_info = AppFolioIntegration._parse_data_pairs(info_pairs)

            property_data["general"] = property_info

        unit_rental_info_elem = soup.select_one("div#unit_rental_information_show")
        if unit_rental_info_elem is not None:
            info_pairs = unit_rental_info_elem.select("div.datapair")
            unit_rental_info = AppFolioIntegration._parse_data_pairs(info_pairs)
            unit_data["rental_info"] = unit_rental_info

        property_rental_info_elem = soup.select_one(
            "div#property_rental_information_show"
        )
        if property_rental_info_elem is not None:
            info_pairs = property_rental_info_elem.select("div.datapair")
            property_rental_info = AppFolioIntegration._parse_data_pairs(info_pairs)
            property_data["rental_info"] = property_rental_info

        amenities_elem = soup.select_one("div#amenities_information_show")
        if amenities_elem is not None:
            info_pairs = amenities_elem.select("div.datapair")
            amenities = AppFolioIntegration._parse_data_pairs(info_pairs)
            data["amenities"] = amenities

        unit_marketing_elem = soup.select_one("div#unit_marketing_information_show")
        if unit_marketing_elem is not None:
            info_pairs = unit_marketing_elem.select("div.datapair")
            unit_info = AppFolioIntegration._parse_data_pairs(info_pairs)
            unit_data["marketing_info"] = unit_info

        property_marketing_elem = soup.select_one(
            "div#property_marketing_information_show"
        )
        if property_marketing_elem is not None:
            info_pairs = property_marketing_elem.select("div.datapair")
            property_info = AppFolioIntegration._parse_data_pairs(info_pairs)
            property_data["marketing_info"] = property_info

        # for campaign pages
        campaign_rental_elem = soup.select_one(
            "div#unit_template_basic_information_show"
        )
        if campaign_rental_elem is not None:
            info_pairs = campaign_rental_elem.select("div.datapair")
            campaign_rental_info = AppFolioIntegration._parse_data_pairs(info_pairs)
            campaign_unit_data["rental_info"] = campaign_rental_info

        campaign_marketing_elem = soup.select_one(
            "div#unit_template_basic_information_show"
        )
        if campaign_marketing_elem is not None:
            info_pairs = campaign_marketing_elem.select("div.datapair")
            campaign_marketing_info = AppFolioIntegration._parse_data_pairs(info_pairs)
            campaign_unit_data["marketing_info"] = campaign_marketing_info

        # First find the h2 with "Amenities" text
        amenities_header = soup.find(
            "h2", text=lambda text: text and text.strip() == "Amenities"
        )

        # Navigate to the card-header div
        if amenities_header:
            if data.get("amenities") is None:
                card_header = amenities_header.find_parent("div", class_="card-header")

                # Then find the parent section element
                if card_header:
                    section_parent = card_header.find_parent("section")
                    if section_parent is not None:
                        info_pairs = section_parent.select("div.datapair")
                        amenities = AppFolioIntegration._parse_data_pairs(info_pairs)
                        data["amenities"] = amenities

        data["unit"] = unit_data
        data["property"] = property_data
        data["campaign"] = campaign_unit_data

        return data

    @staticmethod
    def _parse_data_pairs(info_pairs: list[Tag]):
        data = {}
        for info_pair in info_pairs:
            info_key = info_pair.select_one("div.datapair__key").text.strip()
            info_value_elem = info_pair.select_one("div.datapair__value")
            info_value = AppFolioIntegration._extract_text_from_div(info_value_elem)

            if "View Nearby Advertised Units" in info_value:
                info_value = info_value.replace("View Nearby Advertised Units", "")

            data[info_key] = info_value

        return data

    def _parse_vacancy_card(self, card: Tag):
        vacancy = {}
        name_elem = card.select_one("span.js-card-title")
        if name_elem is not None:
            vacancy["name"] = name_elem.text.strip()

            link_elem = name_elem.select_one("a")
            link = link_elem.get("href")
            vacancy["link"] = self.url + link

        address_elem = card.select_one("span.js-card-address")
        if address_elem is not None:
            address = address_elem.text.strip()
            address = address.split("Edit")[0]
            vacancy["address"] = address

        rent_table_elem = card.select_one("table.unit-property-card__table")
        if rent_table_elem is not None:
            rent_data = []
            table_bits = rent_table_elem.select("td")
            for item in table_bits:
                item_data = {}
                item_title_elem = item.select_one(
                    "span.unit-property-card__tiny-header"
                )
                item_title = item_title_elem.text.strip()
                item_title = codecs.decode(item_title, "unicode-escape")
                item_value_elem = item.select_one('[class^="js-card"]')
                item_value = item_value_elem.text.strip()
                item_value = codecs.decode(item_value, "unicode-escape")

                item_data[item_title] = item_value
                rent_data.append(item_data)

            vacancy["rent_data"] = rent_data

        actions_elem = card.select_one("div.action-table")
        rent_status_card = actions_elem.select_one("p.js-vacancy-type")
        if rent_status_card is not None:
            rent_status = rent_status_card.text.strip()
            vacancy["rent_status"] = rent_status

        actions_table_elem = actions_elem.select_one("table")
        if actions_table_elem is not None:
            website_status_row = actions_table_elem.select_one("tr.js-website-tasks")
            if website_status_row is not None:
                value_elem = website_status_row.select_one("td.js-task-status")
                vacancy["website_status"] = value_elem.text.strip()

            internet_status_row = actions_table_elem.select_one("tr.js-internet-tasks")
            if internet_status_row is not None:
                value_elem = internet_status_row.select_one("td.js-task-status")
                vacancy["internet_status"] = value_elem.text.strip()

            premium_status_row = actions_table_elem.select_one("tr.js-premium-tasks")
            if premium_status_row is not None:
                value_elem = premium_status_row.select_one("td.js-task-status")
                vacancy["premium_status"] = value_elem.text.strip()

            refresh_status_row = actions_table_elem.select_one(
                "td.action-table__refresh-container"
            )
            if refresh_status_row is not None:
                vacancy["last_updated"] = refresh_status_row.text.strip()

        return vacancy

    async def fetch_tenancies(self):
        url = f"{self.url}/lease_documents?filter_type="
        headers = self.headers.copy()
        headers["Accept"] = "application/json, text/javascript, */*; q=0.01"

        response = await self._make_request("GET", url=url, headers=headers)
        try:
            response = json.loads(response)
            results_html = response.get("results_html")
        except json.decoder.JSONDecodeError:
            print(f"not json response: {response[300:1000]}")
            results_html = response

        if results_html is None:
            return None

        tenancies = self._parse_lease_table(results_html)
        return tenancies

    def _parse_lease_table(self, html_content):
        """
        Parse the lease documents table HTML into a list of dictionaries.

        Args:
            html_content (str): The HTML content of the table

        Returns:
            list: A list of dictionaries, each representing a row in the table
        """
        # Parse the HTML
        soup = AppFolioIntegration._create_soup(html_content)

        # Find the table
        table = soup.find("table", id="lease_documents_list_table")

        # Get table headers
        headers = []
        for th in table.find_all("th"):
            headers.append(th.text.strip())

        # Process each row
        result = []
        for row in table.find("tbody").find_all("tr"):
            row_data = {}

            # Get the document ID from the data-href attribute
            data_href = row.get("data-href", "")
            if data_href:
                document_id = data_href.split("/")[-1]
                row_data["document_id"] = document_id

            # Process each cell in the row
            cells = row.find_all("td")

            # Get tenant names (split by <br> tags)
            tenants_cell = cells[0]
            tenants = [tenant.strip() for tenant in tenants_cell.stripped_strings]
            row_data[headers[0]] = tenants

            # Unit name
            row_data[headers[1]] = cells[1].text.strip()

            # Lease generation date
            row_data[headers[2]] = cells[2].text.strip()

            # Status
            row_data[headers[3]] = cells[3].text.strip()

            # Action info
            action_cell = cells[4]
            action_link = action_cell.find("a")
            if action_link:
                action_text = action_link.text.strip()
                action_href = self.url + action_link.get("href", "")
                row_data["action"] = {"text": action_text, "link": action_href}

            result.append(row_data)

        return result

    async def fetch_properties(self, page: int = 1):
        url = f"{self.url}/properties"
        params = {
            "hoa_index_page": "false",
            "include_hidden_properties": "true",
            "page": page,
            "sort[by]": "name",
            "sort[order]": "asc",
        }
        headers = {
            "x-requested-with": "XMLHttpRequest",
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.5",
            "priority": "u=1, i",
            "cookie": self.cookie_string,
        }
        response = await self._make_request(
            "GET", url=url, params=params, headers=headers
        )
        json_data = json.loads(response)
        properties = self._parse_properties_table(json_data)
        return properties

    def _parse_properties_table(self, json_data):
        """
        Parse the JSON data (from the HTML table) into a list of dictionaries
        with keys: name, street_address, city_state_zip, url, type, units, vacant, owner.
        """
        properties = []
        for row in json_data.get("body_row_data", []):
            cells = row.get("data", [])
            # if len(cells) < 5:
            #     continue  # skip rows that do not have all the expected columns

            # --- Column 1: Name (which includes the URL and address details) ---
            # Example HTML: <a href="...">Line1<br />Line2<br />Line3</a>
            name_cell_html = cells[0]["value"]
            soup = BeautifulSoup(name_cell_html, "html.parser")
            a_tag = soup.find("a")
            if a_tag:
                url = a_tag.get("href", "")
                # Use stripped_strings to get text lines
                lines = list(a_tag.stripped_strings)
                if len(lines) >= 3:
                    # If the first line is the same as the second, assume no property name was given.
                    if lines[0] == lines[1]:
                        name = None
                    else:
                        name = lines[0]
                    street_address = lines[1]
                    city_state_zip = lines[2]
                elif len(lines) == 2:
                    # If only two lines exist, assume the first line is a name only if it differs from the address.
                    if lines[0] == lines[1]:
                        name = None
                        street_address = lines[0]
                        city_state_zip = ""
                    else:
                        name = lines[0]
                        street_address = lines[1]
                        city_state_zip = ""
                elif lines:
                    name = None
                    street_address = lines[0]
                    city_state_zip = ""
                else:
                    name = None
                    street_address = ""
                    city_state_zip = ""
            else:
                url = ""
                name = None
                street_address = ""
                city_state_zip = ""

            # --- Column 2: Type ---
            type_val = cells[1]["value"].strip() if cells[1]["value"] else ""

            # --- Column 3: Units ---
            units_val = cells[2]["value"].strip() if cells[2]["value"] else ""

            # --- Column 4: Vacant ---
            # We convert "Yes" (case-insensitive) to True, otherwise False.
            vacant_text = cells[3]["value"].strip() if cells[3]["value"] else ""
            vacant = True if vacant_text.lower() == "yes" else False

            # --- Column 5: Owners ---
            owner = "n/a"
            if len(cells) > 4:
                owner_html = cells[4]["value"]
                owner_soup = BeautifulSoup(owner_html, "html.parser")
                owner = owner_soup.get_text(strip=True)

            # Build the property dictionary
            property_dict = {
                "name": name,
                "street_address": street_address,
                "city_state_zip": city_state_zip,
                "url": url,
                "type": type_val,
                "units": units_val,
                "vacant": vacant,
                "owner": owner,
            }
            properties.append(property_dict)
        return properties

    async def _verify_occupancy_exists(self, occupancy_id: int):
        headers = self.headers.copy()
        headers["Accept-Version"] = "v2"
        headers["Accept"] = "application/vnd.api+json"
        headers["X-Requested-With"] = "XMLHttpRequest"

        path = self.url + f"/api/occupancies/{occupancy_id}?fields[occupancies]=move_in,status,hidden"
        response = await self._make_request("GET", url=path, headers=headers)
        occupancy_data = json.loads(response)
        if "errors" in occupancy_data:
            errors = occupancy_data["errors"]
            error_msg = ", ".join(error['title'] for error in errors)

            raise IntegrationAPIError(
                status_code=404,
                message=f"[{occupancy_id}]: {error_msg}",
                integration_name=self.integration_name
            )

        return True

    async def occupancy_add_attachment(self, occupancy_id: int, file_name: str, file_content: bytes, f_type: str):
        await self._verify_occupancy_exists(occupancy_id)

        headers = self.headers.copy()
        headers["Accept-Version"] = "v2"
        headers["Accept"] = "application/vnd.api+json"
        headers["X-Requested-With"] = "XMLHttpRequest"
        headers["Content-Type"] = "application/vnd.api+json"

        url = self.url + f"/api/occupancies/{occupancy_id}/attachments"
        params = {
            "include": "user"
        }
        query = {
            "data": [
                {
                    "type": "attachments",
                    "attributes": {
                        "content_type": f_type,
                        "name": file_name,
                        "size": len(file_content)
                    },
                    "relationships": {
                        "folder": {
                            "data": {}
                        }
                    }
                }
            ]
        }
        q_data = json.dumps(query).replace(" ", "")

        create_response = await self._make_request("POST", url=url, params=params, headers=headers, data=q_data)
        create_response = json.loads(create_response)
        upload_data = create_response.get("data")

        try:
            upload_data = upload_data[0]
        except IndexError:
            raise IntegrationAPIError(
                integration_name=self.integration_name,
                message="Failed to initiate content upload"
            )

        upload_id = upload_data.get("id")
        upload_params = upload_data.get("attributes", {}).get("upload_params")

        if not upload_params:
            raise IntegrationAPIError(
                integration_name=self.integration_name,
                message="No upload parameters received from API"
            )

        # Upload the file content to S3
        uploaded = await self._upload_attachment(upload_params, file_content)

        confirm_path = self.url + f"/api/occupancies/attachments/{upload_id}/confirm"
        headers["Referer"] = self.url + f"/occupancies/{occupancy_id}"
        confirm_response = await self._make_request("PUT", url=confirm_path, headers=headers, data="{}")

        # Return the download URL and other relevant information
        return {
            "id": upload_id,
            "download_url": upload_data.get("attributes", {}).get("download_url"),
            "preview_url": upload_data.get("attributes", {}).get("preview_url"),
            "name": upload_data.get("attributes", {}).get("name")
        }

    async def _upload_attachment(self, upload_params: dict, content: bytes):
        try:
            # Extract S3 upload URL and form fields from the parameters
            url = upload_params.get("url")
            fields = upload_params.get("fields", {})

            # Create boundary for multipart form data
            boundary = f"----WebKitFormBoundary{uuid.uuid4().hex[:16]}"

            # Build the multipart form data
            form_data = []

            # Add all form fields from the upload_params
            for key, value in fields.items():
                form_data.append(f'--{boundary}\r\n')
                form_data.append(f'Content-Disposition: form-data; name="{key}"\r\n\r\n')
                form_data.append(f'{value}\r\n')

            # Add the file content
            form_data.append(f'--{boundary}\r\n')
            form_data.append(
                f'Content-Disposition: form-data; name="file"; filename="{fields.get("key").split("/")[-1]}"\r\n')
            form_data.append(f'Content-Type: {fields.get("Content-Type")}\r\n\r\n')

            # Convert form_data to bytes and combine with file content
            form_bytes = ''.join(form_data).encode('utf-8')
            final_boundary = f'\r\n--{boundary}--\r\n'.encode('utf-8')
            body = b''.join([
                form_bytes,
                content,
                final_boundary
            ])

            # Setup headers for the S3 request
            headers = {
                'Content-Type': f'multipart/form-data; boundary={boundary}',
                'User-Agent': self.user_agent,
                'Accept': '*/*',
                'Origin': self.url.split("/api")[0] if "/api" in self.url else self.url
            }

            # Make the request to S3
            # response = await self._make_request("POST", url=url, headers=headers, data=body)
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, data=body) as response:
                    if response.status >= 400:
                        error_text = await response.text()
                        raise IntegrationAPIError(
                            status_code=response.status,
                            message=f"Error uploading file to S3: {error_text}",
                            integration_name=self.integration_name
                        )

                    # S3 returns empty 204 response on success
                    return True

        except Exception as e:
            if isinstance(e, IntegrationAPIError):
                raise e

            raise IntegrationAPIError(
                status_code=500,
                message=f"Error uploading attachment: {str(e)}",
                integration_name=self.integration_name
            )

    async def occupancy_add_note(self, occupancy_id: int, note: str = ""):
        await self._verify_occupancy_exists(occupancy_id)

        tenants = await self._fetch_occupancy_tenants(occupancy_id=occupancy_id)
        if not tenants or len(tenants) == 0:
            raise IntegrationAPIError(
                integration_name=self.integration_name,
                message=f"No tenants found for occupancy [{occupancy_id}]",
                status_code=400
            )

        latest_tenant = tenants[0]
        payload = await self._fetch_new_note_params(latest_tenant.get('id'))
        payload['note[body]'] = note

        # param_strings = []
        # for key, value in payload.items():
        #     if key == 'note[body]':
        #         # URL encode the note body parameter
        #         param_strings.append(f"note%5Bbody%5D={value}")
        #     else:
        #         # Regular parameter
        #         param_strings.append(f"{key}={value}")
        #
        # request_data = '&'.join(param_strings)
        create_path = self.url + f"/notes"

        headers = self.headers.copy()
        headers["X-Requested-With"] = "XMLHttpRequest"
        headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"

        response = await self._make_request("POST", url=create_path, headers=headers, data=payload)
        created = self._verify_note_creation(response=response, note=note)
        if created:
            return {
                "success": created
            }

        raise IntegrationAPIError(
            integration_name=self.integration_name,
            message=f"Failed to create note for occupancy [{occupancy_id}]",
            status_code=400
        )

    @staticmethod
    def _verify_note_creation(response: str, note: str):
        # extremely basic but the regex is unstable
        c_note = note.replace(" ", "").lower()
        c_response = response.replace(" ", "").lower()

        if c_response.find(c_note) != -1:
            return True

        # Direct search - simplest and most reliable
        if f'<span>{note}</span>' in response:
            return True

        # Find all span contents and check each one
        spans = re.findall(r'<span>([^<]*)<\/span>', response)
        for span_text in spans:
            unescaped_text = span_text.replace('\\n', '\n').replace('\\"', '"')
            if unescaped_text == note:
                return True

        # Last resort - targeted search
        js_note_text = re.search(r'js-note-text[^>]*>.*?<span>([^<]*)<\/span>', response, re.DOTALL)
        if js_note_text and js_note_text.group(1) == note:
            return True

        return False

    async def _fetch_new_note_params(self, tenant_id: int):
        headers = self.headers.copy()
        headers["X-Requested-With"] = "XMLHttpRequest"

        path = self.url + f"/notes/new?parent_id={tenant_id}&parent_type=Tenant"
        response = await self._make_request("GET", url=path, headers=headers)

        html_pattern = r'BlockEdit\.displayEdit\(\s*"note_new",\s*"(.*?)"\s*\)'
        html_match = re.search(html_pattern, response, re.DOTALL)

        if not html_match:
            return {
                'error': 'Could not extract HTML from BlockEdit.displayEdit call'
            }

        # The HTML will have escaped quotes and newlines
        html_content = html_match.group(1)

        # Unescape the HTML content
        html_content = html_content.replace('\\n', '\n').replace('\\"', '"')

        # Extract authenticity_token
        auth_token_match = re.search(r'name="authenticity_token" value="([^"]+)"', html_content)
        authenticity_token = auth_token_match.group(1) if auth_token_match else None

        # Extract parent_id
        parent_id_match = re.search(r'name="parent_id"\s+value="([^"]+)"', html_content)
        parent_id = parent_id_match.group(1) if parent_id_match else None

        # Extract parent_type
        parent_type_match = re.search(r'name="parent_type"\s+value="([^"]+)"', html_content)
        parent_type = parent_type_match.group(1) if parent_type_match else None

        # Build the request payload dictionary
        payload = {
            'authenticity_token': authenticity_token,
            'parent_id': parent_id,
            'parent_type': parent_type,
            'note[body]': '',  # This will be filled by the user later
            'commit': 'Save'  # The value of the submit button
        }

        return payload

    async def _fetch_occupancy_tenants(self, occupancy_id: int):
        headers = self.headers.copy()
        headers["Accept-Version"] = "v2"
        headers["Accept"] = "application/vnd.api+json"
        headers["X-Requested-With"] = "XMLHttpRequest"

        path = self.url + f"/api/tenants?filter[occupancy][id]={occupancy_id}&fields[tenants]=name,hidden"
        response = await self._make_request("GET", url=path, headers=headers)
        response = json.loads(response)
        occupancy_tenants = response.get("data")
        return occupancy_tenants

    @staticmethod
    def _parse_address_parts(address_parts):
        """
        Parse address parts into property name, street address, and city/state/zip

        Args:
            address_parts (list): List of address parts extracted from HTML

        Returns:
            dict: Dictionary with property information
        """
        property_name = None
        street_address = None
        city_state_zip = None

        if len(address_parts) == 3:
            if not address_parts[0].endswith(("Avenue", "Street", "Ave", "St")):
                property_name = address_parts[0]
                street_address = address_parts[1]
            else:
                street_address = address_parts[0]
            city_state_zip = address_parts[-1]
        elif len(address_parts) == 2:
            street_address = address_parts[0]
            city_state_zip = address_parts[1]
        elif len(address_parts) == 1:
            street_address = address_parts[0]

        return {
            "name": property_name,
            "street_address": street_address,
            "city_state_zip": city_state_zip,
        }

    @staticmethod
    def _extract_service_request_id(url):
        """
        Extracts the first ID (service request ID) from an AppFolio maintenance URL.

        Args:
            url (str): The AppFolio URL containing service_requests ID

        Returns:
            str: The service request ID if found, None otherwise
        """
        # Use regex to find the service_requests ID
        match = re.search(r"/service_requests/(\d+)/", url)

        if match:
            return match.group(1)
        return None

    @staticmethod
    def _create_soup(text: str):
        return BeautifulSoup(text, "html.parser")

    @staticmethod
    def _extract_text_from_div(div_element):
        """
        Extracts all text from the given div element and its descendants,
        returning each child's text on a new line.
        Args:
          div_element: The BeautifulSoup object representing the div element.
        Returns:
          A string containing the extracted text with each child's text on a new line.
        """
        all_text = []
        for child in div_element.descendants:
            if child.name is None:  # Check if it's a NavigableString (text)
                text = child.strip()
                if text:  # Skip empty strings
                    all_text.append(text)
        return "\n".join(all_text)

    async def add_work_order_note(self, work_order_id: str, service_request_id: str, note: str = ""):
        """
        Adds a note to a work order.
        
        Args:
            work_order_id: The ID of the work order
            service_request_id: The ID of the service request
            note: The note text to add
        """
        print(f"Adding note to work order [{work_order_id}] for service request [{service_request_id}]")
        # First get the work order page to get the CSRF token
        work_order_url = f"{self.url}/maintenance/service_requests/{service_request_id}/work_orders/{work_order_id}"
        headers = self.headers.copy()
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
        
        # Add the max_line_size and max_field_size parameters to handle large headers
        response = await self._make_request(
            "GET", 
            work_order_url, 
            headers=headers,
            max_line_size=8190 * 15,
            max_field_size=8190 * 15
        )
        soup = self._create_soup(response)
        
        # Extract CSRF token
        csrf_meta_tag = soup.find('meta', {'name': 'csrf-token'})
        if not csrf_meta_tag or not csrf_meta_tag.get('content'):
            raise IntegrationAPIError(
                integration_name=self.integration_name,
                message="Could not find CSRF token on work order page",
                status_code=400
            )
        
        csrf_token = csrf_meta_tag.get('content')
        
        # Get the new note form
        new_note_url = f"{self.url}/notes/new?parent_id={work_order_id}&parent_type=Maintenance%3A%3AWorkOrderDecorator"
        headers = self.headers.copy()
        headers.update({
            "X-CSRF-Token": csrf_token,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript"
        })
        
        response = await self._make_request("GET", new_note_url, headers=headers)
        
        # Extract form token from JavaScript response
        html_pattern = r'BlockEdit\.displayEdit\(\s*".*?",\s*"(.*)"\s*\);'
        html_match = re.search(html_pattern, response, re.DOTALL)
        
        if not html_match:
            raise IntegrationAPIError(
                integration_name=self.integration_name,
                message="Could not extract HTML from BlockEdit.displayEdit call",
                status_code=400
            )
        
        # Unescape the HTML content
        html_content = html_match.group(1).replace('\\"', '"').replace('\\n', '\n')
        
        # Extract authenticity_token
        auth_token_match = re.search(r'name="authenticity_token" value="([^"]+)"', html_content)
        if not auth_token_match:
            raise IntegrationAPIError(
                integration_name=self.integration_name,
                message="Could not find authenticity token in form",
                status_code=400
            )
        
        authenticity_token = auth_token_match.group(1)
        
        # Prepare the note submission
        note_url = f"{self.url}/notes"
        headers = self.headers.copy()
        headers.update({
            "X-CSRF-Token": csrf_token,
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "*/*;q=0.5, text/javascript, application/javascript, application/ecmascript, application/x-ecmascript"
        })
        
        data = {
            'authenticity_token': authenticity_token,
            'parent_id': work_order_id,
            'parent_type': 'Maintenance::WorkOrderDecorator',
            'note[body]': note,
            'commit': 'Save'
        }
        
        response = await self._make_request("POST", note_url, headers=headers, data=data)
        
        # Verify note was created
        if self._verify_note_creation(response=response, note=note):
            return {"success": True}
        
        raise IntegrationAPIError(
            integration_name=self.integration_name,
            message=f"Failed to create note for work order [{work_order_id}]",
            status_code=400
        )
