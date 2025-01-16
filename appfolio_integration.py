import json
import datetime
import urllib.parse
from datetime import datetime
from typing import Any, Union

import aiohttp
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from helpers.tools import cookie_dict_to_string
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
        self.url = "https://ocf.appfolio.com"
        self.headers = None

    async def initialize(self, network_requester=None, tokens: dict = None):
        self.network_requester = network_requester
        self.headers = {
            'Host': 'ocf.appfolio.com',
            "User-Agent": self.user_agent,
        }

        if isinstance(tokens, dict):
            cookie_str = cookie_dict_to_string(tokens)
            self.headers["Cookie"] = cookie_str

        if isinstance(tokens, str):
            cookie_str = tokens
            self.headers["Cookie"] = cookie_str

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

        max_redirects = kwargs.pop('max_redirects', 5)

        async with aiohttp.ClientSession() as session:
            # First try with automatic redirects
            try:
                async with session.request(method, url, allow_redirects=True, **kwargs) as response:
                    if response.status == 200:
                        return await self._handle_response(response)

                    # If we still get a redirect status, fall back to manual handling
                    if response.status in (301, 302, 303, 307, 308):
                        print("Automatic redirect failed, handling manually")
                        return await self._handle_manual_redirect(session, method, url, max_redirects, **kwargs)

                    return await self._handle_response(response)

            except aiohttp.ClientError as e:
                print(f"Automatic redirect failed with error: {e}, attempting manual redirect")
                return await self._handle_manual_redirect(session, method, url, max_redirects, **kwargs)

    async def _handle_manual_redirect(self, session, method: str, url: str, max_redirects: int, **kwargs) -> str:
        """Handle redirects manually when automatic redirects fail"""
        redirect_count = 0
        current_url = url
        current_method = method

        while redirect_count < max_redirects:
            async with session.request(current_method, current_url, allow_redirects=False, **kwargs) as response:
                if response.status in (301, 302, 303, 307, 308):
                    redirect_count += 1
                    next_url = response.headers.get("Location")

                    if not next_url:
                        raise IntegrationAPIError(
                            self.integration_name,
                            f"Received redirect status {response.status} but no Location header",
                        )

                    # Handle relative URLs
                    if next_url.startswith('/'):
                        parsed_url = urllib.parse.urlparse(current_url)
                        next_url = f"{parsed_url.scheme}://{parsed_url.netloc}{next_url}"

                    print(f"Following manual redirect {redirect_count}/{max_redirects}: {next_url}")
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
            # potential auth caused
            reason = response.reason
            raise IntegrationAuthError(f"AppFolio: {status_code} - {reason}")
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
            "Scheduled": "3"
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
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            # Format it according to RFC 2822 with GMT timezone
            return date_obj.strftime('%a, %d %b %Y %H:%M:%S GMT')
        except ValueError as e:
            raise ValueError(f"Invalid date format. Please use YYYY-MM-DD format. Error: {e}")

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
        for item in response_json.get('included', []):
            key = (item['type'], item['id'])
            included_lookup[key] = item

        def resolve_relationship(relationship):
            """Helper function to resolve a single relationship"""
            if not relationship or 'data' not in relationship:
                return None

            rel_data = relationship['data']
            if not rel_data:  # Handle null relationships
                return None

            # Handle both single items and arrays
            if isinstance(rel_data, list):
                return [included_lookup.get((item['type'], item['id'])) for item in rel_data]
            else:
                key = (rel_data['type'], rel_data['id'])
                return included_lookup.get(key)

        def process_data_item(item):
            """Process a single data item and its relationships"""
            result = {
                'id': item['id'],
                'type': item['type'],
                # 'link': item.get('links', {}).get('page'),
                **item.get('attributes', {}),
                **item.get('links', {}),
            }

            # Process each relationship
            relationships = item.get('relationships', {})
            for rel_name, rel_data in relationships.items():
                resolved = resolve_relationship(rel_data)
                if resolved:
                    # If it's a list of relationships, get their attributes
                    if isinstance(resolved, list):
                        result[rel_name] = [
                            {**r.get('attributes', {}), 'id': r['id'], 'type': r['type']}
                            for r in resolved if r
                        ]
                    else:
                        # For single relationships, merge attributes directly
                        result[rel_name] = {
                            **resolved.get('attributes', {}),
                            'id': resolved['id'],
                            'type': resolved['type']
                        }

                        # Special handling for nested relationships (like property.address)
                        nested_relationships = resolved.get('relationships', {})
                        for nested_name, nested_rel in nested_relationships.items():
                            nested_resolved = resolve_relationship(nested_rel)
                            if nested_resolved:
                                result[f"{rel_name}_{nested_name}"] = {
                                    **nested_resolved.get('attributes', {}),
                                    'id': nested_resolved['id'],
                                    'type': nested_resolved['type']
                                }

            return result

        # Process all data items
        return [process_data_item(item) for item in response_json['data']]

    async def fetch_work_orders(self, status: str, start_date: str):
        params = {
            "page[size]": "100",
            "page[number]": "1",
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
            "include": "occupancy,unit,work_order_assigned_users.user,work_order_category,vendor,vendor_company,service_request,property,property.address"
        }
        filter_status_code = self._get_state_code(status)
        if filter_status_code:
            params.update({"filter[status_code]": filter_status_code})

        headers = self.headers.copy()
        headers["X-Api-Client"] = "/maintenance/service_requests/work_orders"
        headers["Accept-Version"] = "v2"
        headers["Accept"] = "application/vnd.api+json"

        url = f"{self.url}/api/work_orders"
        response = await self._make_request("GET", url, headers=headers, params=params)
        try:
            response = json.loads(response)
        except json.decoder.JSONDecodeError:
            print("not json response")

        denorm_response = self.denormalize_response(response)
        work_orders = []
        for order in denorm_response:
            parsed_order = await self._parse_work_order_page(url=order.get('page'))
            order.update(parsed_order)
            work_orders.append(order)

        return work_orders

    async def _parse_work_order_page(self, url: str):
        headers = self.headers.copy()
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8"

        # include extra parameters to support massive header sizes
        response = await self._make_request("GET", url, headers=headers, max_line_size=8190*15, max_field_size=8190*15)
        soup = self._create_soup(response)

        work_order_data = {}

        # get job description
        description_element = soup.select_one("div.js-work-order-description")
        if description_element:
            description = description_element.text.strip()
            work_order_data['description'] = description

        # get property, owner and resident info
        property_card_element = soup.select_one("div.js-property-contact-card")
        if property_card_element:
            property_card_data = property_card_element.select_one("div.js-contact-card-details")
            card_data_spans = property_card_data.select("span")
            property_data = "\n".join(span.text.strip() for span in card_data_spans)
            work_order_data['property'] = property_data.strip()

        owner_card_element = soup.select_one("div.js-owner-contact-card")
        if owner_card_element:
            owner_data = {}
            owner_name_span = owner_card_element.select_one("span.contact-card__name")
            if owner_name_span:
                owner_data['name'] = owner_name_span.text.strip()

            owner_contact_element = owner_card_element.select_one("div.js-contact-card-details")
            owner_contact_text = self._extract_text_from_div(owner_contact_element)
            owner_data['data'] = owner_contact_text

            work_order_data['owner'] = owner_data

        resident_card_element = soup.select_one("div.js-tenant-contact-card")
        if resident_card_element:
            resident_data = {}
            resident_name_span = resident_card_element.select_one("span.contact-card__name")
            if resident_name_span:
                resident_data['name'] = resident_name_span.text.strip()

            extra_div_element = resident_card_element.select_one("div.js-contact-card-details")
            extra_text = self._extract_text_from_div(extra_div_element)
            resident_data['data'] = extra_text.strip()

            work_order_data['resident'] = resident_data

        # get vendor info
        vendor_element = soup.select_one("div.js-vendor-contact-card")
        if vendor_element:
            vendor_name_element = vendor_element.select_one("span.contact-card__name")
            vendor_name = vendor_name_element.text.strip()

            vendor_contact_element = vendor_element.select_one("div.js-contact-card-details")
            vendor_contact = None
            if vendor_contact_element:
                contact_spans = vendor_contact_element.select("span")
                vendor_contact = "\n".join(span.text.strip() for span in contact_spans)

            vendor = {
                "name": vendor_name,
                "contact": vendor_contact,
            }
            work_order_data['vendor'] = vendor

        # get priority info
        priority_element = soup.find("span", text="Priority:")
        if priority_element:
            priority_element_value = soup.select_one("span.js-service-request-header-priority")
            if priority_element_value:
                priority = priority_element_value.text.strip()
                work_order_data['priority'] = priority

        return work_order_data

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
