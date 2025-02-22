import asyncio
import codecs
import re
import json
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
        self.url = "https://ocf.appfolio.com"
        self.headers = None
        self.token = None

    async def initialize(self, network_requester=None, tokens: dict|str = None):
        self.network_requester = network_requester
        self.headers = {
            'Host': 'ocf.appfolio.com',
            "User-Agent": self.user_agent,
        }

        if isinstance(tokens, dict):
            cookie_str = self._cookie_dict_to_string(tokens)
            self.token = cookie_str
            self.headers["Cookie"] = cookie_str

        if isinstance(tokens, str):
            cookie_str = tokens
            self.token = cookie_str
            self.headers["Cookie"] = cookie_str

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
            if self.token is None:
                raise IntegrationAuthError(
                    message="No access token. [Credentials might not exist/be valid]",
                    status_code=400,
                )
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
        raw_wo_list = []
        page_index = 1
        while True:
            params.update({ "page[number]": f"{page_index}"})
            response = await self._make_request("GET", url, headers=headers, params=params)
            try:
                response = json.loads(response)
            except json.decoder.JSONDecodeError:
                print(f"not json response: {response}")

            denorm_response = self.denormalize_response(response)
            if len(denorm_response) > 0:
                raw_wo_list.extend(denorm_response)
            else:
                break

            page_index += 1

        work_orders = []
        for order in raw_wo_list:
            parsed_order = await self._parse_work_order_page(url=order.get('page'))
            if order.get('vendor_company'):
                order.pop('vendor_company')
            if order.get('remarks'):
                order.pop('remarks')
            if order.get('work_order_assigned_users'):
                order.pop('work_order_assigned_users')

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
        service_id = self._extract_service_request_id(url)

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
            work_order_data['property'] = property_data.strip().replace("-5\n", "")

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

        # get priority information
        priority_element = soup.find("span", text="Priority:")
        if priority_element:
            priority_element_value = soup.select_one("span.js-service-request-header-priority")
            if priority_element_value:
                priority = priority_element_value.text.strip()
                work_order_data['priority'] = priority

        # get actions information
        actions_element = soup.select_one("div.js-activity-log")
        if actions_element:
            actions = []
            activity_rows = actions_element.select("div.js-activity-log-row")
            for row in activity_rows:
                activity_text = self._extract_text_from_div(row)
                actions.append(activity_text)

            work_order_data['actions'] = actions

        # get vendor instructions
        vendor_instructions_element = soup.select_one("div.js-work-order-vendor-instructions")
        if vendor_instructions_element:
            instructions = self._extract_text_from_div(vendor_instructions_element)
            work_order_data['vendor_instructions'] = instructions

        # get work order notes
        notes_element = soup.select_one("div#notes")
        if notes_element:
            notes_card_element = notes_element.select_one("div.card-body")
            if notes_card_element:
                notes = await self._fetch_notes(service_id=service_id)
                work_order_data['notes'] = notes

        # get attachments
        attachments_element = soup.select_one("div.js-work-order-body__attachments")
        if attachments_element:
            attachments = await self._fetch_attachments(service_id=service_id)
            work_order_data['attachments'] = attachments

        # get task assignee
        assignee_element = soup.select_one("div.js-assigned-to")
        if assignee_element:
            assignees = []
            assigned_to_elements = assignee_element.select("span.js-assignee-name")
            for each in assigned_to_elements:
                assigned_to_name = each.text.strip()
                assignees.append(assigned_to_name)
            work_order_data['assigned_to'] = assignees

        return work_order_data

    async def _fetch_notes(self, service_id: str):
        params = {
            'add_notes_for_id': f'{service_id}',
            'add_notes_for_type': 'Maintenance::ServiceRequestDecorator',
            'show_all': 'true',
            'show_notes_for_id': f'{service_id}',
            'show_notes_for_type': 'Maintenance::ServiceRequestDecorator',
        }
        url = f"{self.url}/notes"
        headers = self.headers.copy()
        headers['Accept'] = '*/*;q=0.5, text/javascript, application/javascript, application/ecmascript, application/x-ecmascript'

        response = await self._make_request("GET", url, headers=headers, params=params)
        # regex matching was not consistent
        start_idx = response.find('.html(')
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
        content = response[content_start + 1:content_end]
        # Replace escaped characters
        content = content.replace('\\"', '"')  # Unescape quotes
        content = content.replace('\\n', '\n')  # Handle newlines
        content = content.replace('\\/', '/')  # Handle forward slashes
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
        headers['Accept'] = "application/vnd.api+json"
        headers['Accept-Version'] = "v2"

        response = await self._make_request("GET", url, headers=headers)
        try:
            response = json.loads(response)
        except json.decoder.JSONDecodeError:
            print("failed to decode response for fetching attachments")
            return None

        included: list = response['included']
        attachments = []
        for included_item in included:
            if included_item.get('type') == "attachments":
                attached = included_item.get('attributes')
                attachments.append(attached)

        return attachments

    async def fetch_vacancies(self):
        url = f"{self.url}/vacancies"
        params = {
            'filters[properties_ids]': '',
            'filters[bedrooms]': '',
            'filters[min_rent]': '',
            'filters[max_rent]': '',
            'filters[available_from]': '',
            'filters[available_to]': '',
            'filters[cats]': '',
            'filters[dogs]': '',
            'filters[sort_by]': 'websitePostingVisible',
        }
        headers = self.headers.copy()
        headers['Accept'] = "application/json; q=0.01"
        response = await self._make_request("GET", url=url, headers=headers, params=params)
        try:
            response = json.loads(response)
            results_html = response.get('results_html')
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
            # headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            headers['Accept'] = '*/*'

            parsed = self._parse_vacancy_card(card=vacancy_item)
            property_url = parsed.get('link')
            if "campaigns" in property_url:
                # the response for this is js and has html elements for the modal; the actual url is in here
                campaign_resp = await self._make_request("GET", url=property_url, headers=headers,
                                                         max_line_size=8190*15, max_field_size=8190*15)
                d_start = campaign_resp.find("campaign_unit_type_link")
                c_data = campaign_resp[d_start:d_start+150]
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
                    property_url = "https://ocf.appfolio.com" + property_url
                    parsed["link"] = property_url

            if property_url is None:
                return parsed

            property_page = await self._make_request(method="GET", url=property_url, headers=headers,
                                                     max_line_size=8190*15, max_field_size=8190*15)
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
            unit_type_elem = unit_desc_elem.select_one("div.js-unit_template_key_value_datapair")
            if unit_type_elem is not None:
                unit_type = unit_type_elem.select_one("div.datapair__value")
                unit_data["type"] = unit_type.text.strip()

        property_desc_elem = soup.select_one("div.property-name-and-address")
        if property_desc_elem is not None:
            property_type_elem = property_desc_elem.select_one("div#property_type_value")
            if property_type_elem is not None:
                property_data["type"] = property_type_elem.text.strip()

            county_elem = property_desc_elem.select_one("div.js-marketing-property-county")
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

        property_rental_info_elem = soup.select_one("div#property_rental_information_show")
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

        property_marketing_elem = soup.select_one("div#property_marketing_information_show")
        if property_marketing_elem is not None:
            info_pairs = property_marketing_elem.select("div.datapair")
            property_info = AppFolioIntegration._parse_data_pairs(info_pairs)
            property_data["marketing_info"] = property_info

        # for campaign pages
        campaign_rental_elem = soup.select_one("div#unit_template_basic_information_show")
        if campaign_rental_elem is not None:
            info_pairs = campaign_rental_elem.select("div.datapair")
            campaign_rental_info = AppFolioIntegration._parse_data_pairs(info_pairs)
            campaign_unit_data["rental_info"] = campaign_rental_info

        campaign_marketing_elem = soup.select_one("div#unit_template_basic_information_show")
        if campaign_marketing_elem is not None:
            info_pairs = campaign_marketing_elem.select("div.datapair")
            campaign_marketing_info = AppFolioIntegration._parse_data_pairs(info_pairs)
            campaign_unit_data["marketing_info"] = campaign_marketing_info

        # First find the h2 with "Amenities" text
        amenities_header = soup.find('h2', text=lambda text: text and text.strip() == 'Amenities')

        # Navigate to the card-header div
        if amenities_header:
            if data.get('amenities') is None:
                card_header = amenities_header.find_parent('div', class_='card-header')

                # Then find the parent section element
                if card_header:
                    section_parent = card_header.find_parent('section')
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

    @staticmethod
    def _parse_vacancy_card(card: Tag):
        vacancy = {}
        name_elem = card.select_one("span.js-card-title")
        if name_elem is not None:
            vacancy['name'] = name_elem.text.strip()

            link_elem = name_elem.select_one("a")
            link = link_elem.get("href")
            vacancy['link'] = "https://ocf.appfolio.com" + link

        address_elem = card.select_one("span.js-card-address")
        if address_elem is not None:
            address = address_elem.text.strip()
            address = address.split("Edit")[0]
            vacancy['address'] = address

        rent_table_elem = card.select_one("table.unit-property-card__table")
        if rent_table_elem is not None:
            rent_data = []
            table_bits = rent_table_elem.select("td")
            for item in table_bits:
                item_data = {}
                item_title_elem = item.select_one("span.unit-property-card__tiny-header")
                item_title = item_title_elem.text.strip()
                item_title = codecs.decode(item_title, "unicode-escape")
                item_value_elem = item.select_one('[class^="js-card"]')
                item_value = item_value_elem.text.strip()
                item_value = codecs.decode(item_value, "unicode-escape")

                item_data[item_title] = item_value
                rent_data.append(item_data)

            vacancy['rent_data'] = rent_data

        actions_elem = card.select_one("div.action-table")
        rent_status_card = actions_elem.select_one("p.js-vacancy-type")
        if rent_status_card is not None:
            rent_status = rent_status_card.text.strip()
            vacancy['rent_status'] = rent_status

        actions_table_elem = actions_elem.select_one("table")
        if actions_table_elem is not None:
            website_status_row = actions_table_elem.select_one("tr.js-website-tasks")
            if website_status_row is not None:
                value_elem = website_status_row.select_one("td.js-task-status")
                vacancy['website_status'] = value_elem.text.strip()

            internet_status_row = actions_table_elem.select_one("tr.js-internet-tasks")
            if internet_status_row is not None:
                value_elem = internet_status_row.select_one("td.js-task-status")
                vacancy['internet_status'] = value_elem.text.strip()

            premium_status_row = actions_table_elem.select_one("tr.js-premium-tasks")
            if premium_status_row is not None:
                value_elem = premium_status_row.select_one("td.js-task-status")
                vacancy['premium_status'] = value_elem.text.strip()

            refresh_status_row = actions_table_elem.select_one("td.action-table__refresh-container")
            if refresh_status_row is not None:
                vacancy['last_updated'] = refresh_status_row.text.strip()

        return vacancy

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
        match = re.search(r'/service_requests/(\d+)/', url)

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
