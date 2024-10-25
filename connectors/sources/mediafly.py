"""
Mediafly connector for Elastic Enterprise Search.

This module provides a connector to index Mediafly content into Elastic Enterprise Search.
It includes classes for interacting with the Mediafly API and processing Mediafly items.
"""

import logging
from functools import partial
from typing import Any, Dict, List, Optional, AsyncGenerator
import aiohttp
from connectors.source import BaseDataSource
from connectors.logger import logger
from connectors.utils import TIKA_SUPPORTED_FILETYPES

LOG_LEVEL = "INFO"
logger.setLevel(getattr(logging, LOG_LEVEL, logging.DEBUG))

# Add a console handler if not already added
if LOG_LEVEL == "DEBUG":
    if not logger.hasHandlers():
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)  # Ensure the handler level is set to DEBUG
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    else:
        # Ensure existing handlers are set to DEBUG
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)

FILE_WRITE_CHUNK_SIZE = 1024 * 64
MAX_CONCURRENT_DOWNLOADS = 50

# Define global flags for log message length
LOG_LENGTH_DEBUG = None  # Set to an integer to truncate debug messages
LOG_LENGTH_INFO = None  # Set to an integer to truncate info messages
LOG_LENGTH_WARNING = None  # Set to an integer to truncate warning messages
LOG_LENGTH_ERROR = None  # Set to an integer to truncate error messages
LOG_LENGTH_EXCEPTION = None  # Set to an integer to truncate exception messages


def print_message(level: str, message: str):
    """
    Print a log message with optional truncation based on the log level.
    Setting log message string length is useful when debugging parts of the application that operate on binary files.
    """
    length = {
        "debug": LOG_LENGTH_DEBUG,
        "info": LOG_LENGTH_INFO,
        "warning": LOG_LENGTH_WARNING,
        "error": LOG_LENGTH_ERROR,
        "exception": LOG_LENGTH_EXCEPTION,
    }.get(level, None)

    if length is not None and len(message) > length:
        message = message[:length] + "..."

    {
        "debug": logger.debug,
        "info": logger.info,
        "warning": logger.warning,
        "error": logger.error,
        "exception": logger.exception,
    }[level](message)


class MediaflyClient:
    """
    MediaflyClient is a client for the Mediafly Launch Pad API.
    """

    def __init__(self, api_key: str, product_id: str):
        """
        Initialize the MediaflyClient.

        Args:
            api_key (str): The API key for authentication.
            product_id (str): The product ID for the Mediafly environment.
        """
        self.api_key = api_key
        self.product_id = product_id
        self.base_url = "https://launchpadapi.mediafly.com/3"
        self.headers = {"Authorization": f"Bearer {self.api_key}"}
        self.session = aiohttp.ClientSession()

    async def close(self):
        """Close the aiohttp session."""
        await self.session.close()

    async def get_item(self, item_id: str) -> Dict[str, Any]:
        """
        Retrieve a single item from Mediafly.

        Args:
            item_id (str): The ID of the item to retrieve.

        Returns:
            Dict[str, Any]: The item data.
        """
        url = f"{self.base_url}/items/{item_id}"
        params = {"productId": self.product_id}
        async with self.session.get(url, params=params, headers=self.headers) as resp:
            if resp.status != 200:
                print_message("error", f"Error getting item {item_id}: {resp.status} - {await resp.text()}")
                return {}
            print_message("debug", f"Item {item_id} retrieved: {await resp.json()}")
            return await resp.json()

    async def get_child_items(self, item_id: str, exclude_folder_ids: List[str] = []) -> List[Dict[str, Any]]:
        """
        Retrieve all child items of a given item (folder) recursively.

        Args:
            item_id (str): The ID of the parent item.
            exclude_folder_ids (List[str]) optional: A list of folder IDs to exclude from the results.
        Returns:
            List[Dict[str, Any]]: A list of child items.
        """
        items = []
        if item_id in exclude_folder_ids:
            print_message("debug", f"Skipping folder ID: {item_id} as it is in the exclude list.")
            return items
        item = await self.get_item(item_id)

        print_message("debug", f"Processing folder ID: {item_id}")

        # Check if the current item is a folder
        if item.get("response", {}).get("type") == "folder":
            for child in item.get("response", {}).get("items", []):
                if child.get("type") == "folder":
                    print_message("debug", f"Found subfolder: {child.get('name', '')} (ID: {child.get('id', '')})")
                    items.extend(await self.get_child_items(child["id"], exclude_folder_ids))
                else:
                    print_message("debug", f"Found item: {child.get('name', '')} (ID: {child.get('id', '')})")
                    items.append(child)
        else:
            # If it's not a folder, just add the item itself
            items.append(item.get("response", {}))

        return items

    async def download_item(self, attachment: Dict[str, Any]) -> Optional[aiohttp.ClientResponse]:
        """
        Download the content of a Mediafly item.

        Args:
            item (Dict[str, Any]): The item data.

        Returns:
            Optional[aiohttp.ClientResponse]: The HTTP response object or None if download fails.
        """

        download_url = attachment.get("downloadUrl")
        if not download_url:
            print_message("warning", f"No download URL for: {attachment.get('filename', '')}")
            return None

        print_message("info", f"Downloading: {attachment.get('filename', '')}")
        resp = await self.session.get(download_url, headers=self.headers)
        if resp.status != 200:
            print_message(
                "error",
                f"Error downloading item {attachment.get('filename', '')}: {resp.status} - {await resp.text()}",
            )
            return None
        return resp


class MediaflyDataSource(BaseDataSource):  # pylint: disable=abstract-method
    """
    MediaflyDataSource is a connector for indexing Mediafly content into Elastic Enterprise Search.
    """

    name = "Mediafly"
    service_type = "mediafly"
    incremental_sync_enabled = True

    def __init__(self, configuration: Dict[str, Any]):
        super().__init__(configuration=configuration)
        self.api_key = self.configuration["api_key"]
        self.product_id = self.configuration["product_id"]
        self.folder_ids = self.configuration["folder_ids"]
        self.exclude_file_types = (
            [x.strip() for x in self.configuration["exclude_file_types"].split(",")]
            if self.configuration["exclude_file_types"]
            else []
        )
        self.exclude_folder_ids = (
            [x.strip() for x in self.configuration["exclude_folder_ids"].split(",")]
            if self.configuration["exclude_folder_ids"]
            else []
        )
        self.include_internal_only_files = self.configuration.get("include_internal_only_files", False)
        self.use_text_extraction_service = self.configuration.get("use_text_extraction_service", False)
        self.tika_supported_filetypes = TIKA_SUPPORTED_FILETYPES
        self.client = MediaflyClient(api_key=self.api_key, product_id=self.product_id)

    @classmethod
    def get_default_configuration(cls) -> Dict[str, Any]:
        """
        Get the default configuration for the Mediafly connector.
        These options are displayed in the Elastic Enterprise Search UI when configuring the connector.

        Returns:
            Dict[str, Any]: The default configuration.
        """
        return {
            "api_key": {
                "label": "API Key",
                "order": 1,
                "type": "str",
                "value": "",
                "required": True,
            },
            "product_id": {
                "label": "Product ID (env_id)",
                "order": 2,
                "type": "str",
                "value": "",
                "required": True,
            },
            "folder_ids": {
                "label": "Folder IDs",
                "tooltip": "The IDs of the folders to index. Separate multiple IDs with commas.",
                "order": 3,
                "type": "list",
                "value": [],
                "required": True,
            },
            "exclude_file_types": {
                "label": "Exclude file types",
                "tooltip": "The file types to exclude from indexing. Separate multiple types with commas.",
                "order": 4,
                "type": "str",
                "value": "",
            },
            "exclude_folder_ids": {
                "label": "Exclude folder IDs",
                "tooltip": "The IDs of the folders to exclude from indexing. Separate multiple IDs with commas.",
                "order": 5,
                "type": "str",
                "value": "",
                "required": False,
            },
            "use_share_links": {
                "display": "toggle",
                "label": "Use share links",
                "order": 6,
                "tooltip": "Use share links when available to access files instead of the direct download URL.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "include_internal_only_files": {
                "display": "toggle",
                "label": "Include internal-only files",
                "order": 7,
                "tooltip": "Include files marked as internal-only in the Mediafly environment.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_DOWNLOADS,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 8,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "validations": [{"type": "less_than", "constraint": MAX_CONCURRENT_DOWNLOADS + 1}],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 9,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }

    async def ping(self) -> None:
        """
        Ping the Mediafly API to check if the connector is working.
        """
        try:
            await self.client.get_item(self.folder_ids[0])
            print_message("info", "Successfully connected to Mediafly.")
        except aiohttp.ClientError as e:
            print_message("exception", f"Network error pinging Mediafly: {e}")
            raise
        except KeyError as e:
            print_message("exception", f"Invalid folder ID configuration: {e}")
            raise

    async def close(self) -> None:
        await self.client.close()

    async def get_content(self, attachment, timestamp=None, doit=False):
        """
        Extracts the content for Apache TIKA supported file types.

        Args:
            attachment (Dict[str, Any]): The attachment data.
            timestamp (Optional[str]): The timestamp of the last sync.
            doit (bool): Whether to download the file.

        Returns:
            Dict[str, Any]: The document data.
        """
        file_id = attachment.get("id")
        filename = attachment.get("filename", "")
        file_size = int(attachment.get("size", 0))

        if not (doit and file_size > 0):
            return

        file_extension = self.get_file_extension(filename)

        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        document = {
            "_id": file_id,
            "_timestamp": timestamp or attachment.get("modified"),
        }

        async def download_func():
            try:
                response = await self.client.download_item(attachment)
                if response is None:
                    print_message("error", f"Failed to download item {file_id}: No response received.")
                    return
                # Yield data chunks directly
                async for chunk in response.content.iter_chunked(FILE_WRITE_CHUNK_SIZE):
                    yield chunk
            except aiohttp.ClientResponseError as e:
                print_message("exception", f"HTTP error downloading item {file_id}: {e.status} - {e.message}")
            except aiohttp.ClientError as e:
                print_message("exception", f"Network error downloading item {file_id}: {e}")

        # Use the download_func directly without generic_chunked_download_func
        extracted_doc = await self.download_and_extract_file(document, filename, file_extension, download_func)

        if extracted_doc:
            if "_attachment" in extracted_doc:
                document["_attachment"] = extracted_doc["_attachment"]

        return document

    async def get_docs(self, filtering=None) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Get all documents from Mediafly.

        Args:
            filtering (Optional[Dict[str, Any]], optional): Filtering options. Defaults to None.

        Yields:
            AsyncGenerator[Dict[str, Any], None]: Generator of document data.
        """
        for folder_id in self.folder_ids:
            print_message("info", f"Processing folder ID: {folder_id}")
            items = await self.client.get_child_items(folder_id, self.exclude_folder_ids)
            for item in items:
                doc = self._create_doc_from_item(item)

                # Check if the file is TIKA-supported and not excluded
                asset = item.get("asset", {})
                asset["id"] = item.get("id")
                asset["modified"] = item.get("modified")
                asset["internalOnly"] = item.get("metadata", {}).get("internalOnly", False)

                if not self._pre_checks_for_get_docs(asset):
                    continue

                # Yield each document as we process it.
                # The first value is the document metadata,
                # the second is a partial function to get the document content,
                # and the third is the operation (OP_INDEX for existing or new documents).
                yield doc, partial(self.get_content, asset)

    def _pre_checks_for_get_docs(self, asset):
        """
        Perform pre-checks for the get_docs method.

        Args:
            asset (Dict[str, Any]): The asset data.

        Returns:
            bool: True if the attachment passes all checks, False otherwise.
        """
        filename = asset.get("filename", "")
        file_extension = self.get_file_extension(filename)

        if not self.include_internal_only_files and asset.get("internalOnly", True):
            print_message(
                "debug", f"Skipping {filename} as it is internal-only and include_internal_only_files is False."
            )
            return False

        if file_extension.lower() not in TIKA_SUPPORTED_FILETYPES:
            print_message("debug", f"Skipping {filename} as it is not TIKA-supported.")
            return False

        if file_extension.lower().strip(".") in self.exclude_file_types:
            print_message("debug", f"Skipping {filename} as it is in the excluded file types.")
            return False

        return True

    def _create_doc_from_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a document dictionary from a Mediafly item.

        Args:
            item (Dict[str, Any]): The Mediafly item data.

        Returns:
            Dict[str, Any]: The formatted document dictionary.
        """
        doc = {
            "_id": item.get("id"),
            "_timestamp": item.get("modified"),
            "name": item.get("metadata", {}).get("title"),
            "type": item.get("metadata", {}).get("contentType"),
            "description": item.get("metadata", {}).get("description"),
            "object_type": "mediafly_item",
            "createdDateTime": item.get("created"),
            "lastModifiedDateTime": item.get("modified"),
            "filename": item.get("asset", {}).get("filename"),
            "size": item.get("asset", {}).get("size"),
            "parentReference": {
                "id": item.get("parentId"),
                "name": next((h["title"] for h in item.get("hierarchy", []) if h["id"] == item.get("parentId")), None),
            },
            "lastModifiedBy": {
                "user": {
                    "displayName": item.get("modifiedBy"),
                    "email": item.get("modifiedBy"),  # Assuming email is the same as the display name
                }
            },
            "createdBy": {
                "user": {
                    "displayName": item.get("createdBy"),
                    "email": item.get("createdBy"),  # Assuming email is the same as the display name
                }
            },
            "_allow_access_control": list(
                set(f"{perm['assigneeType']}:{perm['assigneeName']}" for perm in item.get("inheritedPermissions", []))
            ),
        }

        doc["webUrl"] = item.get("asset", {}).get("url")

        # Check for shareLinks and use the first one's URL as webUrl if available
        if self.configuration.get("use_share_links"):
            share_links = item.get("shareLinks", [])
            if share_links and isinstance(share_links, list) and len(share_links) > 0:
                doc["webUrl"] = share_links[0].get("url")

        # Store the original download URL for the connector to use
        doc["_original_download_url"] = item.get("asset", {}).get("downloadUrl")

        return doc
