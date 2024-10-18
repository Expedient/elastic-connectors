from functools import partial
from typing import Any, Dict, List, Optional, AsyncGenerator
import aiohttp
from aiofiles.tempfile import NamedTemporaryFile
from connectors.source import BaseDataSource
from connectors.logger import logger
from connectors.utils import TIKA_SUPPORTED_FILETYPES, convert_to_b64
import os

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
    def __init__(self, api_key: str, product_id: str):
        self.api_key = api_key
        self.product_id = product_id
        self.base_url = "https://launchpadapi.mediafly.com/3"
        self.headers = {"Authorization": f"Bearer {self.api_key}"}
        self.session = aiohttp.ClientSession()

    async def close(self):
        await self.session.close()

    async def get_item(self, item_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/items/{item_id}"
        params = {"productId": self.product_id}
        async with self.session.get(url, params=params, headers=self.headers) as resp:
            if resp.status != 200:
                print_message("error", f"Error getting item {item_id}: {resp.status} - {await resp.text()}")
                return {}
            return await resp.json()

    async def get_child_items(self, item_id: str) -> List[Dict[str, Any]]:
        items = []
        item = await self.get_item(item_id)
        print_message("info", f"Processing folder ID: {item_id}")

        # Check if the current item is a folder
        if item.get("response", {}).get("type") == "folder":
            for child in item.get("response", {}).get("items", []):
                if child.get("type") == "folder":
                    print_message(
                        "info", f"Found subfolder: {child.get('name', 'Unknown')} (ID: {child.get('id', 'Unknown')})"
                    )
                    items.extend(await self.get_child_items(child["id"]))
                else:
                    print_message(
                        "info", f"Found item: {child.get('name', 'Unknown')} (ID: {child.get('id', 'Unknown')})"
                    )
                    items.append(child)
        else:
            # If it's not a folder, just add the item itself
            items.append(item.get("response", {}))

        return items

    async def download_item(self, item: Dict[str, Any]) -> Optional[bytes]:
        asset = item.get("asset", {})
        if not asset:
            print_message("warning", f"No asset information found for item: {item.get('name', 'Unknown')}")
            return None

        download_url = asset.get("downloadUrl")
        if not download_url:
            print_message("warning", f"No download URL for: {item.get('name', 'Unknown')}")
            return None

        print_message("info", f"Downloading: {asset.get('filename', 'Unknown')}")
        async with self.session.get(download_url, headers=self.headers) as resp:
            if resp.status != 200:
                print_message("error", f"Error downloading item {item.get('id')}: {resp.status} - {await resp.text()}")
                return None
            content = await resp.read()
            return content


class MediaflyDataSource(BaseDataSource):
    name = "Mediafly"
    service_type = "mediafly"

    def __init__(self, configuration: Dict[str, Any]):
        super().__init__(configuration=configuration)
        self.api_key = self.configuration["api_key"]
        self.product_id = self.configuration["product_id"]
        self.folder_ids = self.configuration["folder_ids"]
        self.exclude_file_types = self.configuration["exclude_file_types"]
        self.client = MediaflyClient(api_key=self.api_key, product_id=self.product_id)
        self.use_text_extraction_service = self.configuration.get("use_text_extraction_service", False)
        self.tika_supported_filetypes = TIKA_SUPPORTED_FILETYPES

    @classmethod
    def get_default_configuration(cls) -> Dict[str, Any]:
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
                "order": 3,
                "type": "list",
                "value": [],
                "required": True,
            },
            "exclude_file_types": {
                "label": "Exclude file types",
                "order": 4,
                "type": "list",
                "value": [],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 8,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }

    async def ping(self) -> None:
        try:
            # Implement a simple health check by fetching a known item or checking API status
            await self.client.get_item(self.folder_ids[0])
            print_message("info", "Successfully connected to Mediafly.")
        except Exception as e:
            print_message("exception", f"Error pinging Mediafly: {e}")
            raise

    async def close(self) -> None:
        await self.client.close()

    def _pre_checks_for_get_content(self, attachment_name, attachment_size):
        if not attachment_name:
            print_message("debug", "Attachment name is empty, skipping.")
            return False

        attachment_extension = attachment_name[attachment_name.rfind(".") :] if "." in attachment_name else ""

        if attachment_extension == "":
            print_message("debug", f"Files without extension are not supported, skipping {attachment_name}.")
            return False

        if attachment_extension.lower() in self.exclude_file_types:
            print_message(
                "debug",
                f"Files with the extension {attachment_extension} are not supported, skipping {attachment_name}.",
            )
            return False

        if attachment_extension.lower() not in TIKA_SUPPORTED_FILETYPES:
            print_message(
                "debug",
                f"Files with the extension {attachment_extension} are not supported by TIKA, skipping {attachment_name}.",
            )
            return False

        if attachment_size > self.framework_config.max_file_size and not self.use_text_extraction_service:
            print_message(
                "warning",
                f"File size {attachment_size} of file {attachment_name} is larger than {self.framework_config.max_file_size} bytes. Discarding file content",
            )
            return False

        if attachment_size > 100000000:
            print_message(
                "warning",
                f"File size {attachment_size} of file {attachment_name} is larger than 100MB. Discarding file content.",
            )
            return False
        return True

    async def get_content(
        self, item: Dict[str, Any], doit: bool = False, timestamp: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        if not doit:
            return None

        asset = item.get("asset", {})
        attachment_size = int(asset.get("size", 0))
        if not (doit and attachment_size > 0):
            return None

        attachment_name = asset.get("filename", "")
        attachment_extension = os.path.splitext(attachment_name)[1].lower()

        if attachment_size <= 0:
            return None

        if not self._pre_checks_for_get_content(attachment_name=attachment_name, attachment_size=attachment_size):
            return None

        download_url = asset.get("downloadUrl")
        if not download_url:
            print_message("warning", f"No download URL found for item: {attachment_name}")
            return None

        try:
            async with NamedTemporaryFile(
                mode="wb", delete=False, suffix=attachment_extension, dir=self.download_dir
            ) as temp_file:
                temp_file_name = temp_file.name
                async with self.client.session.get(download_url, headers=self.client.headers) as response:
                    if response.status != 200:
                        print_message(
                            "error",
                            f"Failed to download content for item {item.get('id')}: {response.status}",
                        )
                        return None

                    while True:
                        chunk = await response.content.read(8192)  # Read in 8KB chunks
                        if not chunk:
                            break
                        await temp_file.write(chunk)

            if (
                self.configuration.get("use_text_extraction_service")
                and attachment_extension in self.tika_supported_filetypes
            ):
                if hasattr(self, "extraction_service") and self.extraction_service._check_configured():
                    try:
                        print_message("info", f"Extracting text from {attachment_name} using extraction service")
                        extracted_text = await self.extraction_service.extract_text(temp_file_name, attachment_name)
                        if not extracted_text:
                            print_message("warning", f"Extraction service returned empty text for {attachment_name}")
                        return {
                            "_id": item.get("id"),
                            "_timestamp": item.get("modified"),
                            "body": extracted_text,
                        }
                    except Exception as e:
                        print_message(
                            "error",
                            f"Text extraction failed for item {item.get('id')}: {str(e)}",
                        )
                        return None
                else:
                    print_message(
                        "warning",
                        f"Extraction service not configured for item {item.get('id')}",
                    )
                    return None
            else:
                with open(temp_file_name, "rb") as file:
                    encoded_content = convert_to_b64(file.read())
                return {
                    "_id": item.get("id"),
                    "_timestamp": item.get("modified"),
                    "_attachment": encoded_content,
                }
        except Exception as e:
            print_message(
                "exception",
                f"Error downloading content for item {item.get('id')}: {e}",
            )
            return None
        finally:
            if temp_file_name:
                try:
                    os.remove(temp_file_name)
                except Exception as e:
                    print_message("warning", f"Failed to remove temporary file {temp_file_name}: {e}")

    async def get_docs(self, filtering: Optional[Dict[str, Any]] = None) -> AsyncGenerator[Dict[str, Any], None]:
        try:
            for folder_id in self.folder_ids:
                print_message("info", f"Processing folder ID: {folder_id}")
                items = await self.client.get_child_items(folder_id)
                for item in items:
                    doc = {
                        "_id": item.get("id"),
                        "_timestamp": item.get("modified"),
                        "name": item.get("metadata", {}).get("title"),
                        "type": item.get("metadata", {}).get("contentType"),
                        "description": item.get("metadata", {}).get("description"),
                        "object_type": "mediafly_item",
                        "createdDateTime": item.get("created"),
                        "lastModifiedDateTime": item.get("modified"),
                        "size": item.get("asset", {}).get("size"),
                        "parentReference": {
                            "id": item.get("parentId"),
                            "name": next(
                                (h["title"] for h in item.get("hierarchy", []) if h["id"] == item.get("parentId")), None
                            ),
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
                            set(
                                f"{perm['assigneeType']}:{perm['assigneeName']}"
                                for perm in item.get("inheritedPermissions", [])
                            )
                        ),
                    }

                    # Check for shareLinks and use the first one's URL as webUrl if available
                    share_links = item.get("shareLinks", [])
                    if share_links and isinstance(share_links, list) and len(share_links) > 0:
                        doc["webUrl"] = share_links[0].get("url")
                    else:
                        doc["webUrl"] = item.get("asset", {}).get("url")

                    # Store the original download URL for the connector to use
                    doc["_original_download_url"] = item.get("asset", {}).get("downloadUrl")

                    yield doc, partial(self.get_content, item)
        except Exception as e:
            print_message("exception", f"Error fetching documents from Mediafly: {e}")
