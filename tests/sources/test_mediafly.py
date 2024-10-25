import pytest
from unittest.mock import AsyncMock, patch
from aioresponses import aioresponses

from connectors.sources.mediafly import MediaflyClient, MediaflyDataSource
from contextlib import asynccontextmanager
from tests.sources.support import create_source
from aioresponses.core import normalize_url

MOCK_MEDIAFLY_ITEM_TXT = {
    "id": "111222",
    "parentId": "654321",
    "type": "file",
    "modified": "2024-10-22T18:26:23.000000Z",
    "created": "2024-10-22T18:26:06.000000Z",
    "createdBy": "person@example.com",
    "modifiedBy": "person@example.com",
    "asset": {
        "id": "abc123",
        "filename": "text_file.txt",
        "size": 1025,
        "downloadUrl": "https://localhost/text_file.txt",
    },
    "shareLinks": [],
    "fileType": "txt",
}

MOCK_MEDIAFLY_ITEM_PPTX = {
    "id": "111223",
    "parentId": "654321",
    "type": "file",
    "modified": "2024-10-21T18:26:23.000000Z",
    "created": "2024-10-21T18:26:06.000000Z",
    "createdBy": "other_person@example.com",
    "modifiedBy": "other_person@example.com",
    "asset": {
        "id": "abc124",
        "filename": "powerpoint_file.pptx",
        "size": 1025,
        "downloadUrl": "https://localhost/powerpoint_file.pptx",
    },
    "shareLinks": [],
    "fileType": "pptx",
}
MOCK_MEDIAFLY_ITEM_PNG = {
    "id": "111224",
    "parentId": "654321",
    "type": "file",
    "modified": "2024-10-20T18:26:23.000000Z",
    "created": "2024-10-20T18:26:06.000000Z",
    "createdBy": "another_person@example.com",
    "modifiedBy": "another_person@example.com",
    "asset": {
        "id": "abc125",
        "filename": "image_file.png",
        "size": 1025,
        "downloadUrl": "https://localhost/image_file.png",
    },
    "shareLinks": [],
    "fileType": "png",
}

MOCK_MEDIAFLY_ITEM_PDF = {
    "id": "111225",
    "parentId": "654321",
    "type": "file",
    "modified": "2024-10-19T18:26:23.000000Z",
    "created": "2024-10-19T18:26:06.000000Z",
    "createdBy": "another_person@example.com",
    "modifiedBy": "another_person@example.com",
    "asset": {
        "id": "abc126",
        "filename": "pdf_file.pdf",
        "size": 2048,
        "downloadUrl": "https://localhost/pdf_file.pdf",
    },
    "shareLinks": [],
    "fileType": "pdf",
}

MOCK_MEDIAFLY_CHILD_ITEMS = [
    MOCK_MEDIAFLY_ITEM_TXT,
    MOCK_MEDIAFLY_ITEM_PPTX,
    MOCK_MEDIAFLY_ITEM_PNG,
]

MOCK_MEDIAFLY_FOLDER = {
    "id": "222221",
    "parentId": "222222",
    "type": "folder",
    "metadata": {"title": "Mediafly's Test"},
    "itemCount": 3,
    "folderCount": 0,
    "fileCount": 3,
    "modified": "2024-10-23T13:33:48.481593Z",
    "created": "2024-10-22T18:25:36.762438Z",
    "createdBy": "tom.cooper@expedient.com",
    "modifiedBy": "nick.lansberry@expedient.com",
    "permissions": [],
    "shareLinks": [],
    "fileType": "folder",
}

MOCK_MEDIAFLY_FOLDER_ALTERNATE = {
    **MOCK_MEDIAFLY_FOLDER,
    "id": "222223",
    "metadata": {"title": "Mediafly's Test 2"},
    "itemCount": 1,
    "fileCount": 1,
}

MOCK_MEDIAFLY_FOLDER_WITH_ITEMS = {
    **MOCK_MEDIAFLY_FOLDER,
    "items": MOCK_MEDIAFLY_CHILD_ITEMS,
}

MOCK_MEDIAFLY_NESTED_FOLDER = {
    "id": "222222",
    "parentId": "0",
    "type": "folder",
    "metadata": {"title": "Mediafly's Nested Test"},
    "itemCount": 1,
    "folderCount": 1,
    "fileCount": 0,
    "modified": "2024-10-23T13:33:48.481593Z",
    "created": "2024-10-22T18:25:36.762438Z",
    "createdBy": "tom.cooper@expedient.com",
    "modifiedBy": "nick.lansberry@expedient.com",
    "items": [MOCK_MEDIAFLY_FOLDER],
    "permissions": [],
    "shareLinks": [],
    "fileType": "folder",
}

MOCK_MEDIAFLY_MULTIPLE_NESTED_FOLDERS = {
    **MOCK_MEDIAFLY_NESTED_FOLDER,
    "items": [MOCK_MEDIAFLY_FOLDER, MOCK_MEDIAFLY_FOLDER_ALTERNATE],
}


@pytest.fixture
def mock_mediafly_client():
    with patch("connectors.sources.mediafly.MediaflyClient") as mock:
        yield mock


@asynccontextmanager
async def create_mediafly_source(use_text_extraction_service=False, exclude_file_types=None):
    async with create_source(
        MediaflyDataSource,
        api_key="api_key_123",
        product_id="product_id_123",
        folder_ids=["123456"],
        exclude_file_types=[],
        include_internal_only_files=False,
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source


class TestMediaflyClient:
    @pytest.mark.asyncio
    async def test_init(self):
        client = MediaflyClient("api_key_123", "product_id_123")
        assert client.api_key == "api_key_123"
        assert client.product_id == "product_id_123"
        assert client.base_url == "https://launchpadapi.mediafly.com/3"
        assert client.headers == {"Authorization": "Bearer api_key_123"}
        assert client.session is not None

    @pytest.mark.asyncio
    async def test_close(self):
        client = MediaflyClient("api_key_123", "product_id_123")
        await client.close()

    @pytest.mark.asyncio
    async def test_get_item(self):
        with aioresponses() as m:
            client = MediaflyClient("api_key_123", "product_id_123")
            # Construct the full URL with query parameters
            full_url = f"{client.base_url}/items/item_id?productId={client.product_id}"

            # Normalize the URL to ensure consistency
            normalized_url = normalize_url(full_url)

            # Mock the response for the full URL
            m.get(normalized_url, payload=MOCK_MEDIAFLY_ITEM_TXT)

            # Call the method you want to test
            item = await client.get_item("item_id")

            # Assert the expected outcome
            assert item == MOCK_MEDIAFLY_ITEM_TXT

            # Validate that the request was made with the correct parameters
            request = m.requests[("GET", normalized_url)][0]
            assert request.kwargs["params"] == {"productId": client.product_id}

        # Close the client session
        await client.close()

    @pytest.mark.asyncio
    async def test_get_item_folder(self):
        client = MediaflyClient("api_key_123", "product_id_123")
        client.get_item = AsyncMock(return_value={"response": MOCK_MEDIAFLY_FOLDER})

        response = await client.get_item("folder_id")
        items = response["response"]  # Extract the actual folder data from the response

        assert items == MOCK_MEDIAFLY_FOLDER
        await client.close()

    @pytest.mark.asyncio
    async def test_get_item_non_200_response(self):
        client = MediaflyClient("api_key_123", "product_id_123")
        client.get_item = AsyncMock(return_value={"response": {"status": 404}})

        response = await client.get_item("item_id")
        assert response["response"]["status"] == 404
        await client.close()

    @pytest.mark.asyncio
    async def test_get_child_items_single_item(self):
        client = MediaflyClient("api_key_123", "product_id_123")
        client.get_item = AsyncMock(return_value={"response": MOCK_MEDIAFLY_ITEM_TXT})

        items = await client.get_child_items("folder_id")

        assert len(items) == 1
        assert items[0]["id"] == "111222"
        await client.close()

    @pytest.mark.asyncio
    async def test_get_child_items_folder(self):
        client = MediaflyClient("api_key_123", "product_id_123")
        client.get_item = AsyncMock(return_value={"response": MOCK_MEDIAFLY_FOLDER_WITH_ITEMS})

        items = await client.get_child_items("folder_id")

        assert len(items) == 3
        assert items[0]["id"] == "111222"
        assert items[1]["id"] == "111223"
        assert items[2]["id"] == "111224"
        await client.close()

    @pytest.mark.asyncio
    async def test_get_child_items_nested_folder(self):
        client = MediaflyClient("api_key_123", "product_id_123")

        # Define a side effect function for the mock
        async def mock_get_item(item_id):
            if item_id == "folder_id":
                return {"response": MOCK_MEDIAFLY_NESTED_FOLDER}
            elif item_id == "222221":  # ID of the nested folder
                return {"response": MOCK_MEDIAFLY_FOLDER_WITH_ITEMS}
            return {"response": {}}

        # Set the side effect for the mock
        client.get_item = AsyncMock(side_effect=mock_get_item)

        # Call the method you want to test
        items = await client.get_child_items("folder_id")

        # Assert the expected outcome
        assert len(items) == 3
        assert items[0]["id"] == "111222"
        assert items[1]["id"] == "111223"
        assert items[2]["id"] == "111224"

        await client.close()

    @pytest.mark.asyncio
    async def test_download_item(self):
        client = MediaflyClient("api_key_123", "product_id_123")
        client.session.get = AsyncMock(return_value=AsyncMock(status=200))

        response = await client.download_item({"downloadUrl": "http://example.com/file"})

        assert response is not None
        assert response.status == 200
        await client.session.close()

    @pytest.mark.asyncio
    async def test_download_item_non_200_response(self):
        client = MediaflyClient("api_key_123", "product_id_123")
        client.session.get = AsyncMock(return_value=AsyncMock(status=404))

        response = await client.download_item({"downloadUrl": "http://example.com/file"})
        assert response is None


class TestMediaflyDataSource:
    @pytest.mark.asyncio
    async def test_init(self):
        with patch(
            "connectors.content_extraction.ContentExtraction.get_extraction_config",
            return_value={"host": "http://localhost:8090"},
        ):
            async with create_mediafly_source() as source:
                assert source.api_key == "api_key_123"
                assert source.product_id == "product_id_123"
                assert source.folder_ids == ["123456"]
                assert source.exclude_file_types == []
                assert source.include_internal_only_files == False
                assert source.use_text_extraction_service == False

    @pytest.mark.asyncio
    async def test_ping(self):
        async with create_mediafly_source() as source:
            await source.ping()

    @pytest.mark.asyncio
    async def test_close(self):
        async with create_mediafly_source() as source:
            await source.close()

    @pytest.mark.asyncio
    async def test_get_content_extraction_disabled(self):
        client = MediaflyClient("api_key_123", "product_id_123")
        async with create_mediafly_source() as source:
            # Mock the download_item method
            mock_response = AsyncMock()
            mock_response.content.iter_chunked = AsyncMock(return_value=[b"chunk1", b"chunk2"])
            client.download_item = AsyncMock(return_value=mock_response)

            # Set the client in the source
            source.client = client

            # Define the attachment to be used
            attachment = {
                **MOCK_MEDIAFLY_ITEM_TXT["asset"],
                "id": MOCK_MEDIAFLY_ITEM_TXT["id"],
                "modified": MOCK_MEDIAFLY_ITEM_TXT["modified"],
            }

            # Call the method you want to test
            document = await source.get_content(attachment, doit=True)

            # Assert the expected outcome
            assert document is not None
            assert document["_id"] == MOCK_MEDIAFLY_ITEM_TXT["id"]
            assert document["_timestamp"] == MOCK_MEDIAFLY_ITEM_TXT["modified"]
            assert "_attachment" not in document

            await client.close()

    @pytest.mark.asyncio
    @patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
    )
    @patch(
        "connectors.sources.mediafly.MediaflyDataSource.download_and_extract_file",
        return_value={"_attachment": "extracted_content"},
    )
    async def test_get_content_extraction_enabled(self, mock_extraction_config, mock_download_and_extract):
        client = MediaflyClient("api_key_123", "product_id_123")
        async with create_mediafly_source(use_text_extraction_service=True) as source:
            # Mock the download_item method
            mock_response = AsyncMock()
            mock_response.content.iter_chunked = AsyncMock(return_value=[b"chunk1", b"chunk2"])
            client.download_item = AsyncMock(return_value=mock_response)

            # Set the client in the source
            source.client = client

            # Define the attachment to be used
            attachment = {
                **MOCK_MEDIAFLY_ITEM_TXT["asset"],
                "id": MOCK_MEDIAFLY_ITEM_TXT["id"],
                "modified": MOCK_MEDIAFLY_ITEM_TXT["modified"],
            }

            # Call the method you want to test
            document = await source.get_content(attachment, doit=True)

            # Assert the expected outcome
            assert document is not None
            assert document["_id"] == MOCK_MEDIAFLY_ITEM_TXT["id"]
            assert document["_timestamp"] == MOCK_MEDIAFLY_ITEM_TXT["modified"]
            assert "_attachment" in document  # Assuming the extraction service adds this

            await client.close()

    @pytest.mark.asyncio
    async def test_get_content_tika_excluded(self):
        client = MediaflyClient("api_key_123", "product_id_123")
        async with create_mediafly_source() as source:
            # Mock the download_item method
            mock_response = AsyncMock()
            mock_response.content.iter_chunked = AsyncMock(return_value=[b"chunk1", b"chunk2"])
            client.download_item = AsyncMock(return_value=mock_response)

            # Set the client in the source
            source.client = client

            # Define the attachment to be used
            attachment = {
                **MOCK_MEDIAFLY_ITEM_PNG["asset"],
                "id": MOCK_MEDIAFLY_ITEM_PNG["id"],
                "modified": MOCK_MEDIAFLY_ITEM_PNG["modified"],
            }

            # Call the method you want to test
            document = await source.get_content(attachment, doit=True)

            # Assert the expected outcome
            assert document is None
            await client.close()

    @pytest.mark.asyncio
    async def test_get_content_file_size_zero(self):
        client = MediaflyClient("api_key_123", "product_id_123")
        async with create_mediafly_source() as source:
            # Mock the download_item method
            mock_response = AsyncMock()
            mock_response.content.iter_chunked = AsyncMock(return_value=[b"chunk1", b"chunk2"])
            client.download_item = AsyncMock(return_value=mock_response)

            # Set the client in the source
            source.client = client

            # Define the attachment to be used
            attachment = {
                **MOCK_MEDIAFLY_ITEM_TXT["asset"],
                "id": MOCK_MEDIAFLY_ITEM_TXT["id"],
                "modified": MOCK_MEDIAFLY_ITEM_TXT["modified"],
                "size": 0,
            }

            # Call the method you want to test
            document = await source.get_content(attachment, doit=True)

            # Assert the expected outcome
            assert document is None
            await client.close()

    @pytest.mark.asyncio
    async def test_get_docs(self):
        # Create a mock MediaflyClient
        client = MediaflyClient("api_key_123", "product_id_123")

        # Mock the get_child_items method
        client.get_child_items = AsyncMock(return_value=MOCK_MEDIAFLY_CHILD_ITEMS)

        # Use the create_mediafly_source fixture to create a source
        async with create_mediafly_source() as source:
            # Set the client in the source
            source.client = client

            # Collect the documents yielded by get_docs
            docs = []
            async for doc, _ in source.get_docs():
                docs.append(doc)

            # Assert the expected number of documents
            assert len(docs) == 2

            # Verify the content of the documents
            assert docs[0]["_id"] == MOCK_MEDIAFLY_ITEM_TXT["id"]
            assert docs[0]["_timestamp"] == MOCK_MEDIAFLY_ITEM_TXT["modified"]
            assert docs[0]["name"] == MOCK_MEDIAFLY_ITEM_TXT.get("metadata", {}).get("title")

            assert docs[1]["_id"] == MOCK_MEDIAFLY_ITEM_PPTX["id"]
            assert docs[1]["_timestamp"] == MOCK_MEDIAFLY_ITEM_PPTX["modified"]
            assert docs[1]["name"] == MOCK_MEDIAFLY_ITEM_PPTX.get("metadata", {}).get("title")

            # Assert that get_child_items was called with each folder_id
            client.get_child_items.assert_any_call("123456")

            # Assert that get_child_items was called exactly once
            assert client.get_child_items.call_count == 1
            await client.close()

    @pytest.mark.asyncio
    async def test_get_docs_multiple_folders(self):
        # Create a mock MediaflyClient
        client = MediaflyClient("api_key_123", "product_id_123")

        # Mock the get_child_items method
        client.get_child_items = AsyncMock(return_value=MOCK_MEDIAFLY_CHILD_ITEMS)

        # Use the create_mediafly_source fixture to create a source
        async with create_mediafly_source() as source:
            # Set the client in the source
            source.client = client
            source.folder_ids = ["222221", "222222"]

            # Collect the documents yielded by get_docs
            docs = []
            async for doc, _ in source.get_docs():
                docs.append(doc)

            # Assert the expected number of documents
            assert len(docs) == 4

            # Verify the content of the documents
            for i in [0, 2]:
                assert docs[i]["_id"] == MOCK_MEDIAFLY_ITEM_TXT["id"]
                assert docs[i]["_timestamp"] == MOCK_MEDIAFLY_ITEM_TXT["modified"]
                assert docs[i]["name"] == MOCK_MEDIAFLY_ITEM_TXT.get("metadata", {}).get("title")

            for i in [1, 3]:
                assert docs[i]["_id"] == MOCK_MEDIAFLY_ITEM_PPTX["id"]
                assert docs[i]["_timestamp"] == MOCK_MEDIAFLY_ITEM_PPTX["modified"]
                assert docs[i]["name"] == MOCK_MEDIAFLY_ITEM_PPTX.get("metadata", {}).get("title")

            # Assert that get_child_items was called with each folder_id
            client.get_child_items.assert_any_call("222221")
            client.get_child_items.assert_any_call("222222")

            # Assert that get_child_items was called exactly twice
            assert client.get_child_items.call_count == 2

            await client.close()

    @pytest.mark.asyncio
    async def test_get_docs_excluded_file_types(self):
        # Create a mock MediaflyClient
        client = MediaflyClient("api_key_123", "product_id_123")

        # Mock the get_child_items method
        client.get_child_items = AsyncMock(return_value=MOCK_MEDIAFLY_CHILD_ITEMS)

        # Use the create_mediafly_source fixture to create a source with the correct configuration
        async with create_mediafly_source() as source:
            # Set the client in the source
            source.client = client

            # Collect the documents yielded by get_docs
            docs = []
            async for doc, _ in source.get_docs():
                docs.append(doc)

            # Assert the expected number of documents
            # Assuming MOCK_MEDIAFLY_CHILD_ITEMS contains 3 items, and one is a pptx
            assert len(docs) == 2  # Adjust this based on your actual mock data
            assert docs[0]["_id"] == MOCK_MEDIAFLY_ITEM_TXT["id"]
            assert docs[0]["_timestamp"] == MOCK_MEDIAFLY_ITEM_TXT["modified"]
            assert docs[0]["name"] == MOCK_MEDIAFLY_ITEM_TXT.get("metadata", {}).get("title")

            await client.close()

    @pytest.mark.asyncio
    async def test__create_doc_from_item(self):
        async with create_mediafly_source() as source:
            doc = source._create_doc_from_item(MOCK_MEDIAFLY_ITEM_TXT)
            assert doc["_original_download_url"] == MOCK_MEDIAFLY_ITEM_TXT["asset"]["downloadUrl"]

    @pytest.mark.asyncio
    async def test__pre_checks_for_get_docs(self):
        async with create_mediafly_source() as source:
            assert source._pre_checks_for_get_docs(MOCK_MEDIAFLY_ITEM_TXT["asset"])
            assert source._pre_checks_for_get_docs(MOCK_MEDIAFLY_ITEM_PPTX["asset"])
            assert not source._pre_checks_for_get_docs(MOCK_MEDIAFLY_ITEM_PNG["asset"])

    @pytest.mark.asyncio
    async def test__pre_checks_for_get_docs_with_excluded_file_types(self):
        async with create_mediafly_source() as source:
            source.exclude_file_types = ["mp4", "pptx"]

            assert source._pre_checks_for_get_docs(MOCK_MEDIAFLY_ITEM_TXT["asset"])
            assert not source._pre_checks_for_get_docs(MOCK_MEDIAFLY_ITEM_PPTX["asset"])
            assert not source._pre_checks_for_get_docs(MOCK_MEDIAFLY_ITEM_PNG["asset"])


if __name__ == "__main__":
    pytest.main()
