import pytest
from unittest.mock import AsyncMock, patch
from aioresponses import aioresponses

from connectors.sources.mediafly import MediaflyClient, MediaflyDataSource
from connectors.source import DataSourceConfiguration
from contextlib import asynccontextmanager
from tests.sources.support import create_source

@pytest.fixture
def mock_mediafly_client():
    with patch("connectors.sources.mediafly.MediaflyClient") as mock:
        yield mock

@asynccontextmanager
async def create_mediafly_source(use_text_extraction_service=False):
    async with create_source(
        MediaflyDataSource,
        api_key="api_key",
        product_id="product_id",
        folder_ids=["folder1", "folder2"],
        exclude_file_types=["mp4", "mp3"],
        include_internal_only_files=False,
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source

class TestMediaflyClient:
    @pytest.mark.asyncio
    async def test_init(self):
        client = MediaflyClient("api_key", "product_id")
        assert client.api_key == "api_key"
        assert client.product_id == "product_id"

    @pytest.mark.asyncio
    async def test_close(self):
        client = MediaflyClient("api_key", "product_id")
        await client.close()

    @pytest.mark.asyncio
    async def test_get_item(self):
        with aioresponses() as m:
            client = MediaflyClient("api_key", "product_id")
            url = f"{client.base_url}/items/item_id"
            # Include the query parameter in the URL pattern
            m.get(f"{url}?productId=product_id", payload={"response": {}})
            
            item = await client.get_item("item_id")
            assert item == {"response": {}}
            await client.session.close()

    @pytest.mark.asyncio
    async def test_get_child_items(self):
        client = MediaflyClient("api_key", "product_id")
        client.get_item = AsyncMock(return_value={"response": {"type": "folder", "items": [{"id": "1", "type": "file"}]}})

        items = await client.get_child_items("folder_id")

        assert len(items) == 1
        assert items[0]["id"] == "1"
        await client.session.close()

    @pytest.mark.asyncio
    async def test_download_item(self):
        client = MediaflyClient("api_key", "product_id")
        client.session.get = AsyncMock(return_value=AsyncMock(status=200))

        response = await client.download_item({"downloadUrl": "http://example.com/file"})

        assert response is not None
        assert response.status == 200
        await client.session.close()

class TestMediaflyDataSource:
    @pytest.mark.asyncio
    async def test_init(self):
        with patch(
            "connectors.content_extraction.ContentExtraction.get_extraction_config",
            return_value={"host": "http://localhost:8090"},
        ):
            async with create_mediafly_source() as source:
                assert source.api_key == "api_key"
                assert source.product_id == "product_id"
                assert source.folder_ids == ["folder1", "folder2"]
                assert source.exclude_file_types == ["mp4", "mp3"]
                assert source.include_internal_only_files == False
                assert source.use_text_extraction_service == False

    @pytest.mark.asyncio
    async def test_ping(self):
        async with create_mediafly_source() as source:
            await source.ping()

    # TODO: Add tests for get_docs
    @pytest.mark.asyncio
    async def test_get_docs(self):
        async with create_mediafly_source() as source:
            docs = []
            async for doc, _ in source.get_docs():
                docs.append(doc)
            assert len(docs) == 0

if __name__ == "__main__":
    pytest.main()

# @pytest.mark.asyncio
# @patch(
#     "connectors.content_extraction.ContentExtraction.get_extraction_config",
#     return_value={"host": "http://localhost:8090"},
# )
# async def test_ping(mock_extraction):
#     async with MediaflyDataSource(MOCK_CONFIGURATION) as source:
#         source.client.get_item = AsyncMock(return_value={"response": {"type": "folder"}})
#         await source.ping()

# @pytest.mark.asyncio
# @patch(
#     "connectors.content_extraction.ContentExtraction.get_extraction_config",
#     return_value={"host": "http://localhost:8090"},
# )
# async def test_ping_raises_exception(mock_extraction):
#     async with MediaflyDataSource(MOCK_CONFIGURATION) as source:
#         source.client.get_item = AsyncMock(side_effect=Exception("Connection failed"))
#         with pytest.raises(Exception):
#             await source.ping()

# @pytest.mark.asyncio
# @patch(
#     "connectors.content_extraction.ContentExtraction.get_extraction_config",
#     return_value={"host": "http://localhost:8090"},
# )
# async def test_get_docs(mock_extraction):
#     async with MediaflyDataSource(MOCK_CONFIGURATION) as source:
#         mock_items = [
#             {"id": "1", "name": "file1.pdf", "type": "file"},
#             {"id": "2", "name": "file2.txt", "type": "file"},
#         ]
#         source.client.get_child_items = AsyncMock(return_value=mock_items)
#         source.get_content = AsyncMock(return_value={"_id": "1", "_attachment": "content"})

#         docs = []
#         async for doc, _ in source.get_docs():
#             docs.append(doc)

#         assert len(docs) == 2
#         assert docs[0]["_id"] == "1"
#         assert docs[1]["_id"] == "2"

# @pytest.mark.asyncio
# @patch(
#     "connectors.content_extraction.ContentExtraction.get_extraction_config",
#     return_value={"host": "http://localhost:8090"},
# )
# async def test_get_content(mock_extraction):
#     with patch(
#         "connectors.content_extraction.ContentExtraction.extract_text",
#         return_value="Mocked extracted text"
#     ):
#         async with MediaflyDataSource(MOCK_CONFIGURATION) as source:
#             attachment = {
#                 "id": "1",
#                 "filename": "test.pdf",
#                 "size": 1000,
#                 "downloadUrl": "http://example.com/test.pdf",
#             }
#             source.client.download_item = AsyncMock(return_value=AsyncMock(content=AsyncIterator([b"content"])))

#             content = await source.get_content(attachment, doit=True)

#             assert content["_id"] == "1"
#             assert content["_attachment"] == "Y29udGVudA=="  # base64 encoded "content"

# @pytest.mark.asyncio
# @patch(
#     "connectors.content_extraction.ContentExtraction.get_extraction_config",
#     return_value={"host": "http://localhost:8090"},
# )
# async def test_get_content_unsupported_file(mock_extraction):
#     async with MediaflyDataSource(MOCK_CONFIGURATION) as source:
#         attachment = {
#             "id": "1",
#             "filename": "test.mp4",
#             "size": 1000,
#             "downloadUrl": "http://example.com/test.mp4",
#         }

#         content = await source.get_content(attachment, doit=True)

#         assert content is None
