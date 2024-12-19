#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio
import json
import struct
import uuid
from functools import partial
from typing import AsyncGenerator, Optional

import ldap
from smbprotocol.connection import Connection
from smbprotocol.exceptions import SMBException, SMBOSError
from smbprotocol.open import (
    CreateDisposition,
    CreateOptions,
    DirectoryAccessMask,
    FileAttributes,
    FileInformationClass,
    FilePipePrinterAccessMask,
    ImpersonationLevel,
    InfoType,
    Open,
    ShareAccess,
    SMB2QueryInfoRequest,
    SMB2QueryInfoResponse,
)
from smbprotocol.security_descriptor import SMB2CreateSDBuffer
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from winrm import Session as WinRMSession

from connectors.access_control import ACCESS_CONTROL, es_access_control_query
from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import RetryStrategy, iso_utc, retryable

CHUNK_SIZE = 1024 * 1024  # 1MB
SECURITY_INFO_DACL = 0x00000004  # used when fetching our security permssions

# either of these masks indicate a user has sufficient read permissions to read file contents
FILE_READ_DATA = 0x0001
GENERIC_READ = 0x80000000

# these domain ids indicate that an account is a local and not a domain account
LOCAL_DOMAIN_IDS = [32, 11, 18]


INVALID_SIDS = ["S-1-5-11", "S-1-5-18"]

SUPPORTED_FILE_TYPES = [
    "txt",
    "py",
    "rst",
    "html",
    "markdown",
    "json",
    "xml",
    "csv",
    "md",
    "ppt",
    "rtf",
    "docx",
    "odt",
    "xls",
    "xlsx",
    "rb",
    "paper",
    "sh",
    "pptx",
    "pdf",
    "doc",
]


def has_read_access(ace_mask: int) -> bool:
    """Check if an ACE mask includes basic read permissions"""
    return bool(
        (ace_mask & FILE_READ_DATA)  # Can read file contents
        or (ace_mask & GENERIC_READ)  # Has generic read access
    )


def is_directory(file_attributes: FileAttributes) -> bool:
    """
    Check if the file attributes indicate a directory
    this uses a bitwise operation to check the FILE_ATTRIBUTE_DIRECTORY bit
    any value other than 0 means the object is a directory
    """
    return bool(file_attributes & FileAttributes.FILE_ATTRIBUTE_DIRECTORY)


def parse_sid(sid: str) -> dict:
    """Parse a Windows Security Identifier (SID) into its constituent parts.

    A SID has the format: S-1-5-21-xxxxxxxxxx-xxxxxxxxxx-xxxxxxxxxx-xxxx
    Where:
    - S identifies this as a SID
    - 1 is the revision level
    - 5 is the identifier authority value
    - 21 is the domain identifier
    - The following numbers are the domain identifiers
    - The final number is the relative identifier (RID)

    Args:
        sid: The SID string to parse

    Returns:
        dict: Contains the parsed components:
            - revision: SID revision number
            - identifier_authority: Identifier authority value
            - domain_identifiers: List of domain identifier values
            - rid: Relative identifier value
    """
    parts = sid.split("-")

    if not sid.startswith("S-") or len(parts) < 3:
        msg = f"Invalid SID format: {sid}"
        raise ValueError(msg)

    try:
        return {
            "revision": int(parts[1]),
            "identifier_authority": int(parts[2]),
            "domain_identifier": int(parts[3]),
            "domain_identifiers": [int(x) for x in parts[4:-1]],
            "rid": int(parts[-1]),
        }
    except (ValueError, IndexError) as e:
        msg = f"Failed to parse SID {sid}"
        raise ValueError(msg) from e


def convert_sid(binary):
    """
    converts the binary SID value to a string
    no actual idea how this works and using the answer from https://stackoverflow.com/questions/33188413/python-code-to-convert-from-objectsid-to-sid-representation
    """
    version = struct.unpack("B", binary[0:1])[0]
    length = struct.unpack("B", binary[1:2])[0]
    authority = struct.unpack(b">Q", b"\x00\x00" + binary[2:8])[0]
    string = "S-%d-%d" % (version, authority)
    binary = binary[8:]
    for i in range(length):
        value = struct.unpack("<L", binary[4 * i : 4 * (i + 1)])[0]
        string += "-%d" % value
    return string


class LocalAccountClient:
    def __init__(self, host: str, port: int, username: str, password: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._session = None

    async def connect(self):
        """Establish WinRM connection"""
        if self._session is not None:
            return

        try:
            self._session = WinRMSession(
                f"http://{self.host}:{self.port}/wsman",
                auth=(self.username, self.password),
                transport="ntlm",
            )
        except Exception as e:
            logger.error(f"Failed to connect to WinRM server: {e}")
            raise

    async def disconnect(self):
        """Close WinRM connection"""
        self._session = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    async def run_ps(self, script: str) -> dict | list | None:
        """Run a PowerShell script and return the result"""
        try:
            await self.connect()
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, lambda: self._session.run_ps(script)
            )
            return result
        except Exception as e:
            logger.error(f"Error running PowerShell script: {e}")
            raise e

    async def get_group_members(self, sid: str) -> list:
        """Get members of a local group using PowerShell"""
        try:
            script = (
                f"get-localgroup -SID {sid} | get-localgroupmember | ConvertTo-Json"
            )

            result = await self.run_ps(script)

            if result.status_code != 0:
                logger.error(
                    f"Failed to get group members for SID {sid}: {result.std_err.decode()}"
                )
                return []

            return json.loads(result.std_out.decode())

        except Exception as e:
            logger.error(f"Error getting group members for SID {sid}: {e}")
            return []

    async def resolve_local_sid(self, sid: str) -> list:
        """Recursively resolve a local SID to its final domain users/groups"""
        try:
            members = await self.get_group_members(sid)
            resolved = []

            while len(members) > 0:
                member = members.pop(0)
                principal_source = member.get("PrincipalSource")
                object_class = member.get("ObjectClass")

                if principal_source == 2:  # Domain account/group
                    resolved.append(member)
                elif principal_source == 0:
                    continue
                elif object_class == "Group":
                    members.append(member)

            return resolved

        except Exception as e:
            logger.error(f"Error resolving local SID {sid}: {e}")
            return []

    async def ping(self) -> bool:
        """Test if the WinRM connection is alive and working.

        Returns:
            bool: True if connection is working, False otherwise
        """
        try:
            # Run a simple PowerShell command to test connection
            script = "$env:COMPUTERNAME"  # Simple command to get computer name
            result = await self.run_ps(script)
            return result.status_code == 0

        except Exception as e:
            logger.debug(f"WinRM connection test failed: {e}")
            return False


class LDAPClient:
    def __init__(
        self,
        ldap_server: str,
        ldap_port: int,
        ldap_domain: str,
        ldap_username: str,
        ldap_password: str,
    ):
        self.ldap_server = ldap_server
        self.ldap_port = ldap_port
        self.ldap_domain = ldap_domain
        self.ldap_username = ldap_username
        self.ldap_password = ldap_password
        self._conn = None

    async def connect(self):
        """Establish connection to LDAP server asynchronously"""
        if self._conn is not None:
            return

        # Create LDAP URL
        ldap_url = f"ldap://{self.ldap_server}:{self.ldap_port}"

        try:
            loop = asyncio.get_event_loop()
            # Initialize connection
            self._conn = await loop.run_in_executor(None, ldap.initialize, ldap_url)

            # Set connection options
            await loop.run_in_executor(
                None, self._conn.set_option, ldap.OPT_REFERRALS, 0
            )
            await loop.run_in_executor(
                None, self._conn.set_option, ldap.OPT_PROTOCOL_VERSION, 3
            )

            # Bind with credentials
            await loop.run_in_executor(
                None, self._conn.simple_bind_s, self.ldap_username, self.ldap_password
            )
        except ldap.LDAPError as e:
            logger.error(f"Failed to connect to LDAP server: {e}")
            raise

    async def disconnect(self):
        """Close LDAP connection asynchronously"""
        if self._conn:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._conn.unbind_s)
            self._conn = None

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()

    def _get_domain_dn(self) -> str:
        """Convert domain name to Distinguished Name format

        Example: domain.com -> DC=domain,DC=com
        """
        return ",".join([f"DC={dc}" for dc in self.ldap_domain.split(".")])

    async def get_group_members(self, group_dn: str) -> list:
        """Recursively get all user members of a group

        Args:
            group_dn: Distinguished name of the group

        Returns:
            list: List of user account information dictionaries
        """
        try:
            # Search for all members of the group
            attrs = ["member", "memberOf"]
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None,
                self._conn.search_s,
                group_dn,
                ldap.SCOPE_BASE,
                "(objectClass=*)",
                attrs,
            )

            if not results:
                return []

            _, attributes = results[0]
            all_users = []

            # If there are no members, return empty list
            if "member" not in attributes:
                return []

            # Process each member
            for member_dn in attributes["member"]:
                member_dn = member_dn.decode("utf-8")

                # Get member details
                member_results = await loop.run_in_executor(
                    None,
                    self._conn.search_s,
                    member_dn,
                    ldap.SCOPE_BASE,
                    "(objectClass=*)",
                    [
                        "objectClass",
                        "sAMAccountName",
                        "mail",
                        "userPrincipalName",
                        "objectSid",
                    ],
                )

                if not member_results:
                    continue

                _, member_attrs = member_results[0]
                object_classes = [
                    oc.decode("utf-8").lower() for oc in member_attrs["objectClass"]
                ]

                # If member is a user, add to results
                if "user" in object_classes:
                    sid = convert_sid(member_attrs["objectSid"][0])
                    user_info = {
                        "name": member_attrs["sAMAccountName"][0].decode("utf-8"),
                        "distinguished_name": member_dn,
                        "type": "user",
                        "email": (
                            member_attrs["mail"][0].decode("utf-8")
                            if "mail" in member_attrs
                            else None
                        ),
                        "upn": (
                            member_attrs["userPrincipalName"][0].decode("utf-8")
                            if "userPrincipalName" in member_attrs
                            else None
                        ),
                        # sids are weird and need to converted differently than the other attrs
                        "sid": sid,
                    }
                    all_users.append(user_info)

                # If member is a group, recursively get its members
                elif "group" in object_classes:
                    nested_users = await self.get_group_members(member_dn)
                    all_users.extend(nested_users)

            return all_users

        except ldap.LDAPError as e:
            logger.error(f"Error getting group members: {e}")
            return []

    async def sid_to_accounts(self, sid: str) -> list:
        """Convert a SID to user information, resolving group memberships

        Args:
            sid: The security identifier string

        Returns:
            list: List of user account information dictionaries
        """
        try:
            await self.connect()

            # Create LDAP filter to search by SID
            search_filter = f"(objectSid={sid})"

            # Define attributes to retrieve
            attrs = [
                "sAMAccountName",
                "distinguishedName",
                "objectClass",
                "mail",
                "userPrincipalName",
                "objectSid",
            ]

            # Search in the entire domain
            base_dn = self._get_domain_dn()

            # Perform the search asynchronously
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None,
                self._conn.search_s,
                base_dn,
                ldap.SCOPE_SUBTREE,
                search_filter,
                attrs,
            )

            if not results:
                logger.warning(f"No results found for SID: {sid}")
                return []

            dn, attributes = results[0]

            # this try/except handles some odd cases where
            # attributes is a list of tuples instead of a dict, these are not users anyway
            if not isinstance(attributes, dict):
                object_classes = []
            else:
                object_classes = [
                    oc.decode("utf-8").lower() for oc in attributes["objectClass"]
                ]

            # If this is a user, return user info
            if "user" in object_classes:
                return [
                    {
                        "name": attributes["sAMAccountName"][0].decode("utf-8"),
                        "distinguished_name": dn,
                        "type": "user",
                        "email": (
                            attributes["mail"][0].decode("utf-8")
                            if "mail" in attributes
                            else None
                        ),
                        "upn": (
                            attributes["userPrincipalName"][0].decode("utf-8")
                            if "userPrincipalName" in attributes
                            else None
                        ),
                        "sid": str(attributes["objectSid"][0]),
                    }
                ]

            # If this is a group, get all nested members
            elif "group" in object_classes:
                return await self.get_group_members(dn)

            return []

        except ldap.LDAPError as e:
            logger.error(f"Error resolving SID {sid}: {e}")
            return []

    async def ping(self) -> bool:
        """Test if the LDAP connection is alive and working.

        Returns:
            bool: True if connection is working, False otherwise
        """
        try:
            await self.connect()

            # Simple search to validate connection
            base_dn = self._get_domain_dn()
            loop = asyncio.get_event_loop()

            # Search for the root domain object
            await loop.run_in_executor(
                None,
                self._conn.search_s,
                base_dn,
                ldap.SCOPE_BASE,
                "(objectClass=*)",
                ["dc"],
            )

            return True

        except ldap.LDAPError as e:
            logger.debug(f"LDAP connection test failed: {e}")
            return False
        finally:
            await self.disconnect()

    async def get_all_domain_users(self) -> list:
        """Get all user accounts in the domain with their UPN and SID.

        Returns:
            list: List of dictionaries containing user information:
                - name: sAMAccountName
                - upn: userPrincipalName
                - sid: objectSid
                - email: mail attribute (if available)
        """
        try:
            await self.connect()
            base_dn = self._get_domain_dn()

            # LDAP filter to get only user objects
            search_filter = "(&(objectClass=user)(objectCategory=person))"

            # Attributes we want to retrieve
            attrs = ["sAMAccountName", "userPrincipalName", "objectSid", "mail"]

            # Set page size for pagination
            page_size = 1000
            loop = asyncio.get_event_loop()

            # Enable paged results
            ldap_control = ldap.controls.SimplePagedResultsControl(
                True, size=page_size, cookie=""
            )

            users = []
            has_more = True
            while has_more:
                # Perform the search with paging
                msgid = self._conn.search_ext(
                    base_dn,
                    ldap.SCOPE_SUBTREE,
                    search_filter,
                    attrs,
                    serverctrls=[ldap_control],
                )

                try:
                    rtype, rdata, rmsgid, serverctrls = await loop.run_in_executor(
                        None, self._conn.result3, msgid
                    )
                except ldap.SIZELIMIT_EXCEEDED as e:
                    logger.warning(
                        f"Size limit exceeded, but continuing with partial results: {e}"
                    )
                    break

                # Process the page of results
                for _, attributes in rdata:
                    # Skip entries without required attributes
                    if not isinstance(attributes, dict) or not all(
                        attr in attributes
                        for attr in ["sAMAccountName", "userPrincipalName", "objectSid"]
                    ):
                        continue

                    try:
                        user = {
                            "name": attributes["sAMAccountName"][0].decode("utf-8"),
                            "upn": attributes["userPrincipalName"][0].decode("utf-8"),
                            "sid": convert_sid(attributes["objectSid"][0]),
                            "email": (
                                attributes["mail"][0].decode("utf-8")
                                if "mail" in attributes
                                else None
                            ),
                        }
                        users.append(user)
                    except Exception as e:
                        logger.warning(f"Error processing user attributes: {e}")
                        continue

                # Get cookie from the page control
                page_controls = [
                    c
                    for c in serverctrls
                    if c.controlType
                    == ldap.controls.SimplePagedResultsControl.controlType
                ]
                if not page_controls:
                    has_more = False
                else:
                    cookie = page_controls[0].cookie
                    if not cookie:
                        has_more = False
                    else:
                        ldap_control.cookie = cookie

            return users

        except ldap.LDAPError as e:
            logger.error(f"Error getting domain users: {e}")
            return []
        finally:
            await self.disconnect()


class SMBClient:
    def __init__(self, host: str, port: int, username: str, password: str) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection = Connection(uuid.uuid4(), self.host, self.port)
        self.connection.connect()
        self.session = Session(self.connection, self.username, self.password)
        self.session.connect()

    def _get_share(self, share_name: str) -> TreeConnect:
        """
        Get a share from the SMB server
        Also handles formatting the name of the share and host into a smb share path
        currently returns a TreeConnect object
        """
        share = rf"\\{self.host}\{share_name}"
        return TreeConnect(self.session, share)

    def close_connection(self):
        try:
            self.session.disconnect()
            self.connection.disconnect()
        except Exception as e:
            logger.error(f"Error closing SMB connection: {e}")

    async def list_files(
        self, share_name: str, path: str = None, base_path: Optional[str] = None
    ) -> AsyncGenerator[dict, None]:
        """
        Recursively lists all files in a given path on the share
        Yields dicts containing file information including full path
        """
        if base_path is None:
            base_path = path

        try:
            share = self._get_share(share_name)
            share.connect()
            path = path.lstrip("\\")
            path = path.lstrip(" ")
            dir_open = Open(share, path)

            try:
                dir_open.create(
                    ImpersonationLevel.Impersonation,
                    DirectoryAccessMask.FILE_LIST_DIRECTORY,
                    None,
                    ShareAccess.FILE_SHARE_READ,
                    CreateDisposition.FILE_OPEN,
                    CreateOptions.FILE_DIRECTORY_FILE,
                )
            except SMBOSError as e:
                logger.warning(f"Access denied or error opening directory {path}: {e}")
                return

            try:
                files = dir_open.query_directory(
                    "*",
                    FileInformationClass.FILE_ID_BOTH_DIRECTORY_INFORMATION,
                )
            except SMBOSError as e:
                logger.warning(f"Error listing contents of directory {path}: {e}")
                return
            finally:
                dir_open.close()

            for file in files:
                try:
                    file_name = file["file_name"].get_value().decode("utf-16-le")
                    # Skip . and .. directory entries
                    if file_name in [".", ".."]:
                        continue

                    file_attributes = file["file_attributes"].get_value()
                    relative_path = "\\".join(
                        filter(None, [path[len(base_path) :].strip("\\"), file_name])
                    )

                    file_info = {
                        "name": file_name,
                        "relative_path": relative_path,
                        "full_path": f"{path}\\{file_name}",
                        "is_directory": is_directory(file_attributes),
                        "attributes": file_attributes,
                        "file_id": file["file_id"].get_value(),
                        "created_at": iso_utc(file["creation_time"].get_value()),
                        "modified_at": iso_utc(file["change_time"].get_value()),
                        "type": file_name.split(".")[-1],
                        "size": file["end_of_file"].get_value(),
                    }

                    if is_directory(file_attributes):
                        # Recursively get files from subdirectory
                        try:
                            subdir_path = f"{path}\\{file_name}"
                            async for subdir_file in self.list_files(
                                share_name, subdir_path, base_path
                            ):
                                yield subdir_file
                        except Exception as e:
                            logger.warning(
                                f"Error accessing subdirectory {subdir_path}: {e}"
                            )
                            continue

                    if file_info["type"] in SUPPORTED_FILE_TYPES:
                        yield file_info

                except Exception as e:
                    logger.warning(f"Error processing file entry in {path}: {e}")
                    continue

        except Exception as e:
            logger.warning(f"Error accessing path {path}: {e}")
            return

    @retryable(
        retries=3,
        interval=2,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[SMBOSError, SMBException],
    )
    async def _get_file_permissions(self, file_open: Open) -> dict:
        """Get file permissions from an open file handle"""
        try:
            allowed_sids = []
            query_request = SMB2QueryInfoRequest()
            query_request["info_type"] = InfoType.SMB2_0_INFO_SECURITY
            query_request["output_buffer_length"] = 65535
            query_request["additional_information"] = SECURITY_INFO_DACL
            query_request["file_id"] = file_open.file_id

            req = file_open.connection.send(
                query_request,
                file_open.tree_connect.session.session_id,
                file_open.tree_connect.tree_connect_id,
            )

            response = file_open.connection.receive(req)
            query_response = SMB2QueryInfoResponse()
            query_response.unpack(response["data"].get_value())
            security_descriptor = SMB2CreateSDBuffer()
            security_descriptor.unpack(query_response["buffer"].get_value())

            aces = security_descriptor.get_dacl()["aces"]

            denied_sids = [
                str(ace["sid"]) for ace in aces if ace["ace_type"].value == 1
            ]
            # If the SID is present in any deny ACE, it is not allowed to read the file
            for ace in aces:
                if str(ace["sid"]) not in denied_sids and has_read_access(
                    ace["mask"].value
                ):
                    allowed_sids.append(str(ace["sid"]))
            allowed_sids = list(set(allowed_sids))  # remove duplicates
            return allowed_sids
        except Exception as e:
            logger.warning(f"Error getting file permissions: {e}")
            return []

    @retryable(
        retries=3,
        interval=2,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[SMBOSError, SMBException],
    )
    async def _read_file_content(
        self, share, file_path, get_permissions: bool = False
    ) -> tuple[bytes, int, dict | None]:
        """Read file content and optionally get file descriptor information

        Args:
            share: TreeConnect object for the SMB share
            file_path: Path to the file to read
            get_permissions: Whether to query for file permissions

        Returns:
            tuple[bytes, int, dict | None]: Tuple of (file content, file size, file descriptor if requested)
        """
        file_open = Open(share, file_path)
        try:
            logger.debug(f"Opening file {file_path}")
            file_open.create(
                ImpersonationLevel.Impersonation,
                FilePipePrinterAccessMask.FILE_READ_DATA
                | FilePipePrinterAccessMask.FILE_READ_ATTRIBUTES,
                FileAttributes.FILE_ATTRIBUTE_NORMAL,
                ShareAccess.FILE_SHARE_READ,
                CreateDisposition.FILE_OPEN,
                CreateOptions.FILE_NON_DIRECTORY_FILE
                | CreateOptions.FILE_SEQUENTIAL_ONLY,
            )

            offset = 0
            file_size = file_open.end_of_file
            all_content = bytearray()

            logger.debug(f"getting content for {file_path} of length {file_size}")
            while offset < file_size:
                try:
                    chunk_size = min(CHUNK_SIZE, file_size - offset)
                    chunk = file_open.read(offset, chunk_size)
                    all_content.extend(chunk)
                    offset += chunk_size
                except SMBOSError as e:
                    logger.warning(
                        f"SMB error while reading {file_path}, skipping: {e}"
                    )
                    return None

            logger.debug(f"retrieved {len(all_content)} bytes")

            return {
                "body": bytes(all_content),
                "size": file_size,
            }
        except SMBOSError as e:
            logger.warning(f"SMB error while opening {file_path}, skipping: {e}")
            return None
        finally:
            file_open.close()

    @retryable(
        retries=2,
        interval=30,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[SMBOSError, SMBException],
    )
    async def get_files_content(
        self, share_name: str, path: str, get_permissions: bool = False
    ) -> AsyncGenerator[dict, None]:
        """
        Lists all files in the given path and yields their contents
        Yields dicts containing file information and content
        Skips directories as they don't have content to read
        """
        async for file_info in self.list_files(share_name, path):
            if file_info["is_directory"]:
                continue

            share = self._get_share(share_name)
            share.connect()

            content, file_size, descriptor = await self._read_file_content(
                share, file_info["full_path"], get_permissions
            )
            yield {
                "path": rf"\\{self.host}\{share_name}\{file_info['full_path']}",
                "body": content,
                "title": file_info["name"],
                "type": file_info["name"].split(".")[-1],
                "size": file_size,
                "_id": file_info["file_id"],
                "created_at": file_info["created_at"],
                "modified_at": file_info["modified_at"],
                **({"file_descriptor": descriptor} if descriptor else {}),
            }

    async def get_all_sids_in_path(self, share_name: str, path: str) -> list[str]:
        """Collect all unique SIDs that appear in any file's ACL in the given path"""
        all_sids = set()

        async for file_info in self.list_files(share_name, path):
            if file_info["is_directory"]:
                continue
            if file_info["full_path"] == "" or file_info["full_path"] == "\\":
                continue
            try:
                share = self._get_share(share_name)
                share.connect()
                file_open = Open(share, file_info["full_path"].lstrip("\\").lstrip(" "))
                try:
                    file_open.create(
                        ImpersonationLevel.Impersonation,
                        FilePipePrinterAccessMask.FILE_READ_ATTRIBUTES
                        | FilePipePrinterAccessMask.READ_CONTROL,  # Need READ_CONTROL for ACLs
                        FileAttributes.FILE_ATTRIBUTE_NORMAL,
                        ShareAccess.FILE_SHARE_READ,  # Only need read sharing for read-only access
                        CreateDisposition.FILE_OPEN,
                        CreateOptions.FILE_NON_DIRECTORY_FILE,
                    )
                    allowed_sids = await self._get_file_permissions(file_open)
                    all_sids.update(allowed_sids)
                except SMBOSError as e:
                    logger.warning(
                        f"Access denied or error getting permissions for {file_info['full_path']}: {e}"
                    )
                    continue
                finally:
                    file_open.close()

            except Exception as e:
                logger.warning(
                    f"Error accessing file for permissions {file_info['full_path']}: {e}"
                )
                continue

        return list(all_sids)

    async def ping(self, share_name: str) -> bool:
        """Test if the SMB connection is alive and working.

        Returns:
            bool: True if connection is working, False otherwise
        """
        try:
            # Try to list files in the root of any share to validate connection
            share = self._get_share(share_name)
            share.connect()

            # Create a directory open operation
            dir_open = Open(share, "")
            dir_open.create(
                ImpersonationLevel.Impersonation,
                DirectoryAccessMask.FILE_LIST_DIRECTORY,
                None,
                ShareAccess.FILE_SHARE_READ,
                CreateDisposition.FILE_OPEN,
                CreateOptions.FILE_DIRECTORY_FILE,
            )

            # Close the directory handle
            dir_open.close()
            return True

        except Exception as e:
            logger.debug(f"SMB connection test failed: {e}")
            return False

    @retryable(
        retries=3,
        interval=2,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=[SMBOSError, SMBException],
    )
    async def get_file_permissions(self, share_name: str, file_info: dict) -> list[str]:
        """Get file permissions from a file info object

        Args:
            share_name: Name of the SMB share
            file_info: File information dictionary from list_files

        Returns:
            list[str]: List of SIDs with read access
        """
        share = self._get_share(share_name)
        try:
            share.connect()
            file_open = Open(share, file_info["full_path"].lstrip("\\").lstrip(" "))
            try:
                file_open.create(
                    ImpersonationLevel.Impersonation,
                    FilePipePrinterAccessMask.FILE_READ_ATTRIBUTES
                    | FilePipePrinterAccessMask.READ_CONTROL,  # Need READ_CONTROL for ACLs
                    FileAttributes.FILE_ATTRIBUTE_NORMAL,
                    ShareAccess.FILE_SHARE_READ,  # Only need read sharing for read-only access
                    CreateDisposition.FILE_OPEN,
                    CreateOptions.FILE_NON_DIRECTORY_FILE,
                )

                allowed_sids = await self._get_file_permissions(file_open)
                return allowed_sids

            except SMBOSError as e:
                logger.warning(
                    f"Access denied or error getting permissions for {file_info['full_path']}: {e}"
                )
                return []
            finally:
                file_open.close()

        except Exception as e:
            logger.warning(
                f"Error accessing file for permissions {file_info['full_path']}: {e}"
            )
            return []

    async def get_file_content(
        self, share_name: str, file_path: str
    ) -> AsyncGenerator[bytes, None]:
        """Get content of a file from SMB share"""
        share = self._get_share(share_name)

        try:
            share.connect()
            file_open = Open(share, file_path.lstrip("\\").lstrip(" "))

            try:
                file_open.create(
                    ImpersonationLevel.Impersonation,
                    FilePipePrinterAccessMask.FILE_READ_DATA
                    | FilePipePrinterAccessMask.FILE_READ_ATTRIBUTES,
                    FileAttributes.FILE_ATTRIBUTE_NORMAL,
                    ShareAccess.FILE_SHARE_READ,
                    CreateDisposition.FILE_OPEN,
                    CreateOptions.FILE_NON_DIRECTORY_FILE
                    | CreateOptions.FILE_SEQUENTIAL_ONLY,
                )
            except SMBOSError as e:
                logger.warning(f"Access denied or error opening file {file_path}: {e}")
                return

            try:
                offset = 0
                file_size = file_open.end_of_file

                logger.debug(f"getting content for {file_path} of length {file_size}")
                while offset < file_size:
                    try:
                        chunk_size = min(CHUNK_SIZE, file_size - offset)
                        chunk = file_open.read(offset, chunk_size)
                        yield chunk
                        offset += chunk_size
                    except SMBOSError as e:
                        logger.warning(f"Error reading chunk from {file_path}: {e}")
                        return

                logger.debug(f"finished reading {file_path}")

            finally:
                file_open.close()

        except Exception as e:
            logger.warning(f"Error accessing file {file_path}: {e}")
            return


class SMBShareDataSource(BaseDataSource):
    """SMB Share"""

    name = "SMB Share"
    service_type = "smb_share"
    advanced_rules_enabled = False
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self.server_address = self.configuration["server_address"]
        self.port = self.configuration["server_port"]
        self.username = self.configuration["username"]
        self.password = self.configuration["password"]
        self.share_name = self.configuration["share_name"]
        self.path = self.configuration["path"]
        self.domain = self.configuration["domain"]
        self.ldap_username = self.configuration["ldap_username"]
        self.ldap_password = self.configuration["ldap_password"]
        self.ldap_server = self.configuration["ldap_server"]
        self.ldap_port = self.configuration["ldap_port"]
        self.use_document_level_security = self.configuration[
            "use_document_level_security"
        ]

        self.smb_client = SMBClient(
            self.server_address, self.port, self.username, self.password
        )
        self.local_account_client = LocalAccountClient(
            self.server_address, 5985, self.username, self.password
        )
        self.ldap_client = LDAPClient(
            self.ldap_server,
            self.ldap_port,
            self.domain,
            self.ldap_username,
            self.ldap_password,
        )
        self._sid_cache = {}
        self._domain_sid_cache = {}

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for SMB Share"""
        return {
            "server_address": {
                "label": "SMB Server Address",
                "order": 1,
                "type": "str",
            },
            "server_port": {
                "label": "SMB Server Port",
                "order": 2,
                "type": "int",
                "value": 445,
            },
            "username": {
                "label": "Username",
                "order": 3,
                "type": "str",
            },
            "password": {
                "label": "Password",
                "order": 4,
                "type": "str",
                "sensitive": True,
            },
            "share_name": {
                "label": "SMB Share Name",
                "order": 5,
                "type": "str",
                "tooltip": "This should be the first part of the SMB share path, e.g. 'scratch' in 'scratch/elastic-test'",
            },
            "path": {
                "label": "SMB Share Path",
                "order": 6,
                "type": "str",
                "tooltip": "Optional. Subfolder within the share to sync. Leave blank to sync entire share.",
                "value": "",  # Set default empty value
                "required": False,  # Make field optional
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 6,
                "tooltip": "Document level security ensures identities and permissions set in your smb share are mirrored in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
            "domain": {
                "label": "Domain",
                "order": 7,
                "type": "str",
                "depends_on": [
                    {"field": "use_document_level_security", "value": True},
                ],
                "tooltip": "Active Directory domain to use for document level security resolution",
            },
            "ldap_username": {
                "label": "LDAP Username",
                "order": 8,
                "type": "str",
                "depends_on": [
                    {"field": "use_document_level_security", "value": True},
                ],
                "tooltip": "Username to use for LDAP authentication, should be in the modern user.name@domain.com format",
            },
            "ldap_password": {
                "label": "LDAP Password",
                "order": 9,
                "type": "str",
                "depends_on": [
                    {"field": "use_document_level_security", "value": True},
                ],
                "sensitive": True,
            },
            "ldap_server": {
                "label": "LDAP Server",
                "order": 10,
                "type": "str",
                "depends_on": [
                    {"field": "use_document_level_security", "value": True},
                ],
                "tooltip": "LDAP server to use for document level security resolution",
            },
            "ldap_port": {
                "label": "LDAP Port",
                "order": 11,
                "type": "int",
                "depends_on": [
                    {"field": "use_document_level_security", "value": True},
                ],
                "tooltip": "LDAP server port to use for document level security resolution",
                "value": 389,
            },
        }

    @retryable(
        retries=3,
        interval=20,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def ping(self):
        """validates network drive, winrm, and ldap connectivity"""
        smb_status = await self.smb_client.ping(self.share_name)
        if self.use_document_level_security:
            ldap_status = await self.ldap_client.ping()
            local_account_status = await self.local_account_client.ping()
        return all([smb_status, ldap_status, local_account_status])

    async def validate_config(self):
        """basic input validation, probably need to improve this"""
        await super().validate_config()
        path = self.configuration["path"]
        if path and (path.startswith("/") or path.startswith("\\")):
            message = f"SMB Path:{path} should not start with '/' in the beginning."
            raise ConfigurableFieldValueError(message)
        username = self.configuration["username"]
        if "@" not in username:
            message = f"Username:{username} should be in the modern user.name@domain.com format"
            raise ConfigurableFieldValueError(message)
        if self.use_document_level_security:
            ldap_username = self.configuration["ldap_username"]
            if "@" not in ldap_username:
                message = f"LDAP Username:{ldap_username} should be in the modern user.name@domain.com format"
                raise ConfigurableFieldValueError(message)

    async def close(self):
        """close the smb client and clear the sid resolver caches"""
        self.smb_client.close_connection()
        self._sid_cache = {}
        self._domain_sid_cache = {}

    def _dls_enabled(self):
        if (
            self._features is None
            or not self._features.document_level_security_enabled()
        ):
            return False

        return self.configuration["use_document_level_security"]

    async def _resolve_local_sid(self, sid: str) -> tuple:
        """Cached helper method to resolve local SIDs to domain SIDs"""
        self._logger.debug(f"resolving local sid - {sid}")
        if sid not in self._sid_cache:
            async with self.local_account_client as client:
                resolved = await client.resolve_local_sid(sid)
                self._sid_cache[sid] = tuple(x["SID"]["Value"] for x in resolved)
        return self._sid_cache[sid]

    async def resolve_document_sids(self, file_info: dict) -> list:
        """Resolves SIDs on a file into domain user account SIDs

        Args:
            file_info: File information dictionary from list_files

        Returns:
            list: List of resolved domain user SIDs
        """
        sids = await self.smb_client.get_file_permissions(self.share_name, file_info)

        domain_sids = set()
        domain_user_accounts = set()
        self._logger.debug(f"resolving sids - {sids}")

        for sid in sids:
            parsed_sid = parse_sid(sid)
            if parsed_sid["domain_identifier"] in LOCAL_DOMAIN_IDS:
                if sid in INVALID_SIDS:
                    continue
                resolved_sids = await self._resolve_local_sid(sid)
                domain_sids.update(resolved_sids)
            else:
                self._logger.debug(f"domain sid - {sid}")
                domain_sids.add(sid)

        for sid in domain_sids:
            if sid not in self._domain_sid_cache:
                domain_accounts = await self.ldap_client.sid_to_accounts(sid)
                self._logger.debug(f"located {len(domain_accounts)} domain accounts")
                self._domain_sid_cache[sid] = tuple(
                    account["sid"] for account in domain_accounts
                )
            domain_user_accounts.update(self._domain_sid_cache[sid])

        return list(domain_user_accounts)

    async def get_content(self, file, timestamp=None, doit=None):
        """Get the content for a given file

        Args:
            file (dictionary): Formatted file document
            timestamp (timestamp, optional): Timestamp of file last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to None.

        Returns:
            dictionary: Content document with id, timestamp & text
        """
        if not doit:
            return

        # Skip files that exceed size limit
        if not self.is_file_size_within_limit(file["size"], file["title"]):
            self._logger.warning(
                f"File {file['title']} is {file['size']} bytes, which exceeds configured size limit. Skipping."
            )
            return

        # Skip unsupported file types
        if not self.is_valid_file_type(file["file_extension"], file["title"]):
            self._logger.warning(
                f"File type {file['type']} of {file['title']} is not supported. Skipping."
            )
            return

        try:
            document = {
                "_id": file["id"],
                "_timestamp": file["_timestamp"],
            }

            return await self.download_and_extract_file(
                document,
                file["title"],
                file["file_extension"],
                partial(
                    self.smb_client.get_file_content,
                    share_name=self.share_name,
                    file_path=file["path"],
                ),
            )
        except Exception as e:
            self._logger.error(f"Error getting content for file {file['title']}: {e}")
            raise

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch files and folders in async manner.

        Args:
            filtering: Optional filtering configuration

        Yields:
            tuple: (document dict, content download function or None)
        """
        async for file in self.smb_client.list_files(self.share_name, self.path):
            self._logger.debug(f"getting file - {file['name']}")

            # Skip files that exceed size limit
            if not self.is_file_size_within_limit(file["size"], file["name"]):
                self._logger.warning(
                    f"File {file['name']} is {file['size']} bytes, which exceeds configured size limit. Skipping."
                )
                continue

            # Skip unsupported file types
            if not self.is_valid_file_type(
                self.get_file_extension(file["name"]), file["name"]
            ):
                self._logger.warning(
                    f"File type {file['type']} of {file['name']} is not supported. Skipping."
                )
                continue

            document = {
                "path": file["full_path"],
                "title": file["name"],
                "type": file["type"],
                "file_extension": self.get_file_extension(file["name"]),
                "_id": file["file_id"],
                "created_at": file["created_at"],
                "_timestamp": file["modified_at"],
                "size": file["size"],
            }

            if self._dls_enabled():
                self._logger.debug(f"resolving document sids for - {file['full_path']}")
                document[ACCESS_CONTROL] = await self.resolve_document_sids(file)

            # For files, create a content getter function
            # For directories, use None
            content_getter = (
                None if file["is_directory"] else partial(self.get_content, document)
            )

            yield (document, content_getter)

    def _format_access_control(self, domain_user):
        """format the access control for the document"""
        access_control = [domain_user["upn"], domain_user["sid"]]
        return {
            "_id": domain_user["sid"],
            "identity": {
                "username": domain_user["name"],
                "user_id": domain_user["sid"],
            },
            "created_at": iso_utc(),
        } | es_access_control_query(access_control)

    async def get_access_control(self):
        """Get the access control for the SMB share

        Yields:
            dict: Access control document for each domain user
        """
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        domain_users = await self.ldap_client.get_all_domain_users()
        for user in domain_users:
            yield self._format_access_control(user)
