#
# Copyright (c) 2020 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

"""
Plug-and-play node-ID allocation logic. See the class documentation for usage info.

Remember that a network that contains static nodes alongside PnP nodes may encounter node-ID conflicts
when a static node appears online after its node-ID is already granted to a PnP node.
To avoid this, the Specification requires that PnP nodes and static nodes are not to be mixed on the same network
(excepting the allocators themselves -- they are always static, naturally).
"""
from __future__ import annotations

import abc
import os
import sys
import typing
import random
import asyncio
import tempfile
import importlib
import pyuavcan
import pathlib
import logging
import sqlite3
import time
# Explicitly import transports and media sub-layers that we may need here.
import pyuavcan.transport.can
import pyuavcan.transport.can.media.socketcan
import pyuavcan.transport.redundant
from pyuavcan.presentation import Presentation


# We will need a directory to store the generated Python packages in.
#
# It is perfectly acceptable to just use a random temp directory at every run, but the disadvantage of that approach
# is that the packages will be re-generated from scratch every time the program is started, which may be undesirable.
#
# So in this example we select a fixed temp dir name (make sure it's unique enough) and shard its contents by the
# library version. The sharding helps us ensure that we won't attempt to use a package generated for an older library
# version with a newer one, as they may be incompatible.
#
# Another sensible location for the generated package directory is somewhere in the application data directory,
# like "~/.my-app/dsdl/{pyuavcan.__version__}/"; or, for Windows: "%APPDATA%/my-app/dsdl/{pyuavcan.__version__}/".
dsdl_generated_dir = pathlib.Path(tempfile.gettempdir(), 'dsdl-for-my-program', f'pyuavcan-v{pyuavcan.__version__}')
dsdl_generated_dir.mkdir(parents=True, exist_ok=True)
print('Generated DSDL packages will be stored in:', dsdl_generated_dir, file=sys.stderr)

# We will need to import the packages once they are generated, so we should update the module import look-up path set.
# If you're using an IDE for development, add this path to its look-up set as well for code completion to work.
sys.path.insert(0, str(dsdl_generated_dir))

# Now we can import our packages. If import fails, invoke the code generator, then import again.
try:
    import px4     # This is our vendor-specific root namespace. Custom data types.
    import regulated
    import pyuavcan.application  # The application module requires the standard types from the root namespace "uavcan".
except (ImportError, AttributeError):
    script_path = os.path.abspath(os.path.dirname(__file__))
    # Generate the standard namespace. The order actually doesn't matter.
    pyuavcan.dsdl.generate_package(
        root_namespace_directory=os.path.join(script_path, './public_regulated_data_types/uavcan'),
        output_directory=dsdl_generated_dir,
    )
        
    pyuavcan.dsdl.generate_package(
        root_namespace_directory=os.path.join(script_path, './public_regulated_data_types/regulated/'),
        lookup_directories=[os.path.join(script_path, './public_regulated_data_types/uavcan')],
        output_directory=dsdl_generated_dir,
    )
    
    # Okay, we can try importing again. We need to clear the import cache first because Python's import machinery
    # requires that; see the docs for importlib.invalidate_caches() for more info.
    importlib.invalidate_caches()

from uavcan.pnp import NodeIDAllocationData_1_0 as NodeIDAllocationData_1
from uavcan.pnp import NodeIDAllocationData_2_0 as NodeIDAllocationData_2
from uavcan.node import ID_1_0 as ID
import pyuavcan


_PSEUDO_UNIQUE_ID_MASK = \
    2 ** list(pyuavcan.dsdl.get_model(NodeIDAllocationData_1)['unique_id_hash'].data_type.bit_length_set)[0] - 1

_NODE_ID_MASK = 2 ** max(pyuavcan.dsdl.get_model(ID)['value'].data_type.bit_length_set) - 1

_UNIQUE_ID_SIZE_BYTES = 16

_NUM_RESERVED_TOP_NODE_IDS = 2

_DB_DEFAULT_LOCATION = ':memory:'
_DB_TIMEOUT = 0.1


_logger = logging.getLogger(__name__)


class Allocatee:
    """
    Plug-and-play node-ID protocol client.

    This class represents a node that requires an allocated node-ID.
    Once started, the client will keep issuing node-ID allocation requests until either a node-ID is granted
    or until the node-ID of the underlying transport instance ceases to be anonymous (that could happen if the
    transport is re-configured externally).
    The status (whether the allocation is finished or still in progress) is to be queried periodically
    via the method :meth:`get_result`.

    Uses v1 allocation messages if the transport MTU is small (like if the transport is Classic CAN).
    Switches between v1 and v2 as necessary on the fly if the transport is reconfigured at runtime.
    """

    DEFAULT_PRIORITY = pyuavcan.transport.Priority.SLOW

    _MTU_THRESHOLD = max(pyuavcan.dsdl.get_model(NodeIDAllocationData_2).bit_length_set) // 8

    def __init__(self,
                 presentation:      pyuavcan.presentation.Presentation,
                 local_unique_id:   bytes,
                 preferred_node_id: typing.Optional[int] = None):
        """
        :param presentation: The presentation instance to use. If the underlying transport is not anonymous
            (i.e., a node-ID is already set), the allocatee will simply return the existing node-ID and do nothing.

        :param local_unique_id: The 128-bit globally unique-ID of the local node; the same value is also contained
            in the ``uavcan.node.GetInfo.Response``. Beware that random generation of the unique-ID at every launch
            is a bad idea because it will exhaust the allocation table quickly. Refer to the Specification for details.

        :param preferred_node_id: If the application prefers to obtain a particular node-ID, it can specify it here.
            If provided, the PnP allocator will try to find a node-ID that is close to the stated preference.
            If not provided, the PnP allocator will pick a node-ID at its own discretion.
        """
        self._presentation = presentation
        self._local_unique_id = local_unique_id
        self._preferred_node_id = int(preferred_node_id if preferred_node_id is not None else _NODE_ID_MASK)
        if not isinstance(self._local_unique_id, bytes) or len(self._local_unique_id) != _UNIQUE_ID_SIZE_BYTES:
            raise ValueError(f'Invalid unique-ID: {self._local_unique_id!r}')
        if not (0 <= self._preferred_node_id <= _NODE_ID_MASK):
            raise ValueError(f'Invalid preferred node-ID: {self._preferred_node_id}')

        self._result: typing.Optional[int] = None
        self._sub_1 = self._presentation.make_subscriber_with_fixed_subject_id(NodeIDAllocationData_1)
        self._sub_2 = self._presentation.make_subscriber_with_fixed_subject_id(NodeIDAllocationData_2)
        self._pub: typing.Union[None,
                                pyuavcan.presentation.Publisher[NodeIDAllocationData_1],
                                pyuavcan.presentation.Publisher[NodeIDAllocationData_2]] = None
        self._timer: typing.Optional[asyncio.TimerHandle] = None

    def get_result(self) -> typing.Optional[int]:
        """
        None if the allocation is still in progress. If the allocation is finished, this is the allocated node-ID.
        """
        res = self._presentation.transport.local_node_id
        return res if res is not None else self._result

    def start(self) -> None:
        self._sub_1.receive_in_background(self._on_response)
        self._sub_2.receive_in_background(self._on_response)
        self._restart_timer()

    def close(self) -> None:
        """
        The instance automatically closes itself shortly after the allocation is finished,
        so it's not necessary to invoke this method after a successful allocation. The method is idempotent.
        """
        if self._timer is not None:
            self._timer.cancel()
        self._sub_1.close()
        self._sub_2.close()
        if self._pub is not None:
            self._pub.close()

    def _on_timer(self) -> None:
        self._restart_timer()
        if self.get_result() is not None:
            self.close()
            return

        msg: typing.Any = None
        try:
            if self._presentation.transport.protocol_parameters.mtu > self._MTU_THRESHOLD:
                msg = NodeIDAllocationData_2(node_id=ID(self._preferred_node_id), unique_id=self._local_unique_id)
            else:
                msg = NodeIDAllocationData_1(unique_id_hash=_make_pseudo_unique_id(self._local_unique_id))

            if self._pub is None or self._pub.dtype != type(msg):
                if self._pub is not None:
                    self._pub.close()
                self._pub = self._presentation.make_publisher_with_fixed_subject_id(type(msg))
                self._pub.priority = self.DEFAULT_PRIORITY

            _logger.debug('Publishing allocation request %s', msg)
            self._pub.publish_soon(msg)
        except Exception as ex:
            _logger.exception(f'Could not send allocation request {msg}: {ex}')

    def _restart_timer(self) -> None:
        t_request = random.random()
        self._timer = self._presentation.loop.call_later(t_request, self._on_timer)

    async def _on_response(self,
                           msg: typing.Union[NodeIDAllocationData_1, NodeIDAllocationData_2],
                           meta: pyuavcan.transport.TransferFrom) -> None:
        if self.get_result() is not None:  # Allocation already done, nothing else to do.
            return

        if meta.source_node_id is None:  # Another request, ignore.
            return

        allocated: typing.Optional[int] = None
        if isinstance(msg, NodeIDAllocationData_1):
            if msg.unique_id_hash == _make_pseudo_unique_id(self._local_unique_id) and len(msg.allocated_node_id) > 0:
                allocated = msg.allocated_node_id[0].value
        elif isinstance(msg, NodeIDAllocationData_2):
            if msg.unique_id.tobytes() == self._local_unique_id:
                allocated = msg.node_id.value
        else:
            assert False, 'Internal logic error'

        if allocated is None:
            return  # UID mismatch.

        assert isinstance(allocated, int)
        protocol_params = self._presentation.transport.protocol_parameters
        max_node_id = min(protocol_params.max_nodes - 1, _NODE_ID_MASK)
        if not (0 <= allocated <= max_node_id):
            _logger.warning(f'Allocated node-ID {allocated} ignored because it is incompatible with the transport: '
                            f'{protocol_params}')
            return

        _logger.info('Plug-and-play allocation done: got node-ID %s from server %s', allocated, meta.source_node_id)
        self._result = allocated


class Allocator(abc.ABC):
    """
    An abstract PnP allocator interface. See derived classes.

    If an existing allocation table is reused with a least capable transport where the maximum node-ID is smaller,
    the allocator may create redundant allocations in order to avoid granting node-ID values that exceed the valid
    node-ID range for the transport.
    """

    DEFAULT_PUBLICATION_TIMEOUT = 5.0
    """
    The allocation message publication timeout is chosen to be large because the data is constant
    (does not lose relevance over time) and the priority level is usually low.
    """

    @abc.abstractmethod
    def register_node(self, node_id: int, unique_id: typing.Optional[bytes]) -> None:
        """
        This method shall be invoked whenever a new node appears online and/or whenever its unique-ID is obtained.
        The recommended usage pattern is to subscribe to the update events from
        :class:`pyuavcan.application.node_tracker.NodeTracker`, where the necessary update logic is already implemented.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def start(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class CentralizedAllocator(Allocator):
    """
    The centralized plug-and-play node-ID allocator.
    """

    def __init__(self,
                 presentation:    pyuavcan.presentation.Presentation,
                 local_unique_id: typing.Optional[bytes] = None,
                 database_file:   typing.Optional[typing.Union[str, pathlib.Path]] = None):
        """
        :param presentation: The presentation instance to run the allocator on.

        :param local_unique_id: The 128-bit globally unique-ID of the local node; the same value is also contained
            in the ``uavcan.node.GetInfo.Response``. If not set, defaults to all-zeros.
            Refer to the Specification for details.

        :param database_file: If provided, shall specify the path to the database file containing an allocation table.
            If the file does not exist, it will be automatically created. If None (default), the allocation table
            will be created in memory (therefore the allocation data will be lost after the instance is disposed).
        """
        self._presentation = presentation

        self._local_unique_id = local_unique_id if local_unique_id is not None else bytes(_UNIQUE_ID_SIZE_BYTES)
        if not isinstance(self._local_unique_id, bytes) or len(self._local_unique_id) != _UNIQUE_ID_SIZE_BYTES:
            raise ValueError(f'Invalid local unique-ID: {self._local_unique_id!r}')

        local_node_id = self._presentation.transport.local_node_id
        if local_node_id is None:
            raise ValueError('The allocator cannot run on an anonymous node')

        self._alloc = _AllocationTable(sqlite3.connect(str(database_file or _DB_DEFAULT_LOCATION), timeout=_DB_TIMEOUT))
        self._alloc.register(local_node_id, self._local_unique_id)

        self._sub1 = presentation.make_subscriber_with_fixed_subject_id(NodeIDAllocationData_1)
        self._sub2 = presentation.make_subscriber_with_fixed_subject_id(NodeIDAllocationData_2)

        self._pub1 = presentation.make_publisher_with_fixed_subject_id(NodeIDAllocationData_1)
        self._pub2 = presentation.make_publisher_with_fixed_subject_id(NodeIDAllocationData_2)
        self._pub1.send_timeout = self.DEFAULT_PUBLICATION_TIMEOUT
        self._pub2.send_timeout = self.DEFAULT_PUBLICATION_TIMEOUT

    def register_node(self, node_id: int, unique_id: typing.Optional[bytes]) -> None:
        self._alloc.register(node_id, unique_id)

    def start(self) -> None:
        _logger.debug('Centralized allocator starting with the following allocation table:\n%s', self._alloc)
        self._sub1.receive_in_background(self._on_message)
        self._sub2.receive_in_background(self._on_message)

    def close(self) -> None:
        for port in [self._sub1, self._sub2, self._pub1, self._pub2]:
            assert isinstance(port, pyuavcan.presentation.Port)
            port.close()
        self._alloc.close()

    async def _on_message(self,
                          msg: typing.Union[NodeIDAllocationData_1, NodeIDAllocationData_2],
                          meta: pyuavcan.transport.TransferFrom) -> None:
        if meta.source_node_id is not None:
            _logger.error(
                f'Invalid network configuration: another node-ID allocator detected at node-ID {meta.source_node_id}. '
                f'There shall be exactly one allocator on the network. If modular redundancy is desired, '
                f'use a distributed allocator (currently, a centralized allocator is running). '
                f'The detected allocation response message is {msg} with metadata {meta}.'
            )
            return

        _logger.debug('Received allocation request %s with metadata %s', msg, meta)
        max_node_id = self._presentation.transport.protocol_parameters.max_nodes - 1 - _NUM_RESERVED_TOP_NODE_IDS
        assert max_node_id > 0

        if isinstance(msg, NodeIDAllocationData_1):
            allocated = self._alloc.allocate(max_node_id, max_node_id, pseudo_unique_id=msg.unique_id_hash)
            if allocated is not None:
                self._respond_v1(meta.priority, msg.unique_id_hash, allocated)
                return
        elif isinstance(msg, NodeIDAllocationData_2):
            uid = msg.unique_id.tobytes()
            allocated = self._alloc.allocate(msg.node_id.value, max_node_id, unique_id=uid)
            if allocated is not None:
                self._respond_v2(meta.priority, uid, allocated)
                return
        else:
            assert False, 'Internal logic error'
        _logger.warning(
            f'Allocation table is full, ignoring request {msg} with metadata {meta}. Please purge the table.'
        )

    def _respond_v1(self, priority: pyuavcan.transport.Priority, unique_id_hash: int, allocated_node_id: int) -> None:
        msg = NodeIDAllocationData_1(unique_id_hash=unique_id_hash, allocated_node_id=[ID(allocated_node_id)])
        _logger.info('Publishing allocation response v1: %s', msg)
        self._pub1.priority = priority
        self._pub1.publish_soon(msg)

    def _respond_v2(self, priority: pyuavcan.transport.Priority, unique_id: bytes, allocated_node_id: int) -> None:
        msg = NodeIDAllocationData_2(node_id=ID(allocated_node_id), unique_id=unique_id,)
        _logger.info('Publishing allocation response v2: %s', msg)
        self._pub2.priority = priority
        self._pub2.publish_soon(msg)


# noinspection PyAbstractClass
class DistributedAllocator(Allocator):
    """
    This class is a placeholder. The implementation is missing (could use help here).
    The implementation can be based on the existing distributed allocator from Libuavcan v0,
    although the new PnP protocol is much simpler because it lacks multi-stage exchanges.
    """

    def __init__(self, presentation: pyuavcan.presentation.Presentation):
        assert presentation
        raise NotImplementedError((self.__doc__ or '').strip())


class _AllocationTable:
    _SCHEMA = '''
    create table if not exists `allocation` (
        `node_id`          int not null unique check(node_id >= 0),
        `unique_id_hex`    varchar(32) not null,  -- all zeros if unique-ID is unknown.
        `pseudo_unique_id` bigint not null check(pseudo_unique_id >= 0), -- 48 LSB of CRC64WE(unique-ID); v1 compat.
        `ts`               time not null default current_timestamp,
        primary key(node_id)
    );
    '''

    def __init__(self, db_connection: sqlite3.Connection):
        self._con = db_connection
        self._con.execute(self._SCHEMA)
        self._con.commit()

    def register(self, node_id: int, unique_id: typing.Optional[bytes]) -> None:
        unique_id_defined = unique_id is not None
        if unique_id is None:
            unique_id = bytes(_UNIQUE_ID_SIZE_BYTES)
        if not isinstance(unique_id, bytes) or len(unique_id) != _UNIQUE_ID_SIZE_BYTES:
            raise ValueError(f'Invalid unique-ID: {unique_id!r}')
        if not isinstance(node_id, int) or not (0 <= node_id <= _NODE_ID_MASK):
            raise ValueError(f'Invalid node-ID: {node_id!r}')

        def execute() -> None:
            assert isinstance(unique_id, bytes)
            self._con.execute(
                'insert or replace into allocation (node_id, unique_id_hex, pseudo_unique_id) values (?, ?, ?);',
                (node_id, unique_id.hex(), _make_pseudo_unique_id(unique_id))
            )
            self._con.commit()

        res = self._con.execute('select unique_id_hex from allocation where node_id = ?', (node_id,)).fetchone()
        existing_uid = bytes.fromhex(res[0]) if res is not None else None
        if existing_uid is None:
            _logger.debug('Original node registration: NID % 5d, UID %s', node_id, unique_id.hex())
            execute()
        elif unique_id_defined and existing_uid != unique_id:
            _logger.debug('Updated node registration:  NID % 5d, UID %s -> %s',
                          node_id, existing_uid.hex(), unique_id.hex())
            execute()

    def allocate(self,
                 preferred_node_id: int,
                 max_node_id:       int,
                 unique_id:         typing.Optional[bytes] = None,
                 pseudo_unique_id:  typing.Optional[int] = None) -> typing.Optional[int]:
        use_unique_id = unique_id is not None
        preferred_node_id = min(max(preferred_node_id, 0), max_node_id)
        _logger.debug('Table alloc request: preferred_node_id=%s, max_node_id=%s, unique_id=%s, pseudo_unique_id=%s',
                      preferred_node_id, max_node_id, unique_id.hex() if unique_id else None, pseudo_unique_id)
        if unique_id is None:
            unique_id = bytes(_UNIQUE_ID_SIZE_BYTES)
        if pseudo_unique_id is None:
            pseudo_unique_id = _make_pseudo_unique_id(unique_id)
        assert isinstance(unique_id, bytes) and len(unique_id) == _UNIQUE_ID_SIZE_BYTES
        assert isinstance(pseudo_unique_id, int) and (0 <= pseudo_unique_id <= _PSEUDO_UNIQUE_ID_MASK)

        # Check if there is an existing allocation for this UID. If there are multiple matches, pick the newest.
        # Ignore existing allocations where the node-ID exceeds the maximum in case we're reusing an existing
        # allocation table with a less capable transport.
        if use_unique_id:
            res = self._con.execute(
                'select node_id from allocation where unique_id_hex = ? and node_id <= ? order by ts desc limit 1',
                (unique_id.hex(), max_node_id)
            ).fetchone()
        else:
            res = self._con.execute(
                'select node_id from allocation where pseudo_unique_id = ? and node_id <= ? order by ts desc limit 1',
                (pseudo_unique_id, max_node_id)
            ).fetchone()
        if res is not None:
            candidate = int(res[0])
            assert 0 <= candidate <= max_node_id, 'Internal logic error'
            _logger.debug('Serving existing allocation: NID %s, (pseudo-)UID %s',
                          candidate, unique_id.hex() if use_unique_id else hex(pseudo_unique_id))
            return candidate

        # Do a new allocation. Consider re-implementing this in pure SQL -- should be possible with SQLite.
        result: typing.Optional[int] = None
        candidate = preferred_node_id
        while result is None and candidate <= max_node_id:
            if self._try_allocate(candidate, unique_id, pseudo_unique_id):
                result = candidate
            candidate += 1
        candidate = preferred_node_id
        while result is None and candidate >= 0:
            if self._try_allocate(candidate, unique_id, pseudo_unique_id):
                result = candidate
            candidate -= 1

        # Final report.
        if result is not None:
            _logger.debug('New allocation: allocated NID %s, (pseudo-)UID %s, preferred NID %s',
                          result, unique_id.hex() if use_unique_id else hex(pseudo_unique_id), preferred_node_id)
        return result

    def close(self) -> None:
        self._con.close()

    def _try_allocate(self, node_id: int, unique_id: bytes, pseudo_unique_id: int) -> bool:
        try:
            self._con.execute(
                'insert into allocation (node_id, unique_id_hex, pseudo_unique_id) values (?, ?, ?);',
                (node_id, unique_id.hex(), pseudo_unique_id)
            )
            self._con.commit()
        except sqlite3.IntegrityError:  # Such entry already exists.
            return False
        return True

    def __str__(self) -> str:
        """Displays the table as a multi-line string in TSV format with one header line."""
        lines = ['Node-ID\t' + 'Unique-ID/hash (hex)'.ljust(32 + 1 + 12) + '\tUpdate timestamp']
        for nid, uid_hex, pseudo, ts in self._con.execute(
            'select node_id, unique_id_hex, pseudo_unique_id, ts from allocation order by ts desc'
        ).fetchall():
            lines.append(f'{nid: 5d}  \t{uid_hex:32s}/{pseudo:012x}\t{ts}')
        return '\n'.join(lines) + '\n'

class CRCAlgorithm(abc.ABC):
    """
    Implementations are default-constructible.
    """

    @abc.abstractmethod
    def add(self, data: typing.Union[bytes, bytearray, memoryview]) -> None:
        """
        Updates the value with the specified block of data.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def check_residue(self) -> bool:
        """
        Checks if the current state matches the algorithm-specific residue.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def value(self) -> int:
        """
        The current CRC value, with output XOR applied, if applicable.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def value_as_bytes(self) -> bytes:
        """
        The current CRC value serialized in the algorithm-specific byte order.
        """
        raise NotImplementedError

    @classmethod
    def new(cls, *fragments: typing.Union[bytes, bytearray, memoryview]) -> CRCAlgorithm:
        """
        A factory that creates the new instance with the value computed over the fragments.
        """
        self = cls()
        for frag in fragments:
            self.add(frag)
        return self

class CRC64WE(CRCAlgorithm):
    """
    A CRC-64/WE algorithm implementation.
    - Name:          CRC-64/WE
    - Initial value: 0xFFFFFFFFFFFFFFFF
    - Polynomial:    0x42F0E1EBA9EA3693
    - Output XOR:    0xFFFFFFFFFFFFFFFF
    - Residue:       0xFCACBEBD5931A992
    - Check:         0x62EC59E3F1A4F00A
    >>> assert CRC64WE().value == 0
    >>> c = CRC64WE()
    >>> c.add(b'123456')
    >>> c.add(b'789')
    >>> c.value  # 0x62EC59E3F1A4F00A
    7128171145767219210
    >>> c.add(b'')
    >>> c.value
    7128171145767219210
    >>> c.add(c.value_as_bytes)
    >>> c.value  # Inverted residue
    239606959702955629
    >>> c.check_residue()
    True
    >>> CRC64WE.new(b'123', b'', b'456789').value
    7128171145767219210
    """

    def __init__(self) -> None:
        assert len(self._TABLE) == 256
        self._value = self._MASK

    def add(self, data: typing.Union[bytes, bytearray, memoryview]) -> None:
        val = self._value
        table = self._TABLE
        for b in data:
            val = (table[b ^ (val >> 56)] ^ (val << 8)) & self._MASK
        assert 0 <= val < 2 ** 64
        self._value = val

    def check_residue(self) -> bool:
        return self._value == 0xFCACBEBD5931A992

    @property
    def value(self) -> int:
        return self._value ^ self._MASK

    @property
    def value_as_bytes(self) -> bytes:
        return self.value.to_bytes(8, 'big')

    _MASK = 0xFFFFFFFFFFFFFFFF
    _TABLE = [
        0x0000000000000000, 0x42F0E1EBA9EA3693, 0x85E1C3D753D46D26, 0xC711223CFA3E5BB5, 0x493366450E42ECDF,
        0x0BC387AEA7A8DA4C, 0xCCD2A5925D9681F9, 0x8E224479F47CB76A, 0x9266CC8A1C85D9BE, 0xD0962D61B56FEF2D,
        0x17870F5D4F51B498, 0x5577EEB6E6BB820B, 0xDB55AACF12C73561, 0x99A54B24BB2D03F2, 0x5EB4691841135847,
        0x1C4488F3E8F96ED4, 0x663D78FF90E185EF, 0x24CD9914390BB37C, 0xE3DCBB28C335E8C9, 0xA12C5AC36ADFDE5A,
        0x2F0E1EBA9EA36930, 0x6DFEFF5137495FA3, 0xAAEFDD6DCD770416, 0xE81F3C86649D3285, 0xF45BB4758C645C51,
        0xB6AB559E258E6AC2, 0x71BA77A2DFB03177, 0x334A9649765A07E4, 0xBD68D2308226B08E, 0xFF9833DB2BCC861D,
        0x388911E7D1F2DDA8, 0x7A79F00C7818EB3B, 0xCC7AF1FF21C30BDE, 0x8E8A101488293D4D, 0x499B3228721766F8,
        0x0B6BD3C3DBFD506B, 0x854997BA2F81E701, 0xC7B97651866BD192, 0x00A8546D7C558A27, 0x4258B586D5BFBCB4,
        0x5E1C3D753D46D260, 0x1CECDC9E94ACE4F3, 0xDBFDFEA26E92BF46, 0x990D1F49C77889D5, 0x172F5B3033043EBF,
        0x55DFBADB9AEE082C, 0x92CE98E760D05399, 0xD03E790CC93A650A, 0xAA478900B1228E31, 0xE8B768EB18C8B8A2,
        0x2FA64AD7E2F6E317, 0x6D56AB3C4B1CD584, 0xE374EF45BF6062EE, 0xA1840EAE168A547D, 0x66952C92ECB40FC8,
        0x2465CD79455E395B, 0x3821458AADA7578F, 0x7AD1A461044D611C, 0xBDC0865DFE733AA9, 0xFF3067B657990C3A,
        0x711223CFA3E5BB50, 0x33E2C2240A0F8DC3, 0xF4F3E018F031D676, 0xB60301F359DBE0E5, 0xDA050215EA6C212F,
        0x98F5E3FE438617BC, 0x5FE4C1C2B9B84C09, 0x1D14202910527A9A, 0x93366450E42ECDF0, 0xD1C685BB4DC4FB63,
        0x16D7A787B7FAA0D6, 0x5427466C1E109645, 0x4863CE9FF6E9F891, 0x0A932F745F03CE02, 0xCD820D48A53D95B7,
        0x8F72ECA30CD7A324, 0x0150A8DAF8AB144E, 0x43A04931514122DD, 0x84B16B0DAB7F7968, 0xC6418AE602954FFB,
        0xBC387AEA7A8DA4C0, 0xFEC89B01D3679253, 0x39D9B93D2959C9E6, 0x7B2958D680B3FF75, 0xF50B1CAF74CF481F,
        0xB7FBFD44DD257E8C, 0x70EADF78271B2539, 0x321A3E938EF113AA, 0x2E5EB66066087D7E, 0x6CAE578BCFE24BED,
        0xABBF75B735DC1058, 0xE94F945C9C3626CB, 0x676DD025684A91A1, 0x259D31CEC1A0A732, 0xE28C13F23B9EFC87,
        0xA07CF2199274CA14, 0x167FF3EACBAF2AF1, 0x548F120162451C62, 0x939E303D987B47D7, 0xD16ED1D631917144,
        0x5F4C95AFC5EDC62E, 0x1DBC74446C07F0BD, 0xDAAD56789639AB08, 0x985DB7933FD39D9B, 0x84193F60D72AF34F,
        0xC6E9DE8B7EC0C5DC, 0x01F8FCB784FE9E69, 0x43081D5C2D14A8FA, 0xCD2A5925D9681F90, 0x8FDAB8CE70822903,
        0x48CB9AF28ABC72B6, 0x0A3B7B1923564425, 0x70428B155B4EAF1E, 0x32B26AFEF2A4998D, 0xF5A348C2089AC238,
        0xB753A929A170F4AB, 0x3971ED50550C43C1, 0x7B810CBBFCE67552, 0xBC902E8706D82EE7, 0xFE60CF6CAF321874,
        0xE224479F47CB76A0, 0xA0D4A674EE214033, 0x67C58448141F1B86, 0x253565A3BDF52D15, 0xAB1721DA49899A7F,
        0xE9E7C031E063ACEC, 0x2EF6E20D1A5DF759, 0x6C0603E6B3B7C1CA, 0xF6FAE5C07D3274CD, 0xB40A042BD4D8425E,
        0x731B26172EE619EB, 0x31EBC7FC870C2F78, 0xBFC9838573709812, 0xFD39626EDA9AAE81, 0x3A28405220A4F534,
        0x78D8A1B9894EC3A7, 0x649C294A61B7AD73, 0x266CC8A1C85D9BE0, 0xE17DEA9D3263C055, 0xA38D0B769B89F6C6,
        0x2DAF4F0F6FF541AC, 0x6F5FAEE4C61F773F, 0xA84E8CD83C212C8A, 0xEABE6D3395CB1A19, 0x90C79D3FEDD3F122,
        0xD2377CD44439C7B1, 0x15265EE8BE079C04, 0x57D6BF0317EDAA97, 0xD9F4FB7AE3911DFD, 0x9B041A914A7B2B6E,
        0x5C1538ADB04570DB, 0x1EE5D94619AF4648, 0x02A151B5F156289C, 0x4051B05E58BC1E0F, 0x87409262A28245BA,
        0xC5B073890B687329, 0x4B9237F0FF14C443, 0x0962D61B56FEF2D0, 0xCE73F427ACC0A965, 0x8C8315CC052A9FF6,
        0x3A80143F5CF17F13, 0x7870F5D4F51B4980, 0xBF61D7E80F251235, 0xFD913603A6CF24A6, 0x73B3727A52B393CC,
        0x31439391FB59A55F, 0xF652B1AD0167FEEA, 0xB4A25046A88DC879, 0xA8E6D8B54074A6AD, 0xEA16395EE99E903E,
        0x2D071B6213A0CB8B, 0x6FF7FA89BA4AFD18, 0xE1D5BEF04E364A72, 0xA3255F1BE7DC7CE1, 0x64347D271DE22754,
        0x26C49CCCB40811C7, 0x5CBD6CC0CC10FAFC, 0x1E4D8D2B65FACC6F, 0xD95CAF179FC497DA, 0x9BAC4EFC362EA149,
        0x158E0A85C2521623, 0x577EEB6E6BB820B0, 0x906FC95291867B05, 0xD29F28B9386C4D96, 0xCEDBA04AD0952342,
        0x8C2B41A1797F15D1, 0x4B3A639D83414E64, 0x09CA82762AAB78F7, 0x87E8C60FDED7CF9D, 0xC51827E4773DF90E,
        0x020905D88D03A2BB, 0x40F9E43324E99428, 0x2CFFE7D5975E55E2, 0x6E0F063E3EB46371, 0xA91E2402C48A38C4,
        0xEBEEC5E96D600E57, 0x65CC8190991CB93D, 0x273C607B30F68FAE, 0xE02D4247CAC8D41B, 0xA2DDA3AC6322E288,
        0xBE992B5F8BDB8C5C, 0xFC69CAB42231BACF, 0x3B78E888D80FE17A, 0x7988096371E5D7E9, 0xF7AA4D1A85996083,
        0xB55AACF12C735610, 0x724B8ECDD64D0DA5, 0x30BB6F267FA73B36, 0x4AC29F2A07BFD00D, 0x08327EC1AE55E69E,
        0xCF235CFD546BBD2B, 0x8DD3BD16FD818BB8, 0x03F1F96F09FD3CD2, 0x41011884A0170A41, 0x86103AB85A2951F4,
        0xC4E0DB53F3C36767, 0xD8A453A01B3A09B3, 0x9A54B24BB2D03F20, 0x5D45907748EE6495, 0x1FB5719CE1045206,
        0x919735E51578E56C, 0xD367D40EBC92D3FF, 0x1476F63246AC884A, 0x568617D9EF46BED9, 0xE085162AB69D5E3C,
        0xA275F7C11F7768AF, 0x6564D5FDE549331A, 0x279434164CA30589, 0xA9B6706FB8DFB2E3, 0xEB46918411358470,
        0x2C57B3B8EB0BDFC5, 0x6EA7525342E1E956, 0x72E3DAA0AA188782, 0x30133B4B03F2B111, 0xF7021977F9CCEAA4,
        0xB5F2F89C5026DC37, 0x3BD0BCE5A45A6B5D, 0x79205D0E0DB05DCE, 0xBE317F32F78E067B, 0xFCC19ED95E6430E8,
        0x86B86ED5267CDBD3, 0xC4488F3E8F96ED40, 0x0359AD0275A8B6F5, 0x41A94CE9DC428066, 0xCF8B0890283E370C,
        0x8D7BE97B81D4019F, 0x4A6ACB477BEA5A2A, 0x089A2AACD2006CB9, 0x14DEA25F3AF9026D, 0x562E43B4931334FE,
        0x913F6188692D6F4B, 0xD3CF8063C0C759D8, 0x5DEDC41A34BBEEB2, 0x1F1D25F19D51D821, 0xD80C07CD676F8394,
        0x9AFCE626CE85B507,
    ]

def _make_pseudo_unique_id(unique_id: bytes) -> int:
    """
    The recommended mapping function from unique-ID to pseudo unique-ID.
    """
    assert isinstance(unique_id, bytes) and len(unique_id) == _UNIQUE_ID_SIZE_BYTES
    return int(CRC64WE.new(unique_id).value & _PSEUDO_UNIQUE_ID_MASK)

def _uid(as_hex: str) -> bytes:
    out = bytes.fromhex(as_hex)
    assert len(out) == 16
    return out

_TABLE = pathlib.Path('allocation_table.sqlite.tmp')


if __name__ == '__main__':
    media = pyuavcan.transport.can.media.socketcan.SocketCANMedia('can0', mtu=8)
    #transport_client = pyuavcan.transport.can.CANTransport(media, local_node_id=None)
    transport_server = pyuavcan.transport.can.CANTransport(media, local_node_id=123)
    
    
    #pres_client = Presentation(transport_client)
    pres_server = Presentation(transport_server)

    allocator = CentralizedAllocator(pres_server, _uid('deadbeefdeadbeefdeadbeefdeadbeef'), _TABLE)
    allocator.start()   

    app_tasks = asyncio.Task.all_tasks()
    
    async def list_tasks_periodically() -> None:
        """Print active tasks periodically for demo purposes."""
        import re

        def repr_task(t: asyncio.Task) -> str:
            try:
                out, = re.findall(r'^<([^<]+<[^>]+>)', str(t))
            except ValueError:
                out = str(t)
            return out

        while True:
            #print('\nActive tasks:\n' + '\n'.join(map(repr_task, asyncio.Task.all_tasks())), file=sys.stderr)
            await asyncio.sleep(10)

    asyncio.get_event_loop().create_task(list_tasks_periodically())

    # The node and PyUAVCAN objects have created internal tasks, which we need to run now.
    # In this case we want to automatically stop and exit when no tasks are left to run.
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*app_tasks))
