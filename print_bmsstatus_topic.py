#!/usr/bin/env python3
#
# A basic PyUAVCAN demo. This file is included in the user documentation, please keep it tidy.
#
# Distributed under CC0 1.0 Universal (CC0 1.0) Public Domain Dedication. To the extent possible under law, the
# UAVCAN Development Team has waived all copyright and related or neighboring rights to this work.
#

import os
import sys
import typing
import pathlib
import asyncio
import tempfile
import importlib
import pyuavcan
import inspect
import yaml
# Explicitly import transports and media sub-layers that we may need here.
import pyuavcan.transport.can
import pyuavcan.transport.can.media.socketcan
import pyuavcan.transport.redundant

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
        root_namespace_directory=os.path.join(script_path, './px4/'),
        lookup_directories=[os.path.join(script_path, './public_regulated_data_types/uavcan')],
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
    import px4
    import regulated
    import pyuavcan.application

# Import other namespaces we're planning to use. Nested namespaces are not auto-imported, so in order to reach,
# say, "uavcan.node.Heartbeat", you have to do "import uavcan.node".
import uavcan.node                      # noqa E402
import uavcan.diagnostic                # noqa E402
import uavcan.primitive
import regulated.drone.sensor

print('Start')

class PrintBMSStatusApplication:
    def __init__(self):

        # Make sure to initialize the CAN interface. 
        media = pyuavcan.transport.can.media.socketcan.SocketCANMedia('can0', mtu=8)
        transport = pyuavcan.transport.can.CANTransport(media, local_node_id=42)

        assert transport.local_node_id == 42  # Yup, the node-ID is configured.

        # Populate the node info for use with the Node class. Please see the DSDL definition of uavcan.node.GetInfo.
        node_info = uavcan.node.GetInfo_1_0.Response(
            # Version of the protocol supported by the library, and hence by our node.
            protocol_version=uavcan.node.Version_1_0(*pyuavcan.UAVCAN_SPECIFICATION_VERSION),
            # There is a similar field for hardware version, but we don't populate it because it's a software-only node.
            software_version=uavcan.node.Version_1_0(major=1, minor=0),
            # The name of the local node. Should be a reversed Internet domain name, like a Java package.
            name='org.uavcan.pyuavcan.demo.basic_usage',
            # We've left the optional fields default-initialized here.
        )

        # The transport layer is ready; next layer up the protocol stack is the presentation layer. Construct it here.
        presentation = pyuavcan.presentation.Presentation(transport)

        # The application layer is next -- construct the node instance. It will serve GetInfo requests and publish its
        # heartbeat automatically (unless it's anonymous). Read the source code of the Node class for more details.
        self._node = pyuavcan.application.Node(presentation, node_info)

        # A message subscription.
        self._sub_vehiclegpsposition = self._node.presentation.make_subscriber(regulated.drone.sensor.BMSStatus_1_0, 1234)
        self._sub_vehiclegpsposition.receive_in_background(self._handle_msg)

        # When all is initialized, don't forget to start the node!
        self._node.start()

    async def _handle_msg(self,
                                  msg:      regulated.drone.sensor.BMSStatus_1_0,
                                  metadata: pyuavcan.transport.TransferFrom) -> None:
        """
        A subscription message handler. This is also an async function, so we can block inside if necessary.
        The received message object is passed in along with the information about the transfer that delivered it.
        """
        print(chr(27)+'[2j')
        print('\033c')
        print('\x1bc')
        
        msg_string = msg.__repr__()        
        msg_string = msg_string.replace('(',' \n', 1)
        msg_string = msg_string.replace(' ','')
        msg_string = msg_string.replace('=',': ')
        msg_string = msg_string.replace(',','\n')
        print(msg_string)
        

if __name__ == '__main__':
    app = PrintBMSStatusApplication()
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
