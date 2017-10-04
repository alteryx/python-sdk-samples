import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et
import csv
import os


class AyxPlugin:
    """
    Implements the plugin interface methods, to be utilized by the Alteryx engine to communicate with a plugin.
    Prefixed with "pi_", the Alteryx engine will expect the below five interface methods to be defined.

    """

    def __init__(self, n_tool_id: int, alteryx_engine: object, output_anchor_mgr: object):
        """
        Acts as the constructor for AyxPlugin.
        :param n_tool_id: The assigned unique identification for a tool instance.
        :param alteryx_engine: Provides an interface into the Alteryx engine.
        :param output_anchor_mgr: A helper that wraps the outgoing connections for a plugin.
        """

        # Miscellaneous properties
        self.n_tool_id = n_tool_id
        self.name = 'InputPython_' + str(self.n_tool_id)
        self.closed = False
        self.initialized = False

        # Engine handle
        self.alteryx_engine = alteryx_engine

        # Output anchor management
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None

        # Custom member
        self.file_input_name = ''

    def pi_init(self, str_xml: str):
        """
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """

        # Retrieving file name from GUI.html
        self.file_input_name = Et.fromstring(str_xml).find('browseFiles').text if 'browseFiles' in str_xml else ''

        # Getting the output anchor from Config.xml by the output connection name
        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')

        self.initialized = True

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: The IncomingInterface object(s).
        """

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg('This tool does not accept an Incoming Connection'))
        return self


    def pi_add_outgoing_connection(self, str_name: str) -> bool:
        """
        Called when the Alteryx engine is attempting to add an outgoing data connection.
        :param str_name: The name of the output connection anchor, defined in the Config.xml file.
        :return: True signifies that the connection is accepted.
        """

        return True

    def pi_push_all_records(self, n_record_limit: int) -> bool:
        """
        Called by the Alteryx engine for tools that have no incoming connection connected.
        Only pertinent to tools which have no upstream connections, like the Input tool.
        :param n_record_limit: Set it to <0 for no limit, 0 for no records, and >0 to specify the number of records.
        :return: True for success, False for failure.
        """

        if not self.initialized:
            return False

        # Check if input file name is empty.
        if not self.file_input_name:
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg('Please specify a csv file'))
            return False

        # Check file extension and throw error if not csv.
        if not self.is_csv():
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg('This tool only accepts csv files'))
            return False

        # Check if path exists and throws error if it does not
        if not os.path.exists(self.file_input_name):
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg('No such file or directory: ' + self.file_input_name))
            return False

        # Save a reference to the RecordInfo passed into this function in the global namespace, so we can access it later.
        record_info_out = Sdk.RecordInfo(self.alteryx_engine)

        # Create a read-only file object.
        file_out = open(self.file_input_name, 'r', encoding='utf-8')

        # Create a reader object which will iterate over lines in the given file.
        file_reader = csv.reader(file_out)

        # Add metadata info that is passed to tools downstream.
        for field in next(file_reader):
            record_info_out.add_field(
                field
                , Sdk.FieldType.v_wstring
                , 254
                , 0
                , self.name
                , ''
            )

        # Lets the downstream tools know what the outgoing record metadata will look like, based on record_info_out.
        self.output_anchor.init(record_info_out)

        # Creating a new, empty record creator based on record_info_out's record layout.
        record_creator = record_info_out.construct_record_creator()

        # Keeping track of number of records for use in output message.
        rownum = 0

        # Loop through each record (or row) of data that has been passed into this function.
        for row in file_reader:
            rownum += 1
            # Iterate through the fields in this row and add them in order to the output row
            for index, value in enumerate(row):
                record_info_out[index].set_from_string(record_creator, value)
            out_record = record_creator.finalize_record()
            # Push the record downstream, passing False means completed connections will be automatically closed.
            self.output_anchor.push_record(out_record, False)
            # Reset the record creator in order to begin looping through the next row
            record_creator.reset(0)

        # Returns message indicating number of records read from specified file.
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, self.xmsg(str(rownum) + ' records were read from ' + self.file_input_name))

        return True

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed..
        :param b_has_errors: Set to true to not do the final processing.
        """

        self.closed = True

        # Close outgoing connections.
        self.output_anchor.close()
        return

    def is_csv(self):
        """
        A non-interface method.
        Responsible for determining whether file is csv or not.
        """
        filename, file_extension = os.path.splitext(self.file_input_name)
        if file_extension.lower() == '.csv':
            return True
        return False

    def xmsg(self, msg_string: str):
        """
        A non-interface, non-operational placeholder for the eventual localization of predefined user-facing strings.
        :param msg_string: The user-facing string.
        :return: msg_string
        """

        return msg_string

class IncomingInterface:
    """
    This class is returned by pi_add_incoming_connection, and it implements the incoming interface methods, to be
    utilized by the Alteryx engine to communicate with a plugin when processing an incoming connection.
    Prefixed with "ii_", the Alteryx engine will expect the below four interface methods to be defined.
    """

    def __init__(self, parent: object):
        """
        Acts as the constructor for IncomingInterface. Instance variable initializations should happen here for PEP8 compliance.
        :param parent: AyxPlugin
        """

        # Miscellaneous property
        self.parent = parent

    def ii_init(self, record_info_in: object) -> bool:
        """
        Called when the incoming connection's record metadata is available or has changed, and
        has let the Alteryx engine know what its output will look like.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: True for success, otherwise False.
        """

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.parent.xmsg('This tool does not accept an Incoming Connection'))
        return False

    def ii_push_record(self, in_record: object) -> bool:
        """
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: True for success, otherwise False.
        """

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.parent.xmsg('This tool does not accept an Incoming Connection'))
        return False

    def ii_update_progress(self, d_percent: float):
        """
        Called when by the upstream tool to report what percentage of records have been pushed.
        :param d_percent: Value between 0.0 and 1.0.
        """

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.parent.xmsg('This tool does not accept an Incoming Connection'))

    def ii_close(self):
        """
        Called when the incoming connection has finished passing all of its records.
        """

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.parent.xmsg('This tool does not accept an Incoming Connection'))
