import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et


class AyxPlugin:
    """
    Implements the plugin interface methods, to be utilized by the Alteryx engine to communicate with a plugin.
    Prefixed with "pi_", the Alteryx engine will expect the below five interface methods to be defined.

    """
    def __init__(self, n_tool_id: int, alteryx_engine: object, generic_engine: object, output_anchor_mgr: object):
        """
        Acts as the constructor for AyxPlugin.
        :param n_tool_id: The assigned unique identification for a tool instance.
        :param alteryx_engine: Provides an interface into the Alteryx engine.
        :param generic_engine: An abstraction of alteryx_engine.
        :param output_anchor_mgr: A helper that wraps the outgoing connections for a plugin.
        """

        # Miscellaneous properties
        self.n_tool_id = n_tool_id
        self.name = str('PySingleInputOutputToolExample_') + str(self.n_tool_id)
        self.single_input = None
        self.n_record_select = None

        # Engine handles
        self.alteryx_engine = alteryx_engine
        self.generic_engine = generic_engine

        # Output anchor management
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None

    def pi_init(self, str_xml: str):
        """
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """
        root = Et.fromstring(str_xml)

        # Getting the dataName data property from the Gui.html
        self.n_record_select = root.find('NRecords').text

        # Getting the output anchor from Config.xml by the output connection name
        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')

    def pi_add_incoming_connection(self, str_type: str, str_name: str):
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: The IncomingInterface object(s).
        """
        self.single_input = IncomingInterface(self)
        return self.single_input

    def pi_add_outgoing_connection(self, str_name: str):
        """
        Called when the Alteryx engine is attempting to add an outgoing data connection.
        :param str_name: The name of the output connection anchor, defined in the Config.xml file.
        :return: True signifies that the connection is accepted.
        """
        return True

    def pi_push_all_records(self, n_record_limit: int):
        """
        Called by the Alteryx engine for tools that have no incoming connection connected.
        Only pertinent to tools which have no upstream connections, like the Input tool.
        :param n_record_limit: Set it to <0 for no limit, 0 for no records, and >0 to specify the number of records.
        :return: True for success, False for failure.
        """
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, 'Missing Incoming Connection')
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed..
        :param b_has_errors: Set to true to not do the final processing.
        """

        # Checks whether connections were properly closed.
        self.output_anchor.assert_close()


class IncomingInterface:
    """
    This class is returned by pi_add_incoming_connection, and it implements the incoming interface methods, to be
    utilized by the Alteryx engine to communicate with a plugin when processing an incoming connection.
    Prefixed with "ii_", the Alteryx engine will expect the below four interface methods to be defined.
    """

    def __init__(self, parent: object):
        """
        Acts as the constructor for AyxPlugin. Instance variable initializations should happen here for PEP8 compliance.
        :param parent: AyxPlugin
        """

        # Miscellaneous properties
        self.parent = parent

        # Record management
        self.record_info_in = None
        self.record_info_out = None
        self.record_cnt = 0
        self.record_copier = None
        self.record_creator = None

    def ii_init(self, record_info_in: object):
        """
        Called when the incoming connection's record metadata is available or has changed, and
        has let the Alteryx engine know what its output will look like.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: True for success, otherwise False.
        """
        # Storing for later use
        self.record_info_in = record_info_in

        # Returns a new, empty RecordCreator object that is identical to record_info_in.
        self.record_info_out = self.record_info_in.clone()

        # Lets the downstream tools know what the outgoing record metadata will look like, based on record_info_out.
        self.parent.output_anchor.init(self.record_info_out)

        # Creating a new, empty record creator based on record_info_out's record layout.
        self.record_creator = self.record_info_out.construct_record_creator()

        # Instantiate a new instance of the RecordCopier class.
        self.record_copier = Sdk.RecordCopier(self.record_info_out, self.record_info_in)

        # Map each column of the input to where we want in the output
        for index in range(self.record_info_in.num_fields):

            # Adding a field index mapping.
            self.record_copier.add(index, index)

        # Let record copier know that all field mappings have been added.
        self.record_copier.done_adding()
        return True

    def ii_push_record(self, in_record: object):
        """
        Responsible for pushing records out, under a count limit set by the user in n_record_select.
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: False if method calling limit (record_cnt) is hit.
        """

        # Keeping track of the push record calls.
        self.record_cnt += 1

        # Quit calling ii_push_record going forward once n_record_select limit is reached.
        if self.record_cnt <= int(self.parent.n_record_select):

            # Push the record downstream
            self.parent.output_anchor.push_record(in_record)

            # Let the Alteryx engine know of the record count
            self.parent.output_anchor.output_record_count(False)
        else:
            return False
        return True

    def ii_update_progress(self, d_percent: float):
        """
        Called when by the upstream tool to report what percentage of records have been pushed.
        :param d_percent: Value between 0.0 and 1.0.
        """

        # Inform the Alteryx engine of the tool's progress.
        self.parent.alteryx_engine.output_tool_progress(self.parent.n_tool_id, d_percent)

        # Inform the outgoing connections of the tool's progress.
        self.parent.output_anchor.update_progress(d_percent)

    def ii_close(self):
        """
        Called when the incoming connection has finished passing all of its records.
        """

        # Let Alteryx engine know that all records have been sent downstream.
        self.parent.output_anchor.output_record_count(True)

        # Close outgoing connections.
        self.parent.output_anchor.close()


