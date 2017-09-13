import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et
import ast as ast


class AyxPlugin:
    def __init__(self, n_tool_id, alteryx_engine, generic_engine, output_anchor_mgr):
        """Initializing members that will be used."""

        # Miscellaneous properties
        self.n_tool_id = n_tool_id
        self.name = str('OptionalOutputPython_') + str(self.n_tool_id)
        self.initialized = False

        # Engine handles
        self.alteryx_engine = alteryx_engine
        self.generic_engine = generic_engine

        # Default configuration setting
        self.send_downstream = None
        self.create_file = None
        self.file_output_dir = None
        self.file_output_name = None

        # Input and Output anchor management
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None
        self.single_input = None

    def pi_init(self, str_xml: str):
        """
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        :return: True
        """
        # Getting the dataName data property from the GUI config
        self.send_downstream = ast.literal_eval(Et.fromstring(str_xml).find('sendDownstream').text)
        self.create_file = Et.fromstring(str_xml).find('createFile').text
        # self.file_output_dir = Et.fromstring(str_xml).find('fileOutputDir').text
        self.file_output_name = Et.fromstring(str_xml).find('fileOutputName').text

        # Getting the output anchor from Config.xml by the output connection name
        if self.send_downstream:
            self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')
        else:            
            self.output_anchor = None
            self.output_anchor_mgr = None

        return True

    def pi_add_incoming_connection(self, str_type: str, str_name: str):
        """
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The type of each input connection.
        :param str_name: A unique name for each input connection.
        :return: Self, and a reference to an object.
        """
        self.single_input = IncomingInterface(self)
        return self.single_input

    def pi_add_outgoing_connection(self, str_name: str):
        """
        Called when the Alteryx engine is attempting to add an outgoing data connection.
        :param str_name: A unique name for each output connection.
        :return: Boolean, where True signifies that the connection is accepted.
        """
        return True

    def pi_push_all_records(self, n_record_limit: int):
        """
        Called when the Alteryx engine when it's expecting the plugin to provide all of its data.
        Only pertinent to tools which have no upstream connections, like the Input tool.
        :param n_record_limit: Set it to <0 for no limit, 0 for no records, and >0 to specify the number of records.
        :return: False, prevent sending all data downstream.
        """
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, 'Missing Incoming Connection')
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all data has finished flowing through all the fields.
        :param b_has_errors: Boolean; set to true to not do the final processing.
        :return: Void
        """
        return

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
        # Miscellaneous properties
        self.parent = parent

        # Record management properties
        self.record_info_in = None
        self.record_info_out = None
        self.record_creator = None
        self.record_copier = None
        return
    
    def ii_init(self, record_info_in: object):
        """
        Called when the incoming connection's record metadata is available or has changed, and
        has let the Alteryx engine know what its output will look like.
        :param record_info_in: A RecordInfo object containing the XML representation for the incoming connection's field and sort properties.
        :return: True
        """

        self.record_info_in = record_info_in
        self.record_info_out = self.record_info_in
        self.record_creator = self.record_info_out.construct_record_creator()
        self.record_copier = Sdk.RecordCopier(self.record_info_out, self.record_info_in)
        if self.parent.send_downstream:
            self.parent.output_anchor.init(self.record_info_out)

        # Map each column of the input to where we want in the output
        for idx in range(len(self.record_info_in)):
            self.record_copier.add(idx, idx)
        self.record_copier.done_adding()
        self.initialized = True
        return True

    def ii_push_record(self, in_record: object):
        """
        Responsible for pushing records out, and outputting the user-selected message before the first record push.
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: Will return False if:
          1. ii_init has not been initialized
          2. ii_push_record calling limit has been reached.
          3. There's a downstream error
        """
        if not self.initialized:
            return False
        
        if not self.parent.send_downstream:
            self.parent.output_anchor = None
            return True

        self.record_creator.reset()
        self.record_copier.copy(self.record_creator, in_record)
        out_record = self.record_creator.finalize_record()
        return self.parent.output_anchor.push_record(out_record)

    def ii_update_progress(self, d_percent: float):
        """
        Called when the incoming connection is requesting that the plugin update its progress.
        :param d_percent: Value between 0 and 1
        :return: Void
        """
        self.parent.alteryx_engine.output_tool_progress(self.parent.n_tool_id, d_percent)
        if self.parent.send_downstream:
            self.parent.output_anchor.update_progress(d_percent)
        return

    def ii_close(self):
        """
        Responsible for outputting the user-selected message before closing the anchor.
        Called when the incoming connection has finished passing all of its records.
        :return: Void
        """
        # if self.message_timing == 'afterLastRecord':
        #     self.display_message(self.message_type, self.message_string)
        if self.parent.send_downstream:
            self.parent.output_anchor.output_record_count(True)
            self.parent.output_anchor.close()
        return