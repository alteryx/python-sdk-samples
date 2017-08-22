import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et


class AyxPlugin:
    def __init__(self, n_tool_id, alteryx_engine, generic_engine, output_anchor_mgr):
        """Initializing members that will be used."""

        # Miscellaneous properties
        self.n_tool_id = n_tool_id
        self.name = str('PyOptionalOutputToolExample_') + str(self.n_tool_id)
        self.initialized = False

        # Engine handles
        self.alteryx_engine = alteryx_engine
        self.generic_engine = generic_engine

        # Output anchor management
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None

        # Default configuration setting
        self.message_timing = None
        self.message_type = None
        self.message_string = None

        # Record management properties
        self.record_info_in = None
        self.record_info_out = None
        self.record_creator = None
        self.record_copier = None
        self.record_cnt = 0
        pass
        
    def display_message(self, message_type, message_string):
        """
        A non-interface method.
        Responsible for outputting the message based on the message type input.
        :param message_type: The type of message the tool writes.
        :param message_string: The type of message that will be displayed.
        :return: Void
        """
        if message_type == 'info':
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.info, message_string)
        elif message_type == 'warning':
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.warning, message_string)
        elif message_type == 'field_conversion_error':
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.field_conversion_error, message_string)
        elif message_type == 'error':
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, message_string)
        pass

    def pi_init(self, str_xml):
        """
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        :return: True
        """
        # Getting the dataName data property from the GUI config
        self.message_timing = Et.fromstring(str_xml).find('messageTiming').text
        self.message_type = Et.fromstring(str_xml).find('messageType').text
        self.message_string = Et.fromstring(str_xml).find('messageString').text

        # Getting the output anchor from Config.xml by the output connection name
        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')

        return True

    def pi_add_incoming_connection(self, str_type, str_name):
        """
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The type of each input connection.
        :param str_name: A unique name for each input connection.
        :return: Self, and a reference to an object.
        """
        return self

    def pi_add_outgoing_connection(self, str_name):
        """
        Called when the Alteryx engine is attempting to add an outgoing data connection.
        :param str_name: A unique name for each output connection.
        :return: Boolean, where True signifies that the connection is accepted.
        """
        return True

    def pi_push_all_records(self, n_record_limit):
        """
        Called when the Alteryx engine when it's expecting the plugin to provide all of its data.
        Only pertinent to tools which have no upstream connections, like the Input tool.
        :param n_record_limit: Set it to <0 for no limit, 0 for no records, and >0 to specify the number of records.
        :return: False, prevent sending all data downstream.
        """
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, 'Missing Incoming Connection')
        return False

    def pi_close(self, b_has_errors):
        """
        Called after all data has finished flowing through all the fields.
        :param b_has_errors: Boolean; set to true to not do the final processing.
        :return: Void
        """
        pass

    def ii_init(self, record_info_in):
        """
        Called when the incoming connection's record metadata is available or has changed, and
        has let the Alteryx engine know what its output will look like.
        :param record_info_in: A RecordInfo object containing the XML representation for the incoming connection's field and sort properties.
        :return: True
        """
        self.record_info_in = record_info_in
        self.record_info_out = self.record_info_in
        self.output_anchor.init(self.record_info_out)
        self.record_creator = self.record_info_out.construct_record_creator()
        self.record_copier = Sdk.RecordCopier(self.record_info_out, self.record_info_in)

        # Map each column of the input to where we want in the output
        for idx in range(len(self.record_info_in)):
            self.record_copier.add(idx, idx)
        self.record_copier.done_adding()
        self.initialized = True
        return True

    def ii_push_record(self, in_record):
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

        if self.record_cnt < 2:
            self.record_cnt += 1  # tracking help for below condition

        if (self.message_timing == 'beforeFirstRecord') and (self.record_cnt == 1):
            self.display_message(self.message_type, self.message_string)

        self.record_creator.reset()
        self.record_copier.copy(self.record_creator, in_record)
        out_record = self.record_creator.finalize_record()
        return self.output_anchor.push_record(out_record)

    def ii_update_progress(self, d_percent):
        """
        Called when the incoming connection is requesting that the plugin update its progress.
        :param d_percent: Value between 0 and 1
        :return: Void
        """
        self.alteryx_engine.output_tool_progress(self.n_tool_id, d_percent)
        self.output_anchor.update_progress(d_percent)
        pass

    def ii_close(self):
        """
        Responsible for outputting the user-selected message before closing the anchor.
        Called when the incoming connection has finished passing all of its records.
        :return: Void
        """
        if self.message_timing == 'afterLastRecord':
            self.display_message(self.message_type, self.message_string)
        self.output_anchor.close()
        pass


