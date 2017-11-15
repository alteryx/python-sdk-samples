"""
AyxPlugin (required) has-a IncomingInterface (optional).
Although defining IncomingInterface is optional, the interface methods are needed if an upstream tool exists.
"""

import AlteryxPythonSDK as Sdk


class AyxPlugin:
    """
    Implements the plugin interface methods, to be utilized by the Alteryx engine to communicate with a plugin.
    Prefixed with "pi", the Alteryx engine will expect the below five interface methods to be defined.
    """

    def __init__(self, n_tool_id: int, alteryx_engine: object, output_anchor_mgr: object):
        """
       Constructor is called whenever the Alteryx engine wants to instantiate an instance of this plugin.
       :param n_tool_id: The assigned unique identification for a tool instance.
       :param alteryx_engine: Provides an interface into the Alteryx engine.
       :param output_anchor_mgr: A helper that wraps the outgoing connections for a plugin.
       """

        # Default properties
        self.n_tool_id = n_tool_id
        self.alteryx_engine = alteryx_engine
        self.output_anchor_mgr = output_anchor_mgr

        # Custom properties
        self.output_anchor = None
        self.all_inputs = []

    def pi_init(self, str_xml: str):
        """
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """

        # Getting the output anchor from Config.xml by the output connection name.
        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: The IncomingInterface object(s).
        """

        self.all_inputs.append(IncomingInterface(self))
        return self.all_inputs[-1]

    def pi_add_outgoing_connection(self, str_name: str) -> bool:
        """
        Called when the Alteryx engine is attempting to add an outgoing data connection.
        :param str_name: The name of the output connection anchor, defined in the Config.xml file.
        :return: True signifies that the connection is accepted.
        """

        return True

    def pi_push_all_records(self, n_record_limit: int) -> bool:
        """
        Called when a tool has no incoming data connection.
        :param n_record_limit: Set it to <0 for no limit, 0 for no records, and >0 to specify the number of records.
        :return: True for success, False for failure.
        """

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.msg('Missing Incoming Connection'))
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed.
        :param b_has_errors: Set to true to not do the final processing.
        """

        self.output_anchor.assert_close()  # Checks whether connections were properly closed.

    def check_input_complete(self):
        """
        A non-interface helper tasked to verify end of processing for all incoming connections.
        """

        if all([self.all_inputs[an_input].input_complete for an_input in range(len(self.all_inputs))]):
            self.process_output()

    def process_output(self):
        """
        A non-interface helper responsible for pushing records out.
        """

        self.output_anchor.init(self.all_inputs[0].record_info_in)  # Lets the downstream tools know of the outgoing record metadata.

        # Verifying that the first incoming connection's record layout is the same as subsequent incoming connections'
        for an_input in self.all_inputs:
            if not self.all_inputs[0].record_info_in.equal_types(an_input.record_info_in, False):
                self.alteryx_engine.output_message(
                    self.n_tool_id,
                    Sdk.EngineMessageType.error,
                    self.xmsg('Record layout (e.g. size, type) must be the same across all inputs.')
                )
            else:
                for a_record in an_input.record_list:
                    output_record = a_record.finalize_record()  # Asking for a record.
                    self.output_anchor.push_record(output_record)

                    # TODO: The progress update to the downstream tool, based on time elapsed, should go here.

        self.output_anchor.close()  # Close outgoing connections.

    def xmsg(self, msg_string: str):
        """
        A non-interface, non-operational placeholder for the eventual localization of predefined user-facing strings.
        :param msg_string: The user-facing string.
        :return: msg_string
        """

        return msg_string


class IncomingInterface:
    """
    This optional class is returned by pi_add_incoming_connection, and it implements the incoming interface methods, to
    be utilized by the Alteryx engine to communicate with a plugin when processing an incoming connection.
    Prefixed with "ii", the Alteryx engine will expect the below four interface methods to be defined.
    """

    def __init__(self, parent: object):
        """
        Constructor for IncomingInterface.
        :param parent: AyxPlugin
        """

        # Default properties
        self.parent = parent

        # Custom properties
        self.input_complete = False
        self.d_progress_percentage = 0
        self.record_info_in = None
        self.record_copier = None
        self.record_list = []

    def ii_init(self, record_info_in: object) -> bool:
        """
        Although no new records are being added, the prep work here will allow for data state preservation in ii_push_record.
        Called to report changes of the incoming connection's record metadata to the Alteryx engine.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: True for success, otherwise False.
        """

        self.record_copier = Sdk.RecordCopier(record_info_in, record_info_in)

        # Map each column of the input to where we want in the output.
        for index in range(record_info_in.num_fields):
            self.record_copier.add(index, index)

        self.record_copier.done_adding()  # A necessary step to let record copier know that field mappings are done.
        self.record_info_in = record_info_in  # For later reference.
        return True

    def ii_push_record(self, in_record: object) -> bool:
        """
        Preserving the state of the incoming record data, since the reference to a record dies beyond this point.
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        """

        self.record_list.append(self.record_info_in.construct_record_creator())
        self.record_copier.copy(self.record_list[-1], in_record)
        return True

    def ii_update_progress(self, d_percent: float):
        """
        Called by the upstream tool to report what percentage of records have been pushed.
        :param d_percent: Value between 0.0 and 1.0.
        """

        self.d_progress_percentage = d_percent  # Stored for future use for updating the input progress.
        #TODO: self.parent.process_update_input_progress()

    def ii_close(self):
        """
        Called when the incoming connection has finished passing all of its records.
        """

        self.input_complete = True
        self.parent.check_input_complete()
