"""
AyxPlugin (required) has-a IncomingInterface (optional).
Although defining IncomingInterface is optional, the interface methods are needed if an upstream tool exists.
"""

import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et
import itertools as it


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
        self.left_input = None
        self.right_input = None
        self.left_prefix = ''
        self.right_prefix = ''
        self.output_anchor = None

    def pi_init(self, str_xml: str):
        """
        Getting the user-entered prefixes from the GUI, and the output anchor from the XML file.
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """

        self.left_prefix = Et.fromstring(str_xml).find('LeftPrefix').text
        self.right_prefix = Et.fromstring(str_xml).find('RightPrefix').text
        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: The IncomingInterface object(s).
        """

        if str_type == 'Left':
            self.left_input = IncomingInterface(self, self.left_prefix)
            return self.left_input
        elif str_type == 'Right':
            self.right_input = IncomingInterface(self, self.right_prefix)
            return self.right_input
        else:
            self.display_error_message('Invalid Input Connection')

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

        self.display_error_message('Missing Incoming Connection')
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed.
        :param b_has_errors: Set to true to not do the final processing.
        """

        self.output_anchor.assert_close()  # Checks whether connections were properly closed.

    def check_input_complete(self):
        """
        A non-interface helper tasked to verify end of processing for both incoming connections.
        """

        if self.right_input is not None and self.left_input is not None:
            if self.right_input.input_complete and self.left_input.input_complete:
                self.process_output()
        else:
            self.display_error_message('Both left and right inputs must have connections')

    @staticmethod
    def init_record_info_out(child: object, record_info_out: object):
        """
        A non-interface helper for process_output() that handles building out the layout for record_info_out.
        :param child: An incoming connection.
        :param record_info_out: The outgoing record info object.
        :return: Updated initialization of record_info_out.
        """

        record_info_out.init_from_xml(
            child.record_info_in.get_record_xml_meta_data(True),
            child.rename_prefix + '_' if child.rename_prefix is not None else ''
        )
        return record_info_out

    @staticmethod
    def swap_outgoing_order(left_input: object, right_input: object):
        """
        A non-interface helper for process_output() that assigns the mapping order based on number of records.
        :param left_input: the object from the left incoming connection
        :param right_input: the object from the right incoming connection
        :return: New names for the incoming connections.
        """

        min_n_records = min(len(left_input.record_list), len(right_input.record_list))
        max_n_records = max(len(left_input.record_list), len(right_input.record_list))

        # Having the shortest list be the first to output, so set_dest_to_null is applied only for the first copy,\
        # when dealing with an uneven record pair. This swap process will eventually be replaced in subsequent releases.
        if min_n_records != max_n_records:
            first_half_output = left_input if min_n_records == len(left_input.record_list) else right_input
            second_half_output = right_input if first_half_output == left_input else left_input
        else:
            first_half_output = left_input
            second_half_output = right_input
        return first_half_output, second_half_output

    @staticmethod
    def setup_record_copier(child: object, record_info_out: object, start_index: int):
        """
        A non-interface helper for process_output() that maps the appropriate fields to their designated positions.
        :param child: Incoming connection object.
        :param record_info_out: The outgoing record layout.
        :param start_index: The starting field position of an incoming connection object.
        :return: The starting field position for the next incoming connection object.
        """

        child.record_copier = Sdk.RecordCopier(record_info_out, child.record_info_in)
        for index in range(child.record_info_in.num_fields):
            child.record_copier.add(start_index + index, index)
        child.record_copier.done_adding()
        return child.record_info_in.num_fields

    def process_output(self):
        """
        A non-interface method responsible for pushing the records based on the joined record layout.
        """

        # Determining the mapping order based on length of the incoming data streams.
        first_half_output, second_half_output = self.swap_outgoing_order(self.left_input, self.right_input)

        # Having the helper initialize the RecordInfo object for the outgoing stream.
        record_info_out = self.init_record_info_out(first_half_output, Sdk.RecordInfo(self.alteryx_engine))
        record_info_out = self.init_record_info_out(second_half_output, record_info_out)

        self.output_anchor.init(record_info_out)  # Lets the downstream tools know of the outgoing record metadata.

        # Having the helper function handle the field index mapping from both incoming streams, into record_info_out.
        start_index = self.setup_record_copier(first_half_output, record_info_out, 0)
        self.setup_record_copier(second_half_output, record_info_out, start_index)

        record_creator = record_info_out.construct_record_creator()  # Creating a new record_creator for the joined records.

        for input_pair in it.zip_longest(first_half_output.record_list, second_half_output.record_list):

            # Copying the record into the record creator. NULL values will be used to fill for the difference.
            if input_pair[0] is not None:
                first_half_output.record_copier.copy(record_creator, input_pair[0].finalize_record())
            else:
                first_half_output.record_copier.set_dest_to_null(record_creator)
            second_half_output.record_copier.copy(record_creator, input_pair[1].finalize_record())

            # Asking for a record to push downstream, then resetting the record to prevent unexpected results.
            output_record = record_creator.finalize_record()
            self.output_anchor.push_record(output_record, False)
            record_creator.reset()

            #TODO: The progress update to the downstream tool, based on time elapsed, should go here.

        self.output_anchor.close()  # Close outgoing connections.

    def process_update_input_progress(self):
        """
        A non-interface helper to update the incoming progress based on records received from the input streams.
        """

        if self.right_input is not None and self.left_input is not None:
            # We're assuming receiving the input data accounts for half the progress.
            input_percent = (self.right_input.d_progress_percentage + self.left_input.d_progress_percentage) / 2
            self.alteryx_engine.output_tool_progress(self.n_tool_id, input_percent / 2)

    def display_error_message(self, msg_string: str):
        """
        A non-interface helper function, responsible for outputting error messages.
        :param msg_string: The error message string.
        """

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg(msg_string))

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

    def __init__(self, parent: object, rename_prefix: str):
        """
        Constructor for IncomingInterface.
        :param parent: AyxPlugin
        :param rename_prefix: The prefix string entered by the user, if any.
        """

        # Default properties
        self.parent = parent
        self.rename_prefix = rename_prefix

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
        :return: False if method calling limit (record_cnt) is hit.
        """

        self.record_list.append(self.record_info_in.construct_record_creator())
        self.record_copier.copy(self.record_list[-1], in_record)
        return True

    def ii_update_progress(self, d_percent: float):
        """
        Called by the upstream tool to report what percentage of records have been pushed.
        :param d_percent: Value between 0.0 and 1.0.
        """

        self.d_progress_percentage = d_percent
        self.parent.process_update_input_progress()

    def ii_close(self):
        """
        Called when the incoming connection has finished passing all of its records.
        """

        self.input_complete = True
        self.parent.check_input_complete()
