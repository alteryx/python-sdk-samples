import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et
import itertools as it
import time


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
        self.left_input = self.right_input = None
        self.left_prefix = self.right_prefix = ''
        self.record_info_out = None
        self.record_creator = None
        self.output_anchor = None

    def pi_init(self, str_xml: str):
        """
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """

        # Getting the text from the below attributes
        self.left_prefix = Et.fromstring(str_xml).find('LeftPrefix').text
        self.right_prefix = Et.fromstring(str_xml).find('RightPrefix').text

        # Getting the output anchor from Config.xml by the output connection name
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
            self.left_input = IncomingInterface(self)
            return self.left_input
        elif str_type == 'Right':
            self.right_input = IncomingInterface(self)
            return self.right_input
        else:
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg('Invalid Input Connection'))

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

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg('Misssing Incoming Connection'))
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed.
        :param b_has_errors: Set to true to not do the final processing.
        """

        # Checks whether connections were properly closed.
        self.output_anchor.assert_close()

    def check_input_complete(self):
        """
        Helper to verify end of processing for both incoming connections.
        """
        if self.right_input is not None and self.left_input is not None:
            if self.right_input.input_complete and self.left_input.input_complete:
                self.process_output()
        else:
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error,
                                               self.xmsg('Both left and right inputs must have connections'))

    def setup_record_copier(self, child: object, start_index: int):
        """
        Prepares the outgoing stream's meta data by copying the incoming meta data from both input streams
        :param child: One of the incoming connection objects.
        :param start_index: The starting field position of one of the incoming connection objects.
        :return: The starting field position for the next incoming connection object.
        """

        child.record_copier = Sdk.RecordCopier(self.record_info_out, child.record_info_in)
        for index in range(child.record_info_in.num_fields):
            child.record_copier.add(start_index + index, index)
        child.record_copier.done_adding()
        return child.record_info_in.num_fields

    def process_output(self):
        """
        Responsible for creating the record_info_out object, mapping the records, pushing the records out, and updating
        output's progress and also what the percentage progress displayed in designer.
        """

        # ====================================================
        # Setting up variables to track output progress ======
        total_records = max(len(self.left_input.record_list),len(self.right_input.record_list))
        num_records_output = 0
        # ====================================================

        # Building the RecordInfo object for the outgoing stream.
        self.record_info_out = Sdk.RecordInfo(self.alteryx_engine)
        self.record_info_out.init_from_xml(self.left_input.record_info_in.get_record_xml_meta_data(True),
                                           self.left_prefix + '_' if self.left_prefix is not None else '')
        self.record_info_out.init_from_xml(self.right_input.record_info_in.get_record_xml_meta_data(),
                                           self.right_prefix + '_' if self.right_prefix is not None else '')

        # Lets the downstream tools know what the outgoing record metadata will look like, based on record_info_out.
        self.output_anchor.init(self.record_info_out)

        # Helper function to handle the field index mapping from both incoming streams, into record_info_out.
        start_index = self.setup_record_copier(self.left_input, 0)
        self.setup_record_copier(self.right_input, start_index)

        # Creating a new, empty record creator based on record_info_out's record layout.
        self.record_creator = self.record_info_out.construct_record_creator()

        # Having the shortest list be the first to output, so set_dest_to_null is applied only for the first copy,
        # when dealing with an uneven record pair. This swap process will eventually be replaced in subsequent releases.
        if len(self.left_input.record_list) == min(len(self.left_input.record_list), len(self.right_input.record_list)):
            go_first_input = self.left_input
        else:
            go_first_input = self.right_input
        if go_first_input == self.left_input:
            go_second_input = self.right_input
        else:
            go_second_input = self.left_input

        start_process_time = time.clock()

        # Using zip_longest() to allow for uneven incoming streams. Returns an iterator.
        for input_pair in it.zip_longest(go_first_input.record_list, go_second_input.record_list):

            # Resets the capacity for variable-length data in this record to 0 bytes, to prevent unexpected results.
            self.record_creator.reset(0)

            # Copying the reference to a record into the record creator. Field mappings must match both field layouts.
            # NULL values will be used to fill for the uneven number of records, using set_dest_to_null()
            if input_pair[0] is not None:
                go_first_input.record_copier.copy(self.record_creator, input_pair[0].finalize_record())
            else:
                go_first_input.record_copier.set_dest_to_null(self.record_creator)
            go_second_input.record_copier.copy(self.record_creator, input_pair[1].finalize_record())

            # Return the reference to a record containing the data for the record
            output_records = self.record_creator.finalize_record()

            # Push the record downstream
            self.output_anchor.push_record(output_records)

            # ====================================================
            # Update output progress =============================
            num_records_output += 1
            output_progress = num_records_output/total_records

            # Only update once per the below seconds
            if round((time.clock() - start_process_time), 1) % 2.5 == 0:
                self.alteryx_engine.output_tool_progress(self.n_tool_id, ((1 + output_progress) / 2))

            self.output_anchor.update_progress(output_progress)
            # ====================================================

        # Close outgoing connections.
        self.output_anchor.close()

    def process_update_input_progress(self):
        """
        Update progress based on records received from the inputs.
        """

        if self.right_input is not None and self.left_input is not None:
            # We're assuming receiving the input data accounts for half the progress
            input_percent = (self.right_input.d_progress_percentage + self.left_input.d_progress_percentage) / 2
            self.alteryx_engine.output_tool_progress(self.n_tool_id, input_percent / 2 )

    def xmsg(self, msg_string: str):
        """
        A non-interface, non-operational placeholder for the eventual localization of predefined user-facing strings.
        :param msg_string: The user-facing string.
        :return: msg_string
        """

        return msg_string


class IncomingInterface:
    """
    This class is returned by pi_add_incoming_connection, and it implements the incoming interface methods, to be\
    utilized by the Alteryx engine to communicate with a plugin when processing an incoming connection.
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
        self.record_list = []
        self.record_copier = None

    def ii_init(self, record_info_in: object) -> bool:
        """
        Called to report changes of the incoming connection's record metadata to the Alteryx engine.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: True for success, otherwise False.
        """

        # Instantiate a new instance of the RecordCopier class.
        self.record_copier = Sdk.RecordCopier(record_info_in, record_info_in)

        # Map each column of the input to where we want in the output.
        for index in range(record_info_in.num_fields):
            # Adding a field index mapping.
            self.record_copier.add(index, index)

        # Let record copier know that all field mappings have been added.
        self.record_copier.done_adding()

        # Storing for later use
        self.record_info_in = record_info_in

        return True

    def ii_push_record(self, in_record: object) -> bool:
        """
        Appending the incoming record for later use.
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
