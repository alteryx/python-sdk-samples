import AlteryxPythonSDK
import xml.etree.ElementTree as ET
import re


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
        self.message_type = None
        self.output_anchor = None
        self.record_info_out= None
        self.record_creator = None
        self.all_inputs = []
        self.unique_field_names = []

    def pi_init(self, str_xml: str):
        """
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """

        # Getting the dataName data property from the GUI config
        self.message_type = ET.fromstring(str_xml).find('messageType').text if 'messageType' in str_xml else None

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

        # Storing each IncomingInterface object
        self.all_inputs.append(IncomingInterface(self, str_type, str_name))
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

        self.alteryx_engine.output_message(self.n_tool_id, AlteryxPythonSDK.EngineMessageType.error, 'Missing Incoming Connection')
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
        A non-interface method that checks to see if all connections have been initialized.
        """

        if all([self.all_inputs[idx].input_complete for idx in range(len(self.all_inputs))]):
            self.record_processor()

    def setup_record_copier(self, nth_input: object):
        """
        A non-interface method that prepares the outgoing stream's meta data by copying the incoming meta data from each input stream.
        :param nth_input: One of the incoming connection objects.
        """

        # Setup the record_copier for copying data from the input records into our new output records.
        nth_input.record_copier = AlteryxPythonSDK.RecordCopier(self.record_info_out, nth_input.record_info_in)

        # Mapping each field of the input to where we want it to be in the output.
        for input_idx in range(nth_input.record_info_in.num_fields):
            output_idx = self.unique_field_names.index(nth_input.record_info_in[input_idx].name)
            nth_input.record_copier.add(output_idx, input_idx)
        nth_input.record_copier.done_adding()

    def record_processor(self):
        """
        A non-interface method that is responsible for creating the record_info_out object, mapping the records,pushing\
        the records out, and updating output's progress and also what the percentage progress displayed in designer.
        """

        # Sorts by connection name
        self.all_inputs.sort(key=lambda inputObj: int(re.findall('[\d+]' , inputObj.name)[-1]))

        # Constructing a new RecordInfo object that will contain the metadata of the fields we want to output.
        self.record_info_out= AlteryxPythonSDK.RecordInfo(self.alteryx_engine)

        new_fields = []

        # Going through each input connection to extract the field information.
        for nth_input in self.all_inputs:
            # Only runs on the first input
            if (len(self.unique_field_names) == 0):

                # Extracting the XML metadata from the first connection and initializing the record layout in record_info_out.
                self.record_info_out.init_from_xml(nth_input.record_info_in.get_record_xml_meta_data())

                # Using unique_field_names and other_input_field_names to keep track of all unique fields from each connection.
                self.unique_field_names = [field.name for field in self.record_info_out]

            else:
                # Storing all field names of other inputs.
                other_input_field_names = [field.name for field in nth_input.record_info_in]

                # Extracting the names of any new and unique names into a new_fields.
                new_fields = [items for items in other_input_field_names if items not in self.unique_field_names]

                # Outputting appropriate messaging depending on user selection.
                if self.message_type == 'error':
                    if len(new_fields) != 0:
                        for item in new_fields:
                            self.alteryx_engine.output_message(self.n_tool_id, AlteryxPythonSDK.EngineMessageType.error,'The field:' + '"' + item + '"' + 'is not present in in the initial input schema')

                # Extracting the field metadata of the unique fields so they can be added to record_info_out.
                new_field_obj = [nth_input.record_info_in.get_field_by_name(name) for name in new_fields]
                for field in new_field_obj:
                    self.record_info_out.add_field(
                        field.name # name
                        , field.type  # type (string, int, etc.)
                        , field.size  # size (only relevant for string, blob, and spatial)
                        , field.scale  # scale (if you don't know what this means, just use 0)
                        , field.source  # source metadata
                        , field.description  # description metadata
                    )

                self.unique_field_names += new_fields

            # Setup the record_copier for copying data from this input record into our new output records.
            self.setup_record_copier(nth_input)

        # Tell the downstream tools what our records will look like.
        self.output_anchor.init(self.record_info_out)

        # Create the helper for constructing records to pass downstream.
        self.record_creator = self.record_info_out.construct_record_creator()

        # Checks to make sure when the user selects error and new fields are present that records are not processed.
        if new_fields and self.message_type == 'error':
            # Close outgoing connections.
            self.output_anchor.close()

        else:
            # Copy the latest record from each input into the outgoing stream.
            for nth_input in self.all_inputs:
                for record in nth_input.record_list:
                    # Resets the capacity for variable-length data in this record to 0 bytes (default if no number specified.
                    self.record_creator.reset()

                    # Setting all the fields to null in record_creator so that if records don't appear for certain fields they will show up as null records.
                    for field in self.record_info_out:
                        field.set_null(self.record_creator)

                    #  Copying the individual records to record creator.
                    nth_input.record_copier.copy(self.record_creator, record.finalize_record())

                    # Pushing the final record to the output anchor.
                    self.output_anchor.push_record(self.record_creator.finalize_record())

            # Close outgoing connections.
            self.output_anchor.close()

    def process_update_input_progress(self):
        """
        A non-interface method that updates progress based on records received from the inputs.
        """

        # Assuming that each input initialized is a percentage of the total records getting processed
        input_percentage = sum([nth_input.d_progress_percentage for nth_input in self.all_inputs])/len(self.all_inputs)

        self.alteryx_engine.output_tool_progress(self.n_tool_id, input_percentage)

    def xmsg(self, msg_string: str) -> str:
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

    def __init__(self, parent: object, type_: str, name: str):
        """
        Constructor for IncomingInterface.
        :param parent: AyxPlugin
        """

        # Default properties
        self.parent = parent
        self.type = type_
        self.name = name

        # Custom properties
        self.input_complete = False
        self.d_progress_percentage = 0
        self.record_info_in = None
        self.record_copier = None
        self.record_list = []

    def ii_init(self, record_info_in: object) -> bool:
        """
        Called to report changes of the incoming connection's record metadata to the Alteryx engine.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: True for success, otherwise False.
        """

        self.record_copier = AlteryxPythonSDK.RecordCopier(record_info_in, record_info_in)

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
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: True for accepted record.
        """

        # Appending a new RecordCreator Object to a stored list
        self.record_list.append(self.record_info_in.construct_record_creator())

        # Copying the Record Reference object of the incoming record to the newly created RecordCreator Object
        self.record_copier.copy(self.record_list[-1], in_record)
        return True

    def ii_update_progress(self, d_percent: int):
        """
          Called by the upstream tool to report what percentage of records have been pushed.
          :param d_percent: Value between 0.0 and 1.0.
         """

        # Inform the alteryx engine of the tool's progress.
        self.d_progress_percentage = d_percent
        self.parent.process_update_input_progress()

    def ii_close(self):
        """
        Called when the incoming connection has finished passing all of its records.
        """
        self.input_complete = True
        self.parent.check_input_complete()