import AlteryxPythonSDK
import xml.etree.ElementTree as ET
import re

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

        # Miscellaneous variables
        self.n_tool_id = n_tool_id
        self.name = str('PySingleMultiInput') + str(self.n_tool_id)
        self.closed = False

        # Engine handles
        self.alteryx_engine = alteryx_engine
        self.generic_engine = generic_engine

        # Output anchor management
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None

        # Record management
        self.record_info_out= None
        self.record_creator = None

        # Custom members
        self.inputs = []
        self.unique_field_names = []

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

        # Storing each IncomingInterface object
        self.inputs.append(IncomingInterface(self, str_type, str_name))
        return self.inputs[-1]

    def pi_add_outgoing_connection(self, str_name) -> bool:
        """
          Called when the Alteryx engine is attempting to add an outgoing data connection.
          :param str_name: The name of the output connection anchor, defined in the Config.xml file.
          :return: True signifies that the connection is accepted.
          """
        return True

    def pi_push_all_records(self, n_record_limit) -> bool:
        """
        Called by the Alteryx engine for tools that have no incoming connection connected.
        Only pertinent to tools which have no upstream connections, like the Input tool.
        :param n_record_limit: Set it to <0 for no limit, 0 for no records, and >0 to specify the number of records.
        :return: True for success, False for failure.
        """

        self.alteryx_engine.output_message(self.n_tool_id, AlteryxPythonSDK.EngineMessageType.error, 'Missing Incoming Connection')
        return False

    def pi_close(self, b_has_errors):
        """
        Called after all records have been processed..
        :param b_has_errors: Set to true to not do the final processing.
        """

        # Checks whether connections were properly closed.
        self.output_anchor.assert_close()

    def xmsg(self, msg_string: str) -> str:
        """
        A non-interface, non-operational placeholder for the eventual localization of predefined user-facing strings.
        :param msg_string: The user-facing string.
        :return: msg_string
        """

        return msg_string

    def setup_record_copier(self, input_):
        """
        Prepares the outgoing stream's meta data by copying the incoming meta data from each input stream
        :param input_: One of the incoming connection objects.
        """

        # Setup the record_copier for copying data from the input records into our new output records.
        input_.record_copier = AlteryxPythonSDK.RecordCopier(self.record_info_out, input_.record_info_in)
        # Mapping each field of the input to where we want it to be in the output.
        for input_idx in range(input_.record_info_in.num_fields):
            output_idx = self.unique_field_names.index(input_.record_info_in[input_idx].name)
            input_.record_copier.add(output_idx, input_idx)
        input_.record_copier.done_adding()

    def record_processor(self):
        """
        Responsible for creating the record_info_out object, mapping the records, pushing the records out, and updating
        output's progress and also what the percentage progress displayed in designer.
        """

        # Sorts by connection name
        self.inputs.sort(key=lambda inputObj: int(re.findall('[\d+]' , inputObj.name)[-1]))

        # Constructing a new RecordInfo object that will contain the metadata of the fields we want to output.
        self.record_info_out= AlteryxPythonSDK.RecordInfo(self.generic_engine)

        for input_ in self.inputs:
            if (len(self.unique_field_names) == 0):

                # Extracting the XML metadata from the first connection and initializing the record layout in record_info_out.
                self.record_info_out.init_from_xml(input_.record_info_in.get_record_xml_meta_data())

                # Using unique_field_names and other_input_field_names to keep track of all unique fields from each connection.
                self.unique_field_names = [field.name for field in self.record_info_out]

            else:
                # Storing all field names of other inputs.
                other_input_field_names = [field.name for field in input_.record_info_in]

                # Extracting the names of any new and unique names into a new_fields.
                new_fields = [items for items in other_input_field_names if items not in self.unique_field_names]

                # Extracting the field metadata of the unique fields so they can be added to record_info_out.
                new_field_obj = [input_.record_info_in.get_field_by_name(name) for name in new_fields]
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
            self.setup_record_copier(input_)

        # Tell the downstream tools what our records will look like.
        self.output_anchor.init(self.record_info_out)

        # Create the helper for constructing records to pass downstream.
        self.record_creator = self.record_info_out.construct_record_creator()

        # Copy the latest record from each input into the outgoing stream.
        for input_ in self.inputs:
            for record in input_.record_list:
                # Resets the capacity for variable-length data in this record to 0 bytes, to prevent unexpected results.
                self.record_creator.reset()

                # Setting all the fields to null in record_creator so that if records don't appear for certain fields they will show up as null records.
                for field in self.record_info_out:
                    field.set_null(self.record_creator)

                #  Copying the individual records to record creator.
                input_.record_copier.copy(self.record_creator, record.finalize_record())

                # Pushing the final record to the output anchor.
                self.output_anchor.push_record(self.record_creator.finalize_record())

        # Close outgoing connections.
        self.output_anchor.close()

    def process_update_input_progress(self):
        """
        Update progress based on records received from the inputs.
        """

        # Assuming that each input initialized is a percentage of the total records getting processed
        input_percentage = sum([input_.d_progress_percentage for input_ in self.inputs])/len(self.inputs)

        self.alteryx_engine.output_tool_progress(self.n_tool_id, input_percentage)

class IncomingInterface:
    """
    This class is returned by pi_add_incoming_connection, and it implements the incoming interface methods, to be
    utilized by the Alteryx engine to communicate with a plugin when processing an incoming connection.
    Prefixed with "ii_", the Alteryx engine will expect the below four interface methods to be defined.
    """

    def __init__(self, parent, type_, name):
        """
        Acts as the constructor for IncomingInterface. Instance variable initializations should happen here for PEP8 compliance.
        :param parent: AyxPlugin
        """

        # Miscellaneous variables
        self.parent = parent
        self.type = type_
        self.name = name
        self.closed = False
        self.input_complete = False
        self.d_progress_percentage = 0

        # Record management
        self.record_info_in = None
        self.record_copier = None
        self.in_record = None
        self.record_list = []

    def ii_init(self, record_info_in) -> bool:
        """
        Called when the incoming connection's record metadata is available or has changed, and
        has let the Alteryx engine know what its output will look like.
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

    def ii_push_record(self, in_record) -> bool:
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

    def ii_update_progress(self, d_percent):
        """
          Called when by the upstream tool to report what percentage of records have been pushed.
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

        # Checks to see if all connections metainfo and record data has been initialized before deploying the field mapping and record handling method
        if all([self.parent.inputs[idx].input_complete for idx in range(len(self.parent.inputs))]):
            self.parent.record_processor()
