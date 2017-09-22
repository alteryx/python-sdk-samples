import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et
import ast

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
        self.name = 'OptionalOutputPython_' + str(self.n_tool_id)

        # Engine handles
        self.alteryx_engine = alteryx_engine
        self.generic_engine = generic_engine

        # Default configuration setting
        self.send_downstream = None
        self.create_file = None
        self.file_output_name = None

        # Input and Output anchor management
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None
        self.single_input = None

        # Csv path settings
        self.file = None
        self.temp_folder = self.alteryx_engine.get_init_var('TempPath')
        self.file_output_path = ''

    def pi_init(self, str_xml: str) -> bool:
        """
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """

        try:
            # Getting the dataName data property from the GUI config
            # If statements check if the XML node exists before assigning values to array
            if Et.fromstring(str_xml).find('sendDownstream') is not None:
                self.send_downstream = ast.literal_eval(Et.fromstring(str_xml).find('sendDownstream').text)
            if Et.fromstring(str_xml).find('createFile') is not None:
                self.create_file = ast.literal_eval(Et.fromstring(str_xml).find('createFile').text)
            if Et.fromstring(str_xml).find('fileOutputName') is not None:
                self.file_output_name = Et.fromstring(str_xml).find('fileOutputName').text
        except:
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg('Invalid XML: ' + str_xml))
            raise

        if self.file_output_name is None:
            self.file_output_name = 'data_output'

        if self.create_file:
            self.file_output_path = self.temp_folder + self.file_output_name + '.csv'

        # Prepare output anchor if passing records to downstream tools
        if self.send_downstream:
            # Getting the output anchor from Config.xml by the output connection name
            self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')
        else:            
            self.output_anchor = None
            self.output_anchor_mgr = None

        return True

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: The IncomingInterface object(s).
        """

        self.single_input = IncomingInterface(self)
        return self.single_input

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

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg('Missing Incoming Connection'))
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed.
        :param b_has_errors: Set to true to not do the final processing.
        """

        # Close the csv file after records are appended
        if self.create_file and self.file is not None:
            self.file.close()
        return

    def xmsg(self, msg_string: str) -> str:
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

        # Miscellaneous properties
        self.parent = parent
        self.field_names = None
        self.first_record = True

        # Record management properties
        self.record_info_in = None
        self.record_info_out = None
        return
    
    def ii_init(self, record_info_in: object) -> bool:
        """
        Called when the incoming connection's record metadata is available or has changed, and
        has let the Alteryx engine know what its output will look like.
        :param record_info_in: A RecordInfo object containing the XML representation for the incoming connection's field and sort properties.
        :return: True
        """

        # Storing the argument being passed to the record_info_in parameter
        self.record_info_in = record_info_in

        # Clone the record metadata to outgoing record structure
        self.record_info_out = self.record_info_in.clone()

        if self.parent.send_downstream:
            # Initialize output anchor with outgoing record metadata
            self.parent.output_anchor.init(self.record_info_out)

        if self.parent.create_file:
            # Extracting all the field names from record_info_in to a list in self.field_names
            self.field_names = ','.join([field.name for field in record_info_in])

            # Deleting the newline characters in field names if they exist
            self.field_names = self.field_names.replace('\n', '')

        return True

    def ii_push_record(self, in_record: object) -> bool:
        """
        Responsible for pushing records out, and outputting the user-selected message before the first record push.
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: Will return False if:
          - ii_push_record calling limit has been reached.
          - There is a downstream error.
        """

        if self.parent.create_file:
            # Helper function to extract data by field for each record
            def extract_records(field, in_record):
                if field.get_null(in_record):
                    record = ''
                else:
                    record = field.get_as_string(in_record)
                return record
            # Concatenate the data with commas for csv
            record = ','.join([extract_records(field, in_record) for field in self.record_info_in])
            # Open the file only on first record
            if self.first_record:
                self.parent.file = open(self.parent.file_output_path, 'a', encoding='utf-8')
                self.parent.file.write(self.field_names + '\n' + record)
                self.first_record = False
            else:
                self.parent.file.write('\n' + record)
        
        # End ii_push_record early if not passing data downstream
        if not self.parent.send_downstream:
            self.parent.output_anchor = None
            return True
        # Send records as is to output anchor
        self.parent.output_anchor.push_record(in_record)
        return True

    def ii_update_progress(self, d_percent: float):
        """
        Called when by the upstream tool to report what percentage of records have been pushed.
        :param d_percent: Value between 0.0 and 1.0.
        """
        
        # Inform the Alteryx engine of the tool's progress
        self.parent.alteryx_engine.output_tool_progress(self.parent.n_tool_id, d_percent)
        if self.parent.send_downstream:
            # Inform the outgoing connections of the tool's progress
            self.parent.output_anchor.update_progress(d_percent)
        return

    def ii_close(self):
        """
        Called when the incoming connection has finished passing all of its records.
        """
        if self.parent.create_file:
            # Provide feedback on file creation in results pane
            self.parent.alteryx_engine.output_message(self.parent.n_tool_id, Sdk.EngineMessageType.info, self.parent.file_output_path + self.parent.xmsg(' has been created.'))
        
        if self.parent.send_downstream:
            # Let Alteryx engine know that all records have been sent downstream
            self.parent.output_anchor.output_record_count(True)
            # Close outgoing connections
            self.parent.output_anchor.close()
        return