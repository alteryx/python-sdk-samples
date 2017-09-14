import AlteryxPythonSDK
import xml.etree.ElementTree as ET
import os


class AyxPlugin:
    """
    Implements the plugin interface methods, to be utilized by the Alteryx engine to communicate with a plugin.
    Prefixed with "pi_", the Alteryx engine will expect the below five interface methods to be defined.
    """
    def __init__(self, n_tool_id: int, engine_interface: object, generic_engine: object, output_anchor_mgr: object):
        """
        Acts as the constructor for AyxPlugin.
        :param n_tool_id: The assigned unique identification for a tool instance.
        :param alteryx_engine: Provides an interface into the Alteryx engine.
        :param generic_engine: An abstraction of alteryx_engine.
        :param output_anchor_mgr: A helper that wraps the outgoing connections for a plugin.
        """

        # Miscellaneous variables
        self.n_tool_id = n_tool_id
        self.name = str('PyOutputToolExample_') + str(self.n_tool_id)
        self.closed = False
        self.initialized = False

        # Engine handles
        self.alteryx_engine = engine_interface
        self.generic_engine = generic_engine


        # Custom members
        self.str_file_path = None

    def pi_init(self, str_xml: str):
        """
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """

        # Extracting configuration xml
        root = ET.fromstring(str_xml)

        try: # Finding the dataName property from the Gui.html that matches the child node
            self.str_file_path = root.find('fileOutputPath').text
        except AttributeError:
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, xmsg('Invalid XML: ' + str_xml))
            raise

        self.initialized = True

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
        self.alteryx_engine.output_message(self.n_tool_id, AlteryxPythonSDK.EngineMessageType.error, xmsg('Missing Incoming Connection'))
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed..
        :param b_has_errors: Set to true to not do the final processing.
        """

    @staticmethod
    def xmsg(msg_string: str):
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

        # Record management
        self.record_info_in = None

        # Custom members
        self.field_names = None
        self.testfile = ''
        self.first_record = True

    def ii_init(self, record_info_in):
        """
        Called when the incoming connection's record metadata is available or has changed, and
        has let the Alteryx engine know what its output will look like.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: True for success, otherwise False.
        """

        # Storing the argument being passed to the record_info_in parameter
        self.record_info_in = record_info_in

        # Extracting all the field names from record_info_in to a list
        self.field_names = ','.join([field.name for field in record_info_in])

        # Deleting the newline characters in field names if they exist
        self.field_names = self.field_names.replace('\n', '')

        if (self.parent.str_file_path is None):
            # Outputting Error message if no path is entered
            self.parent.alteryx_engine.output_message(self.parent.n_tool_id, AlteryxPythonSDK.EngineMessageType.error, self.parent.xmsg('Error: Please enter a file path.'))
        elif os.access(self.parent.str_file_path, os.F_OK):
            # Outputting Error message if user specified file already exists
            self.parent.alteryx_engine.output_message(self.parent.n_tool_id, AlteryxPythonSDK.EngineMessageType.error, self.parent.xmsg('Error: ' + self.parent.str_file_path + ' already exists. Please enter a different path.'))

            self.initialized = True
        return True

    def ii_push_record(self, in_record):
        """
         Called when an input record is being sent to the plugin.
         :param in_record: The data for the incoming record.
         :return: True for accepted record.
         """

        # extract_records extracts each record for every field object passed in as a string from record_in
        def extract_records(field, in_record):
            if field.get_null(in_record):
                ret = ''
            elif field.type == 'bool':
                ret = str(field.get_as_bool(in_record))
            elif field.type == 'byte':
                ret = str(field.get_as_int32(in_record))
            elif field.type == 'int32':
                ret = str(field.get_as_int32(in_record))
            elif field.type == 'int64':
                ret = str(field.get_as_int64(in_record))
            elif field.type == 'double':
                ret = str(field.get_as_double(in_record))
            else:
                ret = field.get_as_string(in_record)
            return ret

        # looping through extract_records for each field in record_info_in to get a list of data points for the nth record
        nth_record = ','.join([extract_records(field, in_record) for field in self.record_info_in])

        # Using Python's native file write functionality to write each record to the users specified file path
        self.testfile = open(self.parent.str_file_path, 'a')

        # Writing the field names out on the first record iteration
        if self.first_record:
            self.testfile.write(self.field_names + '\n' + nth_record)
            self.first_record = False
            self.testfile.close()
        else:
            self.testfile.write('\n' + nth_record)
            self.testfile.close()

        return True

    def ii_update_progress(self, d_percent):
        """
         Called when by the upstream tool to report what percentage of records have been pushed.
         :param d_percent: Value between 0.0 and 1.0.
        """

        # Inform the Alteryx engine of the tool's progress.
        self.parent.alteryx_engine.output_tool_progress(self.parent.n_tool_id, d_percent)

    def ii_close(self):
        """
        Called when the incoming connection has finished passing all of its records.
        """

        if (self.parent.str_file_path is not None):
            # Outputting message that the file was written
            message = 'Output: ' + self.parent.str_file_path + ' was written.'
            self.parent.alteryx_engine.output_message(self.parent.n_tool_id, AlteryxPythonSDK.EngineMessageType.info, self.parent.xmsg(message))
