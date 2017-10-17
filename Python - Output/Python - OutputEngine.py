import AlteryxPythonSDK
import xml.etree.ElementTree as ET
import os
import csv


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

        # Custom properties
        self.str_file_path = ''
        self.is_valid = False
        self.single_input = None

    def pi_init(self, str_xml: str):
        """
        Handles input data verification.
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """

        # Extracting configuration XML
        root = ET.fromstring(str_xml)

        # Extract file path from the XML node and store it
        self.str_file_path = root.find('fileOutputPath').text if root.find('fileOutputPath').text else ''

        # If there is no error message string returned, set flag to True if validation passes
        error_msg = self.msg_str(self.str_file_path)
        if error_msg != '':
            self.alteryx_engine.output_message(self.n_tool_id, AlteryxPythonSDK.EngineMessageType.error, self.xmsg(error_msg))
        else:
            self.is_valid = True

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: The IncomingInterface object.
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
        Called when a tool has no incoming data connection.
        :param n_record_limit: Set it to <0 for no limit, 0 for no records, and >0 to specify the number of records.
        :return: True for success, False for failure.
        """

        self.alteryx_engine.output_message(self.n_tool_id, AlteryxPythonSDK.EngineMessageType.error, self.xmsg('Missing Incoming Connection'))
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed.
        :param b_has_errors: Set to true to not do the final processing.
        """

        pass

    def xmsg(self, msg_string: str) -> str:
        """
        A non-interface, non-operational placeholder for the eventual localization of predefined user-facing strings.
        :param msg_string: The user-facing string.
        :return: msg_string
        """

        return msg_string

    @staticmethod
    def write_lists_to_csv(file_path: str, field_lists: list):
        """
        A non-interface, helper function that handles writing to csv and clearing the list elements.
        :param file_path: The file path and file name input by user.
        :param field_lists: The data for all fields.
        """

        with open(file_path, 'a', encoding='utf-8', newline='') as output_file:
            csv.writer(output_file, delimiter=',').writerows(zip(*field_lists))
        for sublist in field_lists:
            del sublist[:]

    @staticmethod
    def msg_str(file_path: str):
        """
        A non-interface, helper function that handles validating the file path input.
        :param file_path: The file path and file name input by user.
        """

        msg_str = ''
        if os.access(file_path, os.F_OK):  # Must be a new file
            msg_str = file_path + ' already exists. Enter a different path.'
        elif len(file_path) > 259:  # Check length of filename
            msg_str = 'Maximum path length is 259'
        elif any((char in set('/;?*"<>|')) for char in file_path):  # Check for special characters in filename
            msg_str = 'These characters are not allowed in the filename: /;?*"<>|'
        elif len(file_path) == 0:  # Show error if filename is blank
            msg_str = 'Enter a filename'
        elif not file_path.endswith('.csv'): # File extension must be .csv
            msg_str = 'File extension must be .csv'
        return msg_str


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

        # Custom members
        self.record_info_in = None
        self.field_lists = []
        self.counter = 0

    def ii_init(self, record_info_in: object) -> bool:
        """
        Handles the storage of the incoming metadata for later use.
        Called to report changes of the incoming connection's record metadata to the Alteryx engine.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: True for success, otherwise False.
        """

        # Storing the incoming connection layout.
        self.record_info_in = record_info_in

        # Storing the field names.
        for field in range(record_info_in.num_fields):
            self.field_lists.append([record_info_in[field].name])

        return True

    def ii_push_record(self, in_record: object) -> bool:
        """
        Responsible for writing the data to csv in chunks.
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: False if file path string is invalid, otherwise True.
        """

        self.counter += 1

        # Do not process records if file path is invalid
        if not self.parent.is_valid:
            return False

        # Storing the string data of in_record
        for field in range(self.record_info_in.num_fields):
            in_value = self.record_info_in[field].get_as_string(in_record)
            self.field_lists[field].append(in_value) if in_value is not None else self.field_lists[field].append('')

        # Writing when chunk mark is met
        if self.counter == 1000000:
            self.parent.write_lists_to_csv(self.parent.str_file_path, self.field_lists)
            self.counter = 0  # Reset counter

        return True

    def ii_update_progress(self, d_percent: float):
        """
         Called by the upstream tool to report what percentage of records have been pushed.
         :param d_percent: Value between 0.0 and 1.0.
        """

        # Inform the Alteryx engine of the tool's progress
        self.parent.alteryx_engine.output_tool_progress(self.parent.n_tool_id, d_percent)

    def ii_close(self):
        """
        Handles writing out any residual data out.
        Called when the incoming connection has finished passing all of its records.
        """

        # Write the last chunk
        # First element for each list will always be the field names.
        if len(self.field_lists[0]) > 1 and self.parent.is_valid:
            self.parent.write_lists_to_csv(self.parent.str_file_path, self.field_lists)

        # Outputting message that the file was written
        if len(self.parent.str_file_path) > 0 and self.parent.is_valid:
            self.parent.alteryx_engine.output_message(self.parent.n_tool_id, AlteryxPythonSDK.Status.file_output, self.parent.xmsg(self.parent.str_file_path + "|" + self.parent.str_file_path + " was created."))