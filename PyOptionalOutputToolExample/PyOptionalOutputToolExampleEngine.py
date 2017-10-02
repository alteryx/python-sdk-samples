import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et
import csv


class AyxPlugin:
    """
    Implements the plugin interface methods, to be utilized by the Alteryx engine to communicate with a plugin.
    Prefixed with "pi_", the Alteryx engine will expect the below five interface methods to be defined.
    """

    def __init__(self, n_tool_id: int, alteryx_engine: object, output_anchor_mgr: object):
        """
        Acts as the constructor for AyxPlugin.
        :param n_tool_id: The assigned unique identification for a tool instance.
        :param alteryx_engine: Provides an interface into the Alteryx engine.
        :param output_anchor_mgr: A helper that wraps the outgoing connections for a plugin.
        """

        # Miscellaneous properties
        self.n_tool_id = n_tool_id
        self.name = 'OptionalOutputPython_' + str(self.n_tool_id)

        # Engine handle
        self.alteryx_engine = alteryx_engine

        # Default configuration setting
        self.send_downstream = self.create_file = False
        self.single_input = None
        self.file_temp_path = alteryx_engine.get_init_var(n_tool_id, 'TempPath') + 'data_output.csv'

        # Anchor management
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None

    def pi_init(self, str_xml: str):
        """
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """

        # Getting the dataName data property from the Gui.html
        self.send_downstream = True if Et.fromstring(str_xml).find('sendDownstream').text == 'True' else False
        self.create_file = True if Et.fromstring(str_xml).find('createFile').text == 'True' else False

        # Getting the output anchors from Config.xml
        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')

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
        pass

    def xmsg(self, msg_string: str) -> str:
        """
        A non-interface, non-operational placeholder for the eventual localization of predefined user-facing strings.
        :param msg_string: The user-facing string.
        :return: msg_string
        """

        return msg_string

    @staticmethod
    def write_lists_to_csv(file_temp_path: str, field_lists: list):
        """
        A non-interface, helper function that handles writing to csv and clearing the list elements.
        :param file_temp_path: The default temp path and file name.
        :param field_lists: The data for all fields.
        """
        with open(file_temp_path, 'a', encoding='utf-8', newline='') as output_file:
            csv.writer(output_file, delimiter=',').writerows(zip(*field_lists))
        for sublist in field_lists:
            del sublist[:]


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
        :param file_temp_path: The default temp path and file name.
        """

        # Miscellaneous properties
        self.parent = parent
        self.field_lists = []
        self.counter = 0
        self.record_info_in = None

    def ii_init(self, record_info_in: object) -> bool:
        """
        Initiating output anchor and the field lists by storing the field name as the first element, respectively.
        Called when the incoming connection's record metadata is available or has changed, and
        has let the Alteryx engine know what its output will look like.
        :param record_info_in: A RecordInfo object containing the XML representation for the incoming connection's field and sort properties.
        :return: True
        """

        # Storing the argument being passed to the record_info_in parameter
        self.record_info_in = record_info_in

        if self.parent.send_downstream:
            # Clone the record metadata to outgoing record structure
            self.parent.output_anchor.init(self.record_info_in.clone())

        if self.parent.create_file:
            # Storing field names
            for field in range(record_info_in.num_fields):
                self.field_lists.append([record_info_in[field].name])

        return True

    def ii_push_record(self, in_record: object) -> bool:
        """
        Responsible for pushing records out and/or writing to file.
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: True if user chose to pass data downstream or user chose to write data to file, False if neither.
        """

        if not self.parent.send_downstream and not self.parent.create_file:
            return False

        if self.parent.send_downstream:
            # Push the records out
            self.parent.output_anchor.push_record(in_record)

        if self.parent.create_file:
            self.counter += 1

            # Storing the string data of in_record
            for field in range(self.record_info_in.num_fields):
                in_value = self.record_info_in[field].get_as_string(in_record)
                self.field_lists[field].append(in_value) if in_value is not None else self.field_lists[field].append('')

            # Writing when chunk mark is met
            if self.counter == 1000000:
                self.parent.write_lists_to_csv(self.parent.file_temp_path, self.field_lists)
                self.counter = 0  # Reset counter

        return True

    def ii_update_progress(self, d_percent: float):
        """
        Called when by the upstream tool to report what percentage of records have been pushed.
        :param d_percent: Value between 0.0 and 1.0.
        """

        # Inform the Alteryx engine of the tool's progress.
        self.parent.alteryx_engine.output_tool_progress(self.parent.n_tool_id, d_percent)

        # Inform the outgoing connections of the tool's progress.
        self.parent.output_anchor.update_progress(d_percent)

    def ii_close(self):
        """
        Responsible for writing any remaining data below chunk threshold if necessary, and providing link to file temp path.
        Called when the incoming connection has finished passing all of its records.
        """

        if self.parent.create_file:
            if len(self.field_lists[0]) > 1:  # First element for each list will always be the field names.
                self.parent.write_lists_to_csv(self.parent.file_temp_path, self.field_lists)
            # Generates message with link to file
            self.parent.alteryx_engine.output_message(
                self.parent.n_tool_id,
                Sdk.Status.file_output,
                self.parent.xmsg(self.parent.file_temp_path + "|" + self.parent.file_temp_path + " was created.")
            )

        # Close outgoing connections.
        self.parent.output_anchor.close()
