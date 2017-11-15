"""
AyxPlugin (required) has-a IncomingInterface (optional).
Although defining IncomingInterface is optional, the interface methods are needed if an upstream tool exists.
"""

import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et


class AyxPlugin:
    """
    Implements the plugin interface methods, to be utilized by the Alteryx engine to communicate with this plugin.
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
        self.field_selection = None
        self.single_input = None
        self.xml_sort_info = ''
        self.unique_output_anchor = None
        self.dupe_output_anchor = None

    def pi_init(self, str_xml: str):
        """
        Handles input data verification.
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """

        # Getting the user-entered prefixes from the GUI, and the output anchors from the XML file.
        self.field_selection = Et.fromstring(str_xml).find('FieldSelect').text if 'FieldSelect' in str_xml else None
        self.unique_output_anchor = self.output_anchor_mgr.get_output_anchor('Unique')
        self.dupe_output_anchor = self.output_anchor_mgr.get_output_anchor('Duplicate')

        self.build_sort_info("SortInfo", self.field_selection, "asc")  # Building out the <SortInfo> portion.

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection, also pre_sort() is called here.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: The IncomingInterface object(s).
        """

        self.alteryx_engine.pre_sort(str_type, str_name, self.xml_sort_info)
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

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg('Missing Incoming Connection.'))
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed.
        :param b_has_errors: Set to true to not do the final processing.
        """

        # Check to see that the output anchors are closed.
        self.unique_output_anchor.assert_close()
        self.dupe_output_anchor.assert_close()

    def build_sort_info(self, element: str, subelement: property, order: str):
        """
        A non-interface method responsible for building out the proper XML string format for pre_sort.
        :param element: SortInfo or FieldFilterList
        :param subelement: The user selected field
        :param order: Asc or Desc
        """

        # Building the XML string to pass as an argument to pre_sort's sort info parameter.
        root = Et.Element(element)
        sub_element = 'Field field="{0}" order="{1}"' if order != "" else 'Field field="{0}"'
        Et.SubElement(root, sub_element.format(subelement, order))
        xml_string = Et.tostring(root, encoding='utf8', method='xml')
        self.xml_sort_info += xml_string.decode('utf8').replace("<?xml version='1.0' encoding='utf8'?>\n", "")

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
        self.record_info_in = None
        self.record_info_out = None
        self.target_field = None
        self.previous_value = None
        self.records_unique = 0
        self.records_dupe = 0

    def ii_init(self, record_info_in: object) -> bool:
        """
        Handles setting up the outgoing record layout.
        Called to report changes of the incoming connection's record metadata to the Alteryx engine.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: True for success, otherwise False.
        """

        if self.parent.field_selection is None:
            self.parent.alteryx_engine.output_message(self.parent.n_tool_id, Sdk.EngineMessageType.error, self.parent.xmsg('Select a field.'))
            return False

        self.record_info_in = record_info_in  # For later reference.

        # Storing the user selected field to use in ii_push_record, to avoid repeated field lookup.
        self.target_field = self.record_info_in[
            self.record_info_in.get_field_num(self.parent.field_selection)
        ]

        self.record_info_out = self.record_info_in.clone()  # Creating an exact copy of record_info_in.

        # Initialize output anchors
        self.parent.unique_output_anchor.init(self.record_info_out)
        self.parent.dupe_output_anchor.init(self.record_info_out)
        return True

    def ii_push_record(self, in_record: object) -> bool:
        """
        Responsible for pushing records out, upon evaluation of the record data being passed from the sorted field,
        to see if it equals to the previous data.
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: True
        """

        current_value = self.target_field.get_as_string(in_record)

        if current_value == self.previous_value:
            self.parent.dupe_output_anchor.push_record(in_record)
        else:
            self.parent.unique_output_anchor.push_record(in_record)

        self.previous_value = current_value
        return True

    def ii_update_progress(self, d_percent: float):
        """
        Called by the upstream tool to report what percentage of records have been pushed.
        :param d_percent: Value between 0.0 and 1.0.
        """

        # Inform the Alteryx engine of the tool's progress.
        self.parent.alteryx_engine.output_tool_progress(self.parent.n_tool_id, ((d_percent/2)+0.5))

        # Inform the outgoing connections of the tool's progress.
        self.parent.unique_output_anchor.update_progress(d_percent)
        self.parent.dupe_output_anchor.update_progress(d_percent)

    def ii_close(self):
        """
        Responsible for outputting the final count of unique and duplicates as a message, and closing out both anchors.
        Called when the incoming connection has finished passing all of its records.
        """

        self.parent.alteryx_engine.output_message(
            self.parent.n_tool_id,
            Sdk.EngineMessageType.info,
            self.parent.xmsg('{} unique records and {} dupes were found'.format(self.records_unique, self.records_dupe))
        )

        # Close outgoing connections.
        self.parent.unique_output_anchor.close()
        self.parent.dupe_output_anchor.close()
