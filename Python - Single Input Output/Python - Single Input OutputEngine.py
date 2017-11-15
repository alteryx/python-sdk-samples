"""
AyxPlugin (required) has-a IncomingInterface (optional).
Although defining IncomingInterface is optional, the interface methods are needed if an upstream tool exists.
"""

import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et


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
        self.single_input = None
        self.n_record_select = None
        self.xml_sort_info = ''
        self.do_sort = False
        self.field_selection = None
        self.output_anchor = None

    def pi_init(self, str_xml: str):
        """
        Handles building out the sort info, to pass into pre_sort() later on, from the user configuration.
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        """

        # Getting the user-entered selections from the GUI.
        self.n_record_select = Et.fromstring(str_xml).find('NRecords').text if 'NRecords' in str_xml else None
        self.do_sort = Et.fromstring(str_xml).find('DoSort').text == 'True' if 'DoSort' in str_xml else None
        if Et.fromstring(str_xml).find('FieldSelect') is not None:
            self.field_selection = Et.fromstring(str_xml).find('FieldSelect').text
        order_selection = Et.fromstring(str_xml).find('OrderType').text if 'OrderType' in str_xml else None

        # Letting the user know of the necessary selections, if they haven't been selected.
        if self.do_sort and self.field_selection is None:
            self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, 'Please select field to order by')
        elif self.do_sort and self.field_selection is not None:
            self.build_sort_info("SortInfo", self.field_selection, order_selection)  # Building out the <SortInfo> portion.

        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')  # Getting the output anchor from the XML file.

    def pi_add_incoming_connection(self, str_type: str, str_name: str) -> object:
        """
        The IncomingInterface objects are instantiated here, one object per incoming connection, also pre_sort() is called here.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The name of the input connection anchor, defined in the Config.xml file.
        :param str_name: The name of the wire, defined by the workflow author.
        :return: The IncomingInterface object(s).
        """

        if self.do_sort:
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

        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, self.xmsg('Missing Incoming Connection'))
        return False

    def pi_close(self, b_has_errors: bool):
        """
        Called after all records have been processed..
        :param b_has_errors: Set to true to not do the final processing.
        """

        self.output_anchor.assert_close()  # Checks whether connections were properly closed.

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

    def xmsg(self, msg_string: str) -> str:
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
        self.record_cnt = 0

    def ii_init(self, record_info_in: object) -> bool:
        """
        Called to report changes of the incoming connection's record metadata to the Alteryx engine.
        :param record_info_in: A RecordInfo object for the incoming connection's fields.
        :return: True for success, otherwise False.
        """

        record_info_out = record_info_in.clone()  # Since no new data is being introduced, setting the outgoing layout the same as record_info_in.
        self.parent.output_anchor.init(record_info_out)  # Lets the downstream tools know what the outgoing record metadata will look like, based on record_info_out.
        return True

    def ii_push_record(self, in_record: object) -> bool:
        """
        Responsible for pushing records out, under a count limit set by the user in n_record_select.
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: False if method calling limit (record_cnt) is hit.
        """

        self.record_cnt += 1  # To keep track of the push record calls.

        # Quit calling ii_push_record going forward once n_record_select limit is reached.
        if self.record_cnt <= int(self.parent.n_record_select):
            self.parent.output_anchor.push_record(in_record)
            self.parent.output_anchor.output_record_count(False)  # False: Let the Alteryx engine know of the record count
        else:
            return False
        return True

    def ii_update_progress(self, d_percent: float):
        """
        Called by the upstream tool to report what percentage of records have been pushed.
        :param d_percent: Value between 0.0 and 1.0.
        """

        self.parent.alteryx_engine.output_tool_progress(self.parent.n_tool_id, d_percent)  # Inform the Alteryx engine of the tool's progress.
        self.parent.output_anchor.update_progress(d_percent)  # Inform the downstream tool of this tool's progress.

    def ii_close(self):
        """
        Called when the incoming connection has finished passing all of its records.
        """

        self.parent.output_anchor.output_record_count(True)  # True: Let Alteryx engine know that all records have been sent downstream.
        self.parent.output_anchor.close()  # Close outgoing connections.
