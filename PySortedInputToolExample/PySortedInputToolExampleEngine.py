import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et


class NonInterface:
    def __init__(self):
        self.xml_sort_info = ""

    @staticmethod
    def extract_field_type(record_info_in):
        """
        Extracts the selected field's data type from the XML.
        :param record_info_in: A RecordInfo object containing the XML representation for the incoming connection's field and sort properties.
        :return: A short list of the selected field's meta data
        """
        xml_meta_data = record_info_in.get_record_xml_meta_data(False)  # False = do not return source attribute
        dict = Et.fromstring(xml_meta_data).findall('Field')[0].attrib  # To access field name and type
        list_of_dict_values = list(dict.values())  # Turn to list from dict to access index
        return list_of_dict_values[len(list_of_dict_values)-1]  # Extract and return last index for field's data type

    def build_sort_info(self, element: str, subelement: property, order: str):
        """
        Responsible for building out the proper XML string format for pre_sort.
        :param element: SortInfo or FieldFilterList
        :param subelement: The user selected field
        :param order: Asc or Desc
        :return: Void
        """
        # Building the XML string to pass as an argument to pre_sort's sort info parameter.
        root = Et.Element(element)
        sub_element = 'Field field="{0}" order="{1}"' if order != "" else 'Field field="{0}"'
        Et.SubElement(root, sub_element.format(subelement, order))
        xml_string = Et.tostring(root, encoding='utf8', method='xml')
        # Decode to string and remove the xml info
        self.xml_sort_info += xml_string.decode('utf8').replace("<?xml version='1.0' encoding='utf8'?>\n", "")
        pass


class AyxPlugin(NonInterface):
    def __init__(self, n_tool_id, alteryx_engine, generic_engine, output_anchor_mgr):
        """Initializing members that will be used."""
        super().__init__()
        # Miscellaneous properties
        self.n_tool_id = n_tool_id
        self.name = str('PySortedInputToolExample_') + str(self.n_tool_id)
        self.initialized = False
        # Engine handles
        self.alteryx_engine = alteryx_engine
        self.generic_engine = generic_engine
        # Output anchor management
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None
        # Default configuration setting
        self.selected_aggregation = None
        self.field_selection = None
        # Record management properties
        self.record_info_in = None
        self.record_info_out = None
        self.record_creator = None
        self.record_copier = None
        pass

    def pi_init(self, str_xml):
        """
        Extracting the user's selections and passing those as arguments to the XML builder, set_str_xml_sort_info.
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI.
        :return: True
        """
        # Getting the dataName data property from the GUI config
        self.selected_aggregation = Et.fromstring(str_xml).find('operationSelect').text
        self.field_selection = Et.fromstring(str_xml).find('FieldSelect').text
        # Build <SortInfo>
        if self.selected_aggregation == "min":
            self.build_sort_info("SortInfo", self.field_selection, "Asc")
        elif self.selected_aggregation == "max":
            self.build_sort_info("SortInfo", self.field_selection, "Desc")
        # Build <FieldFilter>
        self.build_sort_info("FieldFilterList", self.field_selection, "")
        # Getting the output anchor from Config.xml by the output connection name
        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')
        return True

    def pi_add_incoming_connection(self, str_type, str_name):
        """
        Sorting the user's selected field happens here.
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The type of each input connection.
        :param str_name: A unique name for each input connection.
        :return: Self, and a reference to an object.
        """
        self.alteryx_engine.pre_sort(str_type, str_name, self.xml_sort_info)
        return self

    def pi_add_outgoing_connection(self, str_name):
        """
        Called when the Alteryx engine is attempting to add an outgoing data connection.
        :param str_name: A unique name for each output connection.
        :return: Boolean, where True signifies that the connection is accepted.
        """
        return True

    def pi_push_all_records(self, n_record_limit):
        """
        Called when the Alteryx engine when it's expecting the plugin to provide all of its data.
        Only pertinent to tools which have no upstream connections, like the Input tool.
        :param n_record_limit: Set it to <0 for no limit, 0 for no records, and >0 to specify the number of records.
        :return: False
        """
        self.alteryx_engine.output_message(self.n_tool_id, Sdk.EngineMessageType.error, 'Missing Incoming Connection')
        return False

    def pi_close(self, b_has_errors):
        """
        Called after all data has finished flowing through all the fields.
        :param b_has_errors: Boolean; set to true to not do the final processing.
        :return: Void
        """
        pass

    def ii_init(self, record_info_in):
        """
        Handles type validation, and renames the outgoing field.
        Called when the incoming connection's record metadata is available or has changed, and
        has let the Alteryx engine know what its output will look like.
        :param record_info_in: A RecordInfo object containing the XML representation for the incoming connection's field and sort properties.
        :return: False if type validation fails.
        """
        if (self.extract_field_type(record_info_in)) not in\
                {'Byte', 'Int16', 'Int32', 'Int64', 'FixedDecimal', 'Float', 'Double'}:
            self.alteryx_engine.output_message(
                self.n_tool_id,
                Sdk.EngineMessageType.error,
                'Selected A Non-Numeric Field'
            )
            return False
        else:
            self.record_info_in = record_info_in
            self.record_info_out = self.record_info_in
            # Rename outgoing field
            self.record_info_out.rename_field_by_name(
                self.field_selection,
                '{0}_{1}'.format(self.selected_aggregation, self.field_selection)
            )
            # Copy over record_info_in to record_info_out
            self.output_anchor.init(self.record_info_out)
            self.record_creator = self.record_info_out.construct_record_creator()
            self.record_copier = Sdk.RecordCopier(self.record_info_out, self.record_info_in)
            # Map each column of the input to where we want in the output
            for idx in range(len(self.record_info_in)):
                self.record_copier.add(idx, idx)
            self.record_copier.done_adding()
            # Let ii_put_record know that ii_init has been initialized
            self.initialized = True
        return True

    def ii_push_record(self, in_record):
        """
        Responsible for pushing the first record out - either the highest or lowest numeric value of the field.
        Called when an input record is being sent to the plugin.
        :param in_record: The data for the incoming record.
        :return: False
        """
        self.record_creator.reset()
        self.record_copier.copy(self.record_creator, in_record)
        out_record = self.record_creator.finalize_record()
        self.output_anchor.push_record(out_record)
        return False

    def ii_update_progress(self, d_percent):
        """
        Called when the incoming connection is requesting that the plugin update its progress.
        :param d_percent: Value between 0 and 1
        :return: Void
        """
        self.alteryx_engine.output_tool_progress(self.n_tool_id, d_percent)
        self.output_anchor.update_progress(d_percent)
        pass

    def ii_close(self):
        """
        Called when the incoming connection has finished passing all of its records.
        :return: Void
        """
        self.output_anchor.close()
        pass

