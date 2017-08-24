import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et


class NonInterface:
    def __init__(self):
        self.xml_out = None
        self.hash_right = None
        self.hash_left = None
        self.fields_right = []
        self.fields_left = []

    @property
    def is_compatible(self):
        # Returns true if same pattern of zeroes and ones in the data sets
        if self.hash_left == self.hash_right:
            return True
        else:
            return False

    def is_equal(self, index: int):
        # Returns true if the `other_field` argument has the same type, size, and scale as this field.
        return Sdk.Field.equal_type(self.fields_right[index], self.fields_left[index])

    def validate_inputs(self, left_count: int, right_count: int):
        if left_count != right_count:
            raise self.__alteryx_engine.output_message(
                self.__n_tool_id,
                Sdk.EngineMessageType.error,
                'Both inputs must have the same number of fields.'
            )
        pass

    @property
    def __xml_out(self):
        return self.xml_out

    @__xml_out.setter
    def __xml_out(self, xml_string: str):
        self.xml_out = xml_string
        pass

    @staticmethod
    def change_to_bool(xml_string: str):
        root = Et.fromstring(xml_string)
        for field in root.iter('Field'):
            field.set('type', 'Bool')
            field.set('size', '1')
        return Et.tostring(root, encoding='utf8', method='xml').decode('utf8')


class AyxPlugin(NonInterface):
    def __init__(self, n_tool_id, alteryx_engine, generic_engine, output_anchor_mgr):
        """Initializing members that will be used."""
        super().__init__()
        self.__n_tool_id = n_tool_id
        self.name = str('PyMultiInput_') + str(self.__n_tool_id)
        self.__alteryx_engine = alteryx_engine
        self.__generic_engine = generic_engine
        self.__output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None
        self.record_info_out = None
        self.record_creator = None
        self.is_left = True  # flag for distinguishing input flow
        pass

    def pi_init(self, str_xml):
        """
        Called when the Alteryx engine is ready to provide the tool configuration from the GUI.
        :param str_xml: The raw XML from the GUI
        :return: Void
        """
        # Getting the output anchor from Config.xml by the output connection name
        self.output_anchor = self.__output_anchor_mgr.get_output_anchor('Output')
        pass

    def pi_add_incoming_connection(self, str_type, str_name):
        """
        Called when the Alteryx engine is attempting to add an incoming data connection.
        :param str_type: The type of each input connection.
        :param str_name: A unique name for each input connection.
        :return: Self, and a reference to an object.
        """
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
        self.__alteryx_engine.output_message(self.__n_tool_id, Sdk.EngineMessageType.error, 'Misssing Incoming Connection')
        return False
        
    def pi_close(self, b_has_errors):
        """
        Called after all data has finished flowing through all the fields.
        :param b_has_errors: Boolean; set to true to not do the final processing.
        :return: Void
        """
        self.output_anchor.assert_close()
        pass
        
    def ii_init(self, record_info_in):
        """
        For each connection:
            1) Handles hash extraction from the data
            2) Stores the references of each record_info_in field into an array
            3) Building out the data structure layout for the record_info_out reference
        Called when the incoming connection's record metadata is available or has changed, and
        :param record_info_in: A RecordInfo object containing the XML representation for the incoming connection's field and sort properties.
        :return: True
        """
        if self.is_left:
            self.hash_left = record_info_in.get_hash()
            
            for field in range(record_info_in.__len__()):
                self.fields_left.append(record_info_in[field])
            
            self.__xml_out = str(record_info_in.get_record_xml_meta_data(False)).replace('</RecordInfo>', '')
        else:
            self.hash_right = record_info_in.get_hash()
            
            for field in range(record_info_in.__len__()):
                self.fields_right.append(record_info_in[field])
                
            # check to see if same number of fields for both connections
            self.validate_inputs(len(self.fields_left), len(self.fields_right))
            
            self.__xml_out += str(record_info_in.get_record_xml_meta_data(False)).replace('<RecordInfo>', '')
            self.__xml_out = self.__xml_out.replace('</RecordInfo>', '<Field name="IsBinaryCompatible" size="1" type="Bool"/></RecordInfo>')        
            self.__xml_out = self.__xml_out.replace('\n', '').replace('\t', '')
            self.__xml_out = self.change_to_bool(self.__xml_out)        
        
            self.record_info_out = Sdk.RecordInfo(self.__generic_engine)
            self.record_info_out.init_from_xml(self.__xml_out, '')
            self.record_creator = self.record_info_out.construct_record_creator()
            self.output_anchor.init(self.record_info_out)
        return True
        
    def ii_push_record(self, in_record):
        """
        Responsible for pushing out the evaluated boolean results for both the is_equal() and is_compatible() custom tests
        :param in_record: The data for the incoming record.
        :return: False
        """
        if self.is_left:
            return False
        else:
            self.record_creator.reset()

            for field in range(len(self.fields_right)):
                self.record_info_out[field].set_from_bool(
                    self.record_creator,
                    self.is_equal(field)
                )
                # the above's index couple
                self.record_info_out[field + len(self.fields_right)].set_from_bool(
                    self.record_creator,
                    self.is_equal(field)
                )
            
            # very last include binary compatible
            self.record_info_out[len(self.fields_right) + len(self.fields_left)].set_from_bool(
                self.record_creator,
                self.is_compatible
            )
            
            out_record = self.record_creator.finalize_record()
            self.output_anchor.push_record(out_record)
            return False
        
    def ii_update_progress(self, d_percent):
        """
        Called when the incoming connection is requesting that the plugin update its progress.
        :param d_percent: Value between 0 and 1
        :return: Void
        """
        if self.is_left:
            pass
        else:
            self.__alteryx_engine.output_tool_progress(self.__n_tool_id, d_percent)
            self.output_anchor.update_progress(d_percent)
            pass

    def ii_close(self):
        """
        Responsible for setting the stream flow to focus on the right input
        Called when the incoming connection has finished passing all of its records.
        :return: Void
        """
        if self.is_left:
            self.is_left = not self.is_left
        else:
            self.output_anchor.close()
        pass

