import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et


class AyxPlugin:
    def __init__(self, n_tool_id, alteryx_engine, generic_engine, output_anchor_mgr):
        self.__n_tool_id = n_tool_id
        self.name = str('PyMultiInput_') + str(self.__n_tool_id)
        self.__alteryx_engine = alteryx_engine
        self.__generic_engine = generic_engine
        self.__output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None
        self.record_info_out = None
        self.record_creator = None
        self.is_left = True
        self.xml_out = None
        self.hash_right = None
        self.hash_left = None
        self.fields_right = []
        self.fields_left = []
        pass
    
    @property
    def is_compatible(self):
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

    def pi_init(self, str_xml):
        self.output_anchor = self.__output_anchor_mgr.get_output_anchor('Output')
        pass

    def pi_add_incoming_connection(self, str_type, str_name):
        return self

    def pi_add_outgoing_connection(self, str_name):
        return True

    def pi_push_all_records(self, n_record_limit):
        self.__alteryx_engine.output_message(
            self.__n_tool_id,
            Sdk.EngineMessageType.error,
            'Misssing Incoming Connection'
        )
        return False
        
    def pi_close(self, b_has_errors):        
        self.output_anchor.assert_close()
        pass
        
    def ii_init(self, record_info_in):
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
        if self.is_left:
            pass
        else:
            self.__alteryx_engine.output_tool_progress(self.__n_tool_id, d_percent)
            self.output_anchor.update_progress(d_percent)
            pass

    def ii_close(self):
        if self.is_left:
            self.is_left = not self.is_left
        else:
            self.output_anchor.close()
        pass

