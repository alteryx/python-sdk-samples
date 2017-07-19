import AlteryxPythonSDK
import xml.etree.ElementTree as ET
import os


class AyxPlugin:
    def __init__(self, n_tool_id, engine_interface, generic_engine, output_anchor_mgr):
        # initialize *all* members that will be used (for PEP8 compliance)

        # miscellaneous variables
        self.n_tool_id = n_tool_id
        self.name = str('PyOutputTool_') + str(self.n_tool_id)
        self.closed = False
        self.initialized = False

        # engine handles
        self.alteryx_engine = engine_interface
        self.generic_engine = generic_engine

        # record management
        self.record_info_in = None

        #
        # TODO: create your custom members here and give them default values
        #
        # self.str_file_path = 'testing'
        #
        # END custom members
        #

        return

    def output_message(self, method, status, message):
        # helper for printing messages out to the engine
        self.alteryx_engine.output_message(self.n_tool_id, status, method + ': ' + str(message))

    #
    # pi_init will be called when the Engine is ready to give us the tool configuration from the GUI
    #
    def pi_init(self, str_xml):
        try:
            root = ET.fromstring(str_xml)
            #
            # TODO: parse the XML from Designer here
            #
            self.str_file_path = root.find('TextBox1').text
            #
            # END XML parsing
            #
        except AttributeError:
            self.output_message('pi_init', AlteryxPythonSDK.EngineMessageType.error, 'Invalid XML: ' + str_xml)
            raise

        self.initialized = True

        return

    #
    # pi_close will be called after all the records have been processed
    #
    def pi_close(self, b_has_errors):
        return

    #
    # pi_add_incoming_connection will be called when a new input is connected to this tool
    #
    def pi_add_incoming_connection(self, str_type, str_name):
        return self

    #
    # pi_add_outgoing_connection will be called when a new output is connected to this tool
    #
    def pi_add_outgoing_connection(self, str_name):
        self.output_message(
            'ii_push_record'
            , AlteryxPythonSDK.EngineMessageType.error
            , 'This tool does not accept an Outgoing Connection'
        )
        return True

    #
    # pi_push_all_records will be called if there are no inputs connected to this tool
    #
    def pi_push_all_records(self, n_record_limit):
        self.output_message(
            'pi_push_all_records'
            , AlteryxPythonSDK.EngineMessageType.error
            , 'Missing Incoming Connection'
        )

        return False

    #
    # ii_init will be called when an incoming connection has been initalized and has told the Engine
    #   what its output will look like. record_info_in represents what the incoming record will look like
    #
    def ii_init(self, record_info_in):
        self.record_info_in = record_info_in

        def get_field_names(record_info_in):
            ret = []
            for idx in range(len(record_info_in)):
                ret.append(record_info_in[idx].name) # record_info_in metadata about a field record_info_in[0].name
            return ret

        field_names = get_field_names(record_info_in)
        field_names = str(field_names)
        field_names = field_names.replace('[','')
        field_names = field_names.replace(']','')
        field_names = field_names.replace('\'','')

        self.field_names = field_names
        self.all_records = ''

        self.initialized = True

        return True

    #
    # ii_push_record will be called every time we get a new record from the upstream tool
    #
    def ii_push_record(self, in_record):
        if self.initialized is not True:
            return False

        def null_to_str(field, in_record):
            ret = field.get_as_string(in_record)
            if ret is None:
                return '[Null]'
            return ret

        all_records = str([null_to_str(field, in_record) for field in self.record_info_in])
        all_records = all_records.replace('[','')
        all_records = all_records.replace(']','')
        all_records = all_records.replace('\'','')

        self.all_records += all_records + ' \n'

        return True

    #
    # ii_update_progress will be called periodically from the upstream tools, where they will tell us how far along
    #   they are in processing their data. If our tool needs to do any custom logic about how much work it has left to
    #   do, that logic should happen in here
    #
    def ii_update_progress(self, d_percent):
        self.alteryx_engine.output_tool_progress(self.n_tool_id, d_percent)
        return

    #
    # ii_close will be called when the upstream tool is finished
    #
    def ii_close(self):
            # self.output_message('Error ',AlteryxPythonSDK.EngineMessageType.info, self.str_file_path + ' already exists. Please enter a different path.')
        if os.access(self.str_file_path, os.F_OK):
            self.output_message('Error ', AlteryxPythonSDK.EngineMessageType.error, self.str_file_path + ' already exists. Please enter a different path.')
        else:
            with open(self.str_file_path, 'a') as testfile:
                testfile.write(self.field_names + '\n' + self.all_records)
                testfile.close
                message = self.str_file_path + ' was written.'
                self.output_message('Output ', AlteryxPythonSDK.EngineMessageType.info, message)
        return
