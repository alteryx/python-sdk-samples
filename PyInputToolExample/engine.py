import AlteryxPythonSDK
import xml.etree.ElementTree as ET
import csv
import os


class AyxPlugin:
    def __init__(self, n_tool_id, engine_interface, generic_engine, output_anchor_mgr):
        # initialize *all* members that will be used (for PEP8 compliance)

        # miscellaneous variables
        self.n_tool_id = n_tool_id
        self.name = str('PyInputTool_') + str(self.n_tool_id)
        self.closed = False
        self.initialized = False

        # engine handles
        self.alteryx_engine = engine_interface
        self.generic_engine = generic_engine

        # output anchor management
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None

        # record management
        self.record_info_out = None
        self.record_creator = None
        self.output_field = None

        #
        # create your custom members here and give them default values
        #
        self.file_input_name = 'C:\\Users\\username\\Desktop\\PythonInputTest.csv'
        self.file_out = None
        self.file_reader = None
        #
        # END custom members
        #

        return

    # helper for determining if the file is csv, used for error messaging later
    def is_csv(self):
        filename, file_extension = os.path.splitext(self.file_input_name)
        if file_extension == '.csv' or file_extension == '.CSV':
            return True
        return False

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
            # parse the necessary XML from Designer where node name is equal
            #  to the dataname defined in Gui.html
            #
            self.file_input_name = root.find('browseFiles').text

            # END XML parsing

        except AttributeError:
            self.output_message('pi_init', AlteryxPythonSDK.EngineMessageType.error, 'Invalid XML: ' + str_xml)
            raise

        #
        # the Engine is ready for us to get the Output anchor. We know it is called 'Output' because
        #  that is what we put in our Config.xml as its name
        #

        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')

        self.initialized = True

        return

    #
    # pi_close will be called after all the records have been processed
    #
    def pi_close(self, b_has_errors):
        self.closed = True
        self.output_anchor.close()
        return

    #
    # pi_add_incoming_connection will be called when a new input is connected to this tool
    #
    def pi_add_incoming_connection(self, str_type, str_name):
        self.output_message(
            'pi_add_incoming_connection'
            , AlteryxPythonSDK.EngineMessageType.error
            , 'This tool does not accept an Incoming Connection'
        )
        return self

    #
    # pi_add_outgoing_connection will be called when a new output is connected to this tool
    #
    def pi_add_outgoing_connection(self, str_name):
        return True

    #
    # pi_push_all_records will be called if there are no inputs connected to this tool
    #
    def pi_push_all_records(self, n_record_limit):
        if self.initialized != True:
            return False

        # stop processing and returns error if the file is not a csv
        if self.is_csv() != True:
            self.output_message('Error', AlteryxPythonSDK.EngineMessageType.error, 'This tool only accepts csv files')
            return False

        # Save a reference to the RecordInfo passed into this function in the global namespace, so we can access it later
        self.record_info_out = AlteryxPythonSDK.RecordInfo()
        # create a read-only file object
        self.file_out = open(self.file_input_name, 'r', errors = 'replace')
        # map the information read into a dict where the fieldnames are the keys
        self.file_reader = csv.DictReader(self.file_out)

        #
        # add metadata info that is passed to tools downstream
        #
        for field in self.file_reader.fieldnames:
            self.record_info_out.add_field(
                field
                , AlteryxPythonSDK.FieldType.v_string
                , 254
                , 0
                , self.name
                , ''
            )

        #
        # tell the downstream tools what our output will look like
        #
        self.output_anchor.init(self.record_info_out, '')

        #
        # create the helper for constructing records to pass downstream
        #
        self.record_creator = self.record_info_out.construct_record_creator()
        rownum = 0

        #
        # Loop through each record (or row) of data that has been passed into this function
        #
        for row in self.file_reader:
            rownum += 1
            # Iterate through the fields in this row and add them in order to the output row
            for index, value in enumerate(row.items()):
                self.record_info_out[index].set_from_string(self.record_creator, value[1])
            out_record = self.record_creator.finalize_record()
            # Push this row onto the end of the output anchor
            self.output_anchor.push_record(out_record, False)
            # Reset the record creator in order to begin looping through the next row
            self.record_creator.reset(0)

        # self.rownum = sum(1 for row in self.file_reader)
        self.output_message('Info', AlteryxPythonSDK.EngineMessageType.info, str(rownum) + ' records were read from ' + self.file_input_name)

        return True


    #
    # ii_init will be called when an incoming connection has been initalized and has told the Engine
    #   what its output will look like. record_info_in represents what the incoming record will look like
    #
    def ii_init(self, record_info_in):
        self.output_message(
            'ii_init'
            , AlteryxPythonSDK.EngineMessageType.error
            , 'This tool does not accept an Incoming Connection'
        )
        return False

    #
    # ii_push_record will be called every time we get a new record from the upstream tool
    #
    def ii_push_record(self, in_record):
        self.output_message(
            'ii_push_record'
            , AlteryxPythonSDK.EngineMessageType.error
            , 'This tool does not accept an Incoming Connection'
        )
        return False

    #
    # ii_update_progress will be called periodically from the upstream tools, where they will tell us how far along
    #   they are in processing their data. If our tool needs to do any custom logic about how much work it has left to
    #   do, that logic should happen in here
    #
    def ii_update_progress(self, d_percent):
        self.output_message(
            'ii_update_progress'
            , AlteryxPythonSDK.EngineMessageType.error
            , 'This tool does not accept an Incoming Connection'
        )
        return

    #
    # ii_close will be called when the upstream tool is finished
    #
    def ii_close(self):
        self.output_message(
            'ii_close'
            , AlteryxPythonSDK.EngineMessageType.error
            , 'This tool does not accept an Incoming Connection'
        )
        return
