import AlteryxPythonSDK
import xml.etree.ElementTree as ET


class AyxPlugin:
    def __init__(self, n_tool_id, alteryx_engine, generic_engine, output_anchor_mgr):
        # initialize *all* members that will be used (for PEP8 compliance)

        # miscellaneous variables
        self.n_tool_id = n_tool_id
        self.name = str('PyRecordID_') + str(self.n_tool_id)
        self.initialized = False

        # engine handles
        self.alteryx_engine = alteryx_engine
        self.generic_engine = generic_engine

        # output anchor management
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None

        # record management
        self.record_info_in = None
        self.record_info_out = None
        self.record_creator = None
        self.record_copier = None
        self.output_field = None

        # default config settings
        self.str_record_id = None
        self.n_record_count = None
        self.total_record_count = None
        self.output_type = None
        self.record_inc = None
        self.previous_inc_value = None

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
            self.str_record_id = root.find('FieldName').text
            self.total_record_count = int(root.find('EndValue').text)
            self.record_inc = int(root.find('StepByValue').text)
            self.n_record_count = int(root.find('StartValue').text) - self.record_inc
            t = root.find('FieldType').text
            if t == 'Int16':
                self.output_type = AlteryxPythonSDK.FieldType.int16
            elif t == 'Int32':
                self.output_type = AlteryxPythonSDK.FieldType.int32
            elif t == 'Int64':
                self.output_type = AlteryxPythonSDK.FieldType.int64
        except AttributeError:
            self.output_message('pi_init', AlteryxPythonSDK.EngineMessageType.error, 'Invalid XML: ' + str_xml)
            raise

        #
        # the Engine is ready for us to get the Output anchor. We know it is called 'Output' because
        # that is what we put in our Config.xml as its name
        #
        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')

        return

    #
    # pi_close will be called after all the records have been processed
    #
    def pi_close(self, b_has_errors):
        # self.closed = True
        # self.output_anchor.close()
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
        return True

    #
    # pi_push_all_records will be called if there are no inputs connected to this tool
    #
    def pi_push_all_records(self, n_record_limit):

        self.record_info_out = AlteryxPythonSDK.RecordInfo()

        #
        # add the new field we'll be writing to in the output stream
        #
        self.record_info_out.add_field(self.str_record_id, self.output_type)

        #
        # tell the downstream tools what our output will look like
        #
        self.output_anchor.init(self.record_info_out)

        #
        # create the helper for constructing records to pass downstream
        #
        self.record_creator = self.record_info_out.construct_record_creator()

        self.previous_inc_value = self.n_record_count

        for i in range(0, self.total_record_count):

            loop_value = self.previous_inc_value + self.record_inc

            #
            # set the value on our new column in the record_creator helper to be the new record_count
            #
            self.record_info_out[0].set_from_int64(self.record_creator, loop_value)

            #
            # ask the record_creator helper to give us a record we can pass downstream
            #
            out_record = self.record_creator.finalize_record()

            #
            # push the record downstream
            #
            self.output_anchor.push_record(out_record, False)

            #
            # Sets the capacity in bytes for variable-length data in this record to 0
            #
            self.record_creator.reset(0)

            self.previous_inc_value = loop_value

        return True

    #
    # ii_init will be called when an incoming connection has been initialized and has told the Engine
    #   what its output will look like. record_info_in represents what the incoming record will look like
    #
    def ii_init(self, record_info_in):
        self.record_info_in = record_info_in

        self.record_info_out = self.record_info_in.clone()

        self.record_info_out.add_field(self.str_record_id, self.output_type)

        self.output_anchor.init(self.record_info_out)

        self.record_creator = self.record_info_out.construct_record_creator()

        #
        # setup the record_copier for copying data from the input records into our new output records
        #
        self.record_copier = AlteryxPythonSDK.RecordCopier(self.record_info_out, self.record_info_in)
        # map each column of the input to where we want it to be in the output
        for idx in range(len(self.record_info_in)):
            self.record_copier.add(idx, idx)
        self.record_copier.done_adding()

        #
        # grab the index of our new field in the record, so we don't have to do a string lookup on every push_record
        #
        self.output_field = self.record_info_out[self.record_info_out.get_field_num(self.str_record_id)]

        self.initialized = True

        return True

    #
    # ii_push_record will be called every time we get a new record from the upstream tool
    #
    def ii_push_record(self, in_record):
        if self.initialized is not True:
            return False

        #
        # increment our custom record_count variable by the selected record increment to show we have a new record
        #
        self.n_record_count += self.record_inc

        #
        # copy the data from the incoming record into the outgoing record
        #
        self.record_creator.reset()
        self.record_copier.copy(self.record_creator, in_record)

        self.output_field.set_from_int64(self.record_creator, self.n_record_count)

        out_record = self.record_creator.finalize_record()

        #
        # push the record downstream and quit if there's a downstream error
        #
        if self.output_anchor.push_record(out_record) is False:
            return False

        return True

    #
    # ii_update_progress will be called periodically from the upstream tools, where they will tell us how far along
    #   they are in processing their data. If our tool needs to do any custom logic about how much work it has left to
    #   do, that logic should happen in here
    #
    def ii_update_progress(self, d_percent):
        self.alteryx_engine.output_tool_progress(self.n_tool_id, d_percent)

        self.output_anchor.update_progress(d_percent)

        return

    #
    # ii_close will be called when the upstream tool is finished
    #
    def ii_close(self):
        self.output_anchor.close()

        return
