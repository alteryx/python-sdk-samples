import AlteryxPythonSDK
import xml.etree.ElementTree as ET


class Input:
    def __init__(self, parent, type_, name):
        # initialize *all* members that will be used (for PEP8 compliance)

        # miscellaneous variables
        self.parent = parent
        self.type = type_
        self.name = name
        self.closed = False
        self.initialized = False

        # record management
        self.record_info_in = None
        self.record_copier = None
        self.in_record = None

        return

    #
    # ii_init will be called when an incoming connection has been initalized and has told the Engine
    #   what its output will look like. record_info_in represents what the incoming record will look like
    #
    def ii_init(self, record_info_in):
        self.record_info_in = record_info_in
        # print(len(self.record_info_in)) # record_info_in
        self.initialized = True

        # print(self.parent)
        # if ii_init has been called for all the inputs, then do the final init in the parent
        if all(input_.initialized for input_ in self.parent.inputs):
            self.parent.init_two()
            # print(len(self.parent.record_info_out))
        return True

    #
    # ii_push_record will be called every time we get a new record from the upstream tool
    #
    def ii_push_record(self, in_record):
        if any(not input_.initialized for input_ in self.parent.inputs):
            return False

        # cache the incoming data so it can be reused by the other inputs
        self.in_record = in_record

        self.parent.record_creator.reset()

        # copy the latest record from each input into the outgoing stream
        for input_ in filter(lambda input_: input_.in_record is not None, self.parent.inputs):
            input_.record_copier.copy(self.parent.record_creator, input_.in_record)

        # ask the record_creator helper to give us a record we can pass downstream
        out_record = self.parent.record_creator.finalize_record()

        # push the output downstream
        return self.parent.output_anchor.push_record(out_record)

    #
    # ii_update_progress will be called periodically from the upstream tools, where they will tell us how far along
    #   they are in processing their data. If our tool needs to do any custom logic about how much work it has left to
    #   do, that logic should happen in here
    #
    def ii_update_progress(self, d_percent):
        self.parent.update_progress(d_percent)

        return

    #
    # ii_close will be called when the upstream tool is finished
    #
    def ii_close(self):
        self.closed = True

        if all(input_.closed for input_ in self.parent.inputs):
            self.parent.close()

        return


class AyxPlugin:
    def __init__(self, n_tool_id, engine_interface, generic_engine, output_anchor_mgr):
        # initialize *all* members that will be used (for PEP8 compliance)

        # miscellaneous variables
        self.n_tool_id = n_tool_id
        self.name = str('PySingleMultiInput') + str(self.n_tool_id)
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
        self.record_info_out_two = None
        self.record_creator = None

        #
        # TODO: create your custom members here and give them default values
        #

        self.inputs = []
        self.primary_field_names = []
        #
        # END custom members
        #

        return

    def output_message(self, method, status, message):
        # helper for printing messages out to the engine
        self.alteryx_engine.output_message(self.n_tool_id, status, method + ': ' + str(message))

        return

    #
    # pi_init will be called when the Engine is ready to give us the tool configuration from the GUI
    #
    def pi_init(self, str_xml):
        try:
            root = ET.fromstring(str_xml)
            #
            # TODO: parse the XML from Designer here
            #

            #
            # END XML parsing
            #
        except AttributeError:
            self.output_message('pi_init', AlteryxPythonSDK.EngineMessageType.error, 'Invalid XML: ' + str_xml)
            raise

        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')

        return

    #
    # pi_close will be called after all the records have been processed
    #
    def pi_close(self, b_has_errors):
        pass

    #
    # pi_add_incoming_connection will be called when a new input is connected to this tool
    #
    def pi_add_incoming_connection(self, str_type, str_name):
        self.inputs.append(Input(self, str_type, str_name))
        return self.inputs[-1]

    #
    # pi_add_outgoing_connection will be called when a new output is connected to this tool
    #
    def pi_add_outgoing_connection(self, str_name):
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

    # a helper for setting up a child record copier
    def init_child_record_copier(self, child, start_idx):
        # setup the record_copier for copying data from the input records into our new output records
        child.record_copier = AlteryxPythonSDK.RecordCopier(self.record_info_out, child.record_info_in)
        # map each column of the input to where we want it to be in the output
        for idx in range(len(child.record_info_in)):
            child.record_copier.add(start_idx+idx, idx)
        child.record_copier.done_adding()
        return

    def init(self):
        if len(self.inputs) == 0:
            self.output_message('init', AlteryxPythonSDK.EngineMessageType.error, 'At least one input is required')
            return

        # construct a RecordInfo that has the fields from each of the inputs
        inputs_reversed = []
        for input_ in reversed(self.inputs):
            inputs_reversed.append(input_)
        print(inputs_reversed[0].name)
        print(inputs_reversed[1].name)
        self.inputs = inputs_reversed
        # if len(self.inputs) > 1:
        #     for idx in range(0,len(self.inputs)):
        #         inputs_reversed.append(self.inputs[])


        self.record_info_out = AlteryxPythonSDK.RecordInfo(self.generic_engine)
        field_count = 0
        for input_ in self.inputs:
            if not input_.initialized:
                self.output_message(
                    'init'
                    , AlteryxPythonSDK.EngineMessageType.error
                    , 'An incoming connection is not initialized'
                )
                return
            """change everything below in this function get the metadata to match. TEST BY CREATING record_info_out2 """
            # add the fields from this input to record_info_out
            self.record_info_out.init_from_xml(
                input_.record_info_in.get_record_xml_meta_data()
                , input_.type + input_.name
            )
            # setup the record_copier for copying data from this input record into our new output records
            self.init_child_record_copier(input_, field_count)
            print(input_.name)
            field_count += len(input_.record_info_in)

        # tell the downstream tools what our records will look likeS
        self.output_anchor.init(self.record_info_out)

        # create the helper for constructing records to pass downstream
        self.record_creator = self.record_info_out.construct_record_creator()

        return

    """TEST FUNCTIONS BELOW """
    # a helper for setting up a child record copier
    def init_child_record_copier_two(self, child, start_idx):
        # setup the record_copier for copying data from the input records into our new output records
        child.record_copier = AlteryxPythonSDK.RecordCopier(self.record_info_out_two, child.record_info_in)
        # map each column of the input to where we want it to be in the output
        for idx in range(len(child.record_info_in)):
            child.record_copier.add(start_idx+idx, idx)
        child.record_copier.done_adding()
        return

    def init_two(self):
        if len(self.inputs) == 0:
            self.output_message('init', AlteryxPythonSDK.EngineMessageType.error, 'At least one input is required')
            return

        inputs_reversed = []
        for input_ in reversed(self.inputs):
            inputs_reversed.append(input_)
        self.inputs = inputs_reversed

        # construct a RecordInfo that has the fields from each of the inputs
        self.record_info_out_two = AlteryxPythonSDK.RecordInfo(self.generic_engine)
        # print(self.record_info_out_two)
        field_count = 0
        for input_ in self.inputs:
            if not input_.initialized:
                self.output_message(
                    'init'
                    , AlteryxPythonSDK.EngineMessageType.error
                    , 'An incoming connection is not initialized'
                )
                return
            """change everything below in this function get the metadata to match. TEST BY CREATING record_info_out2 """
            # add the fields from this input to record_info_out
            if (len(self.primary_field_names) == 0):
                self.record_info_out_two.init_from_xml(input_.record_info_in.get_record_xml_meta_data())
                self.primary_field_names = ','.join([field.name for field in self.record_info_out_two])

            else:
                secondary_field_names = [field.name for field in input_.record_info_in]
                new_fields = [items for items in secondary_field_names if items not in self.primary_field_names]
                print(new_fields)

            print(self.record_info_out_two[1].name)
            # print(input_.record_info_in.get_record_xml_meta_data())
            print(len(self.record_info_out_two))

            print(input_.record_info_in.get_field_by_name('test'))
            print(input_.record_info_in.get_field_by_name('test'))



            # setup the record_copier for copying data from this input record into our new output records
            self.init_child_record_copier_two(input_, field_count)

            field_count += len(input_.record_info_in)

        # tell the downstream tools what our records will look like
        self.output_anchor.init(self.record_info_out_two)

        # create the helper for constructing records to pass downstream
        self.record_creator = self.record_info_out_two.construct_record_creator()

        return

    def update_progress(self, d_percent):
        self.alteryx_engine.output_tool_progress(self.n_tool_id, d_percent)

        self.output_anchor.update_progress(d_percent)

    def close(self):
        self.output_anchor.close()

        return
