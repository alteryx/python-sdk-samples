import AlteryxPythonSDK
import xml.etree.ElementTree as ET


class AyxPlugin:
    def __init__(self, n_tool_id, engine_interface, generic_engine, output_anchor_mgr):
        # initialize *all* members that will be used (for PEP8 compliance)

        # miscellaneous variables
        self.n_tool_id = n_tool_id
        self.name = str('PyPreSort_') + str(self.n_tool_id)
        self.closed = False
        self.initialized = False

        # engine handles
        self.alteryx_engine = engine_interface
        self.generic_engine = generic_engine

        # output anchor management
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None

        # config settings go here
        self.str_xml_sort_info = None

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
            #
            # parse the XML from Designer here
            #

            root = ET.fromstring(str_xml)
            #
            # create the xml string that tells the pre_sort function how to sort
            #  the data
            #
            # if FieldFilterList is not specified, all fields will be passed
            #  through
            #
            self.str_xml_sort_info = r'<FieldFilterList>'
            self.str_xml_sort_info += ''.join(
                ['<Field field="{}" />'.format(node.text) for node in filter(
                    lambda node: node.text is not None
                    , [root.find(name) for name in ['Sort1', 'Sort2', 'Sort3']]
                )]
            )
            self.str_xml_sort_info += r'</FieldFilterList>'

            self.str_xml_sort_info += r'<SortInfo noProgress="True">'
            self.str_xml_sort_info += ''.join(
                ['<Field field="{}" />'.format(node.text) for node in filter(
                    lambda node: node.text is not None
                    , [root.find(name) for name in ['Sort1', 'Sort2', 'Sort3']]
                )]
            )
            self.str_xml_sort_info += r'</SortInfo>'

            #
            # END XML parsing
            #
        except AttributeError:
            self.output_message('pi_init', AlteryxPythonSDK.EngineMessageType.error, 'Invalid XML: ' + str_xml)
            raise

        #
        # the Engine is ready for us to get the Output anchor. The name 'Output'
        #  is specified in Config.xml
        #
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
        #
        # pre_sort sorts data from incoming connection according to str_xml_sort_info
        #  and can only be called during pi_add_incoming_connection
        #
        self.alteryx_engine.pre_sort(str_type, str_name, self.str_xml_sort_info)
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
        self.output_message(
            'pi_push_all_records'
            , AlteryxPythonSDK.EngineMessageType.error
            , 'Missing Incoming Connection'
        )

        return False

    #
    # ii_init will be called when an incoming connection has been initialized and has told the Engine
    #   what its output will look like. record_info_in represents what the incoming record will look like
    #
    def ii_init(self, record_info_in):
        #
        # notifies the downstream tools that the data is sorted with the optional
        #  argument str_xml_sort_info
        #
        self.output_anchor.init(record_info_in, self.str_xml_sort_info)

        self.initialized = True

        return True

    #
    # ii_push_record will be called every time we get a new record from the upstream tool
    #
    def ii_push_record(self, in_record):
        if not self.initialized:
            return False

        #
        # the pre_sort did all our work for us, and this particular tool has no additional fields to
        #   add or modify, so we are just going to pass the input record downstream
        #

        #
        # push the record downstream and quit if there's a downstream error
        #
        return self.output_anchor.push_record(in_record)

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

        self.closed = True

        return
