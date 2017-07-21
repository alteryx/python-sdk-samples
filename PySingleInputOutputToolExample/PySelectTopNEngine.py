import AlteryxPythonSDK as Sdk
import xml.etree.ElementTree as Et


class Constants:
    def __init__(self, n_tool_id, alteryx_engine, generic_engine, output_anchor_mgr):
        self.n_tool_id = n_tool_id
        self.alteryx_engine = alteryx_engine
        self.generic_engine = generic_engine
        self.output_anchor_mgr = output_anchor_mgr
        self.output_anchor = None
        return

    def output_message(self, method, status, message):
        self.alteryx_engine.output_message(self.n_tool_id, status, method + ': ' + str(message))

    def pi_close(self, b_has_errors):
        pass

    def pi_add_incoming_connection(self, str_type, str_name):
        return self

    def pi_add_outgoing_connection(self, str_name):
        return True

    def pi_push_all_records(self, n_record_limit):
        self.output_message('pi_push_all_records', Sdk.EngineMessageType.error, 'Missing Incoming Connection')
        return False

    def ii_update_progress(self, d_percent):
        self.alteryx_engine.output_tool_progress(self.n_tool_id, d_percent)
        self.output_anchor.update_progress(d_percent)
        return

    def ii_close(self):
        self.output_anchor.close()
        return


class AyxPlugin(Constants):
    # Initiations
    def __init__(self, n_tool_id, alteryx_engine, generic_engine, output_anchor_mgr):
        super().__init__(n_tool_id, alteryx_engine, generic_engine, output_anchor_mgr)
        self.name = str('PyRandomRecordSelect_') + str(self.n_tool_id)
        self.initialized = False
        self.n_record_select = None
        self.record_cnt = 0
        self.record_info_in = None
        self.record_info_out = None
        self.record_creator = None
        self.record_copier = None

    def pi_init(self, str_xml):
        # Identifying the input and output points
        root = Et.fromstring(str_xml)
        self.n_record_select = root.find('NRecords').text
        self.output_anchor = self.output_anchor_mgr.get_output_anchor('Output')
        return

    def ii_init(self, record_info_in):
        # Determining what the output will look like by copying the inputs over to outputs
        self.record_info_in = record_info_in
        self.record_info_out = self.record_info_in
        self.output_anchor.init(self.record_info_out)
        self.record_creator = self.record_info_out.construct_record_creator()
        self.record_copier = Sdk.RecordCopier(self.record_info_out, self.record_info_in)
        for idx in range(len(self.record_info_in)):
            self.record_copier.add(idx, idx)
        self.record_copier.done_adding()
        self.initialized = True
        return True

    def ii_push_record(self, in_record):
        # Responsible for pushing records out, with a count limit set by the user in n_record_select
        if not self.initialized:
            return False
        self.record_cnt += 1
        self.record_creator.reset()
        self.record_copier.copy(self.record_creator, in_record)
        if self.record_cnt <= int(self.n_record_select):
            out_record = self.record_creator.finalize_record()
            if self.output_anchor.push_record(out_record) is False:
                return False
        return True

