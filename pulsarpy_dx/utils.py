#!/usr/bin/env python3                                                                                 
                                                                                                       
###                                                                                                    
#Nathaniel Watson                                                                                      
#Stanford School of Medicine                                                                           
#Nov. 6, 2018                                                                                          
#nathankw@stanford.edu                                                                                 
### 

from pulsarpy import models

def create_srun(sreq, dxres):
    """
    Creates a SequencingRun record based on the provided DNAnexus sequencing results, to be linked
    to the given SequencingRequest object.

    Args:
        sreq: A `pulsarpy.models.SequencingRequest` instance.
        dxres: `scgpm_seqresults_dnanexus.dnanexus_utils.du.DxSeqResults()` instance that contains
               sequencing results metadata in DNAnexus for the given srun.
    """
    data_storage_json = create_data_storage(dxres)
    payload = {}
    payload["name"] = dxres.dx_project_props["seq_run_name"]
    payload["sequencing_request_id"] = sreq.id
    payload["status"] = "finished"
    payload["data_storage_id"]	= data_storage_json["id"]
    payload["lane"] = dxres.dx_project_props["seq_lane_index"]
    return models.SequencingRun.post(payload)

def create_data_storage(dxres):
    """
    Creates a DataStorage record for the given SequencingRun record based on the given DNAnexus
    sequencing results. After the DataStorage record is created, a few attribuets of the SequencingRun
    object will then be set:

        1. `SequencingRun.data_storage_id`: Link to newly creatd DataStroage record.
        2. `SequencingRun.lane`: Set to the value of the DNAnexus project property "seq_lane_index".
        3. `SequencingRun.status`: Set to "finished".


     Note that I would also like to try and set the attributes `SequencingRun.forward_read_len` and
     `SequencingRun.reverse_read_len`, however, I can't obtain these results from DNAnexus based on
     the existing metadata that's sent there via GSSC.

    key in the SequeningRun record.

    Args:
        dxres: `scgpm_seqresults_dnanexus.dnanexus_utils.du.DxSeqResults()` instance that contains
               sequencing results metadata in DNAnexus for the given srun.

    Returns:
        `dict`. The response from the server containing the JSON serialization of the new
            DataStorage record.
    """
    payload = {}
    payload["name"] = dxres.dx_project_name
    payload["project_identifier"] = dxres.dx_project_id
    payload["data_storage_provider_id"] = models.DataStorage("DNAnexus")["id"]
    # Create DataStorage
    res_json = models.DataStorage.post(ds_payload)
    return res_json
