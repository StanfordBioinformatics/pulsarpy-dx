#!/usr/bin/env python3

###
#Nathaniel Watson
#Stanford School of Medicine
#Nov. 6, 2018
#nathankw@stanford.edu
###

import pdb

from pulsarpy_dx import logger
from pulsarpy import models
from pulsarpy.elasticsearch_utils import MultipleHitsException
import scgpm_seqresults_dnanexus.dnanexus_utils as du 

class BarcodeNotSet(Exception):
    """
    Raised when a barcode (paired or single-end) is expected to be set on a Library record but isn't.
    """

class MissingSequencingRequest(Exception):
    """
    Raised when a SequencingRequest was expected to be found in Pulsar but was not. 
    """


def get_or_create_srun_by_ids(sreq_id, dx_project_id):
    """
    A wrapper over get_or_create_srun() below that simplifies the parameters to use IDs instead of
    objects.

    Args:
        sreq_id: `int`. A Pulsar SequencingRequest record ID.
        dx_project_id: `str`. The project ID of a DNAnexus project, i.e. FPg8yJQ900P4ZgzxFZbgJZY2.
    Returns:
        `pulsarpy.models.SequencingRun` instance.
    """
    sreq = models.SequencingRequest(sreq_id)
    dxres = du.DxSeqResults(dx_project_id=dx_project_id)
    return get_or_create_srun(sreq, dxres)
    
def get_or_create_srun(sreq, dxres):
    """
    Checks whether a given SequencingRequest record already has a SequencingRun record for a
    particular DNAnexus project. This check is satisfied if either of the following are true:

    1. There is a SequencingRun whose name attribute is equal to the value of the DNAnexus project's
       `seq_run_name` property (case-insensitive), or
    2. There is a SequencingRun with an associated DataStorage whose project_identifier attribute
       is equal to the project ID if the DNAnexus project.

    If such a SequencingRun record exists, it is returned, otherwise a new SequencingRun record
    based off of the provided DNAnexus sequencing results is created and then returned.

    Args:
        sreq: `pulsarpy.models.SequencingRequest` instance.
        dxres - `scgpm_seqresults_dnanexus.dnanexus_utils.du.DxSeqResults()` instance that contains
               sequencing results metadata from DNAnexus that represents a sequencing run of the given
               `pulsarpy.models.SequencingRequest`.
    Returns:
        `pulsarpy.models.SequencingRun` instance.
    """
    seq_run_name = dxres.dx_project_props["seq_run_name"].lower()
    srun_ids = sreq.sequencing_run_ids
    if srun_ids:
        for i in srun_ids:
            srun = models.SequencingRun(i)
            # Check by name, case-insensitive.
            if srun.name.lower() == seq_run_name:
                return srun
            # Also check by DataStorage
            elif srun.data_storage_id:
                ds = models.DataStorage(srun.data_storage_id)
                if ds.project_identifier == dxres.dx_project_id:
                    return srun
    # Create SequencingRun
    srun_json = create_srun(sreq, dxres)
    srun = models.SequencingRun(srun_json["id"])
    return srun

def create_srun(sreq, dxres):
    """
    Creates a SequencingRun record based on the provided DNAnexus sequencing results, to be linked
    to the given SequencingRequest object.

    Note that I would also like to try and set the attributes `SequencingRun.forward_read_len` and
    `SequencingRun.reverse_read_len`, however, I can't obtain these results from DNAnexus based on
    the existing metadata that's sent there via GSSC.

    Args:
        sreq: A `pulsarpy.models.SequencingRequest` instance.
        dxres: `scgpm_seqresults_dnanexus.dnanexus_utils.du.DxSeqResults()` instance that contains
               sequencing results metadata in DNAnexus for the given srun.
    """
    logger.debug("Creating SequencingRun and associated DataStorage")
    data_storage_json = create_data_storage(dxres)
    payload = {}
    payload["name"] = dxres.dx_project_props["seq_run_name"]
    payload["sequencing_request_id"] = sreq.id
    payload["status"] = "finished"
    payload["data_storage_id"]	= data_storage_json["id"]
    payload["lane"] = dxres.dx_project_props["seq_lane_index"]
    logger.debug("Creating SequencingRun record.")
    return models.SequencingRun.post(payload)

def create_data_storage(dxres):
    """
    Creates a DataStorage record for the given DNAnexus sequencing results.

    Args:
        dxres: `scgpm_seqresults_dnanexus.dnanexus_utils.du.DxSeqResults()` instance that contains
               sequencing results metadata in DNAnexus for the given srun.

    Returns:
        `dict`. The response from the server containing the JSON serialization of the new
            DataStorage record.
    """
    logger.debug("In create_data_storage().")
    payload = {}
    payload["name"] = dxres.dx_project_name
    payload["project_identifier"] = dxres.dx_project_id
    payload["data_storage_provider_id"] = models.DataStorageProvider("DNAnexus").id
    # Create DataStorage
    res_json = models.DataStorage.post(payload)
    return res_json

def check_pairedend_correct(sreq, dx_pe_val):
    """
    Checks whether the SequencingRequest.paired_end attribute and the 'paired' property of the
    DNAnexus project in question are in accordance. It's possible that the request originally went
    in as SE (or the tech forgot to check PE), but the sequencing run was acutally done PE. If this
    is the case, then the SequencingRequest.paired_end attribute will be updated to be set to True
    in order that PE sequencing results will be allowed (PE attributes of a SequencingResult will
    be hidden in the UI if the SequencingRequest is set to paired_end being false).

    Args:
        sreq: A `pulsarpy.models.SequencingRequest` instance.
        dx_pe_val: `str`. The value of the 'paired' property of the DNAnexus project in questions.
    """
    if sreq.paired_end == False:
        if dx_pe_val == "true":
            sreq.patch({"paired_end": True})

def import_dx_project(dx_project_id):
    """
    Attemps to import DNAnexus sequencing results for the given DNAnexus project ID. This entails
    having a SequencingRequest object in Pulsar that in turn has a SequecingRun object to import the results
    into, creating SequencingResult objects in the process. Thus, we must first try to find the
    appropriate SequencingRequest if it exists. If it doesn't, an Exception will be raised. 

    We first try to find the SequencingRequest by matching the value of its name attribute to the 
    DNAnexus project's library_name property value. Normally, when provinding the sequencing center a
    name for their library to be sequenced, the lab uses the record ID of the SequencingRequest object,
    which is the concatenation of the model abbreviation in Pulsar, a hyphen, and the record's primary 
    ID (i.e. SREQ-25). Normally, we could just search by the integer portion on the primary ID field. 
    However, SequencingRequests from the old Syapse LIMS have been backported into Pulsar, and they
    used the same record ID forming convention there too.  So for these records, the Syaspe record
    ID has been added into a Pulsar SequencingRequest via the name attribute. Thus, as a precaution,
    a SequencingRequest is first searched on its name attribute.  If that fails, then the SequencingRequests
    are searched on the primary ID attribute using only the interger portion of the DNAnexus project's
    library_name property. 

    Raises:
        `pulsarpy.elasticsearch_utils.MultipleHitsException`:  Multiple SequencingRequest records were found
            in searching by name in pulsarpy.models.Model.replace_name_with_id().
        `MissingSequencingRequest`: A relevant SequencingRequest record to import the DNAnexus sequencing results
            into could not be found.         
        `BarcodeNotSet`: A library on the SequencingRequest object at hand does not have a barcode
            set, making it impossible to import sequening results from DNAnexus for it.  
        `scgpm_seqresults_dnanexus.dnanexus_utils.FastqNotFound`: There aren't any FASTQ files in 
            the DNAnexus project for a given Library, based on the barcode specified for that Library.
    """
    dxres = du.DxSeqResults(dx_project_id=dx_project_id)
    logger.debug("Preparing to import DNAnexus sequencing results for {} ({}).".format(dx_project_id, dxres.dx_project_name))
    lib_name_prop = dxres.dx_project_props["library_name"]
    logger.debug("DNAnexus library_name property value: {}.".format(lib_name_prop))
    #sreq = models.SequencingRequest.find_by(payload={"name": lib_name_prop})
    # Using Elasticsearch here mainly in order to achieve a case-insensitive search on the SequencingRequest
    # name field. 
    logger.debug("Searching Pulsar for matching SequencingRequest record.")
    try:
        sreq = models.SequencingRequest(lib_name_prop) 
    except MultipleHitsException as e: # raised in pulsarpy.models.Model.replace_name_with_id()
        logger.error("Found multiple SequencingRequest records with name '{}'. Skipping DNAnexus project {} ({}) with library_name property set to '{}'".format(lib_name_prop, t, dxres.name))
        raise
    except models.RecordNotFound as e: # raised in pulsarpy.models.Model.replace_name_with_id()
        # Search by ID. The lab sometimes doesn't add a value for SequencingRequest.name and
        # instead uses the SequencingRequest record ID, which is a concatenation of the model
        # abbreviation, a hyphen, and the records primary ID. 
        try:
            sreq = models.SequencingRequest(lib_name_prop.split("-")[1])
        except models.RecordNotFound:
            msg = "Can't find Pulsar SequencingRequest for DNAnexus project {} ({}) with library_name property set to '{}'.".format(dx_project_id, dxres.dx_project_name, lib_name_prop)
            logger.error(msg)
            raise MissingSequencingRequest(msg)
    check_pairedend_correct(sreq, dxres.dx_project_props["paired_end"])
    logger.debug("Found SequencingRequest {}.".format(sreq.id))
    srun = get_or_create_srun(sreq, dxres)
    logger.debug("SequencingRun record is: {}.".format(srun.id))
    # Check if DataStorage is aleady linked to SequencingRun object. May be if user created it
    # manually in the past.
    if not srun.data_storage_id:
        ds_json = create_data_storage(dxres)
        srun.patch({"data_storage_id": ds_json["id"], "status": "finished"})

    library_sequencing_results = srun.library_sequencing_results()
    # Create SequencingResult record for each library on the SReq
    for library_id in sreq.library_ids:
        # Check if SequencingResult record for given library already exists.
        if library_id in library_sequencing_results:
            continue
        payload = {}
        payload["mapper"] = "bwa"
        payload["sequencing_run_id"] = srun.id
        payload["library_id"] = library_id
        # First check if library was 
        library = models.Library(library_id)
        barcode = library.get_barcode_sequence()
        if not barcode:
            msg = "Library {} does not have a barcode set.".format(library_id)
            logger.error(msg)
            raise BarcodeNotSet(msg)
        # Find the barcode file on DNAnexus
        logger.debug("Processing Library {} ({}) with barcode {}.".format(library.name, library_id, barcode))
        try:
            logger.debug("Locating sequencing files for Library {}, barcode {}.".format(library_id, barcode))
            barcode_files = dxres.get_fastq_files_props(barcode=barcode)
        except du.FastqNotFound as e:
            logger.error(e.message)
            raise 
        # Above - keys are the FASTQ file DXFile objects; values are the dict of associated properties
        # on DNAnexus on the file. In addition to the properties on the file in DNAnexus, an
        # additional property is present called 'fastq_file_name'.

        # Read barcode_stats.json to get mapped read counts for the given barcode:
        #barcode_stats = dxres.get_barcode_stats_json(barcode=barcode)
        
        logger.debug("Download alignment summary metrics for barcode {}.".format(barcode))
        asm = dxres.get_alignment_summary_metrics(barcode=barcode)
        for dxfile in barcode_files:
            props = barcode_files[dxfile]
            read_num = int(props["read"])
            if not read_num in [1, 2]:
                raise Exception("Unknown read number '{}'. Should be either 1 or 2.".format(read_num))

            if sreq.paired_end:
                payload["pair_aligned_perc"] = round(float(asm["PAIR"]["PCT_READS_ALIGNED_IN_PAIRS"]) * 100, 2)
            file_id = dxfile.id
            if read_num == 1:
                metrics = asm["FIRST_OF_PAIR"]
                payload["read1_uri"] = file_id
                payload["read1_count"] = metrics["PF_READS"]
                payload["read1_aligned_perc"] = round(float(metrics["PCT_PF_READS_ALIGNED"]) * 100, 2)
            else:
                metrics = asm["SECOND_OF_PAIR"]
                payload["read2_uri"] = file_id
                payload["read2_count"] = metrics["PF_READS"]
                payload["read2_aligned_perc"] = round(float(metrics["PCT_PF_READS_ALIGNED"]) * 100, 2)
        models.SequencingResult.post(payload)

