#!/usr/bin/env python3

###
#Nathaniel Watson
#Stanford School of Medicine
#Nov. 6, 2018
#nathankw@stanford.edu
###

"""
Accepts DNAnexus projects pending transfer to the ENCODE org, then downloads each of the projects to the
local host at the designated output directory. In DNAnexus, a project property will be added to the
project; this property is 'scHub' and will be set to True to indicate that the project was
downloaded to the SCHub pod. Project downloading is handled by the script download_cirm_dx-project.py,
which sends out notification emails as specified in the configuration file {} in both successful
and unsuccessful circomstances.".format(conf_file). See more details at
https://docs.google.com/document/d/1ykBa2D7kCihzIdixiOFJiSSLqhISSlqiKGMurpW5A6s/edit?usp=sharing
and https://docs.google.com/a/stanford.edu/document/d/1AxEqCr4dWyEPBfp2r8SMtz8YE_tTTme730LsT_3URdY/edit?usp=sharing.
"""

import os
import sys
import subprocess
import logging
import argparse
import json

import dxpy

import pulsarpy.models
from pulsarpy.elasticsearch_utils import MultipleHitsException
import scgpm_seqresults_dnanexus.dnanexus_utils as du
from pulsarpy_dx import logger
import pulsarpy_dx.utils as utils


#The environment module gbsc/gbsc_dnanexus/current should also be loaded in order to log into DNAnexus

ENCODE_ORG = "org-snyder_encode"

def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    return parser

def main():
    get_parser()
    #accept pending transfers
    transferred = du.accept_project_transfers(dx_username=DX_USER,access_level="ADMINISTER",queue="ENCODE",org=ENCODE_ORG,share_with_org="CONTRIBUTE")
    #transferred is a dict. identifying the projects that were transferred to the specified billing account. Keys are the project IDs, and values are the project names.
    logger.debug("The following projects were transferred to {org}:".format(org=ENCODE_ORG))
    logger.debug(transferred)

    if not transferred: #will be an empty dict otherwise.
        return
    transferred_proj_ids = transferred.keys()
    for t in transferred_proj_ids:
        uitls.import_dx_project(t)


def get_read_stats(barcode_stats, read_num):
    """
    .. deprecated:: 0.1.0
       Read stats are now parsed from the output of Picard Tools's CollectAlignmentSummaryMetrics.
       Such files are also stored in the DNAnexus projects created by GSSC. 

    Each barcoded library in a DNAnexus project from GSSC contains a ${barcode}_stats.json file, where ${barcode} is a 
    barcode sequence, that has read-level stats. This function accepts a barcode-specific hash 
    from that file and parses out some useful read-based stats for the given read number. 
    An example of a barcode_stats.json file is provided in the data subdirectory of this script.

    Args:
        barcode_stats: `dict`. The JSON-loaded content of a particular ${barcode}_stats.json file. 
            See `scgpm_seqresults_dnanexus.dnanexus_utils.DxSeqResults.get_barcode_stats()` for
            more details.
        read_num: `int`. The read number (1 or 2) for which you need read stats.
    """
    read_num_key = "Read {}".format(read_num)
    read_hash = barcode_stats[read_num_key]
    stats = {}
    stats["bwa_mapped"] = 
    stats["pass_filter"] = read_hash["Post-Filter Reads"]
    return stats

if __name__ == "__main__":
    main()
