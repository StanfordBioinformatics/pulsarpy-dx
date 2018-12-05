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

If the --log-s3 flag is set, then the log files will be uploaded to S3 in the bucket specified by the
environment variable PULSARPYDX_S3. The log files will be stored in this bucket by timestamp.
"""

import argparse
import datetime
import logging
import os

import boto3
import dxpy

import pulsarpy.models
import pulsarpy.utils
from pulsarpy.elasticsearch_utils import MultipleHitsException
import scgpm_seqresults_dnanexus.dnanexus_utils as du
from pulsarpy_dx import logger, LOG_DIR
import pulsarpy_dx.utils as utils


#The environment module gbsc/gbsc_dnanexus/current should also be loaded in order to log into DNAnexus

ENCODE_ORG = "org-snyder_encode"

def get_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-d',"--days-ago",type=int,default=30, help="""
        The number of days ago to query for new projects that are billed to {}.""".format(ENCODE_ORG)
    )
    parser.add_argument("--log-s3", action="store_true", help="""
        Presence of this option means to upload the log files to the S3 bucket indicated by the
        environment variable PULSARPYDX_S3."""
    )
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()
    log_s3 = args.log_s3
    days_ago = args.days_ago 
    days_ago = "-" + str(days_ago) + "d" 
   
    projects = list(dxpy.find_projects(created_after=days_ago,billed_to=ENCODE_ORG))
    # projects is a list of dicts (was a generator)
    num_projects = len(projects)
    logger.debug("Found {} projects.".format(num_projects))
    if projects:
        for i in range(num_projects):
            logger.debug("{}. {}".format(str(i + 1), projects[i]["id"]))
    else: 
        return

    for i in projects:
        proj_id = i["id"]
        try:
            utils.import_dx_project(proj_id)
        except utils.MissingSequencingRequest:
            logger.error("No SequencingRequest for DNAnexus project {}.".format(proj_id))
        except Exception as e:
            # Send email with error details to Admin
            body = "Error importing sequencing results for DNAnexus project {}.\n\n".format(proj_id)
            body += e.__class__.__name__ + ": " + str(e)
            logger.error(body)
            form = {
                "subject": "Error in import_seq_results.py",
                "text": body,
                "to": pulsarpy.DEFAULT_TO,
            }
            res = pulsarpy.utils.send_mail(form=form, from_name="import_seq_results")
        finally:
            if log_s3:
                s3 = boto3.resource('s3')
                bucket = s3.Bucket(os.environ["PULSARPYDX_S3"])
                # Add subfolder for the present day
                upload_folder = str(datetime.date.today()) + "/"
                bucket.put_object(Key=upload_folder)  # put_object() is idempotent
                today_logs = os.path.join(LOG_DIR)
                for logfile in os.listdir(today_logs):
                    filepath = os.path.join(today_logs, logfile)
                    key = os.path.join(upload_folder, logfile)
                    bucket.upload_file(Key=key, Filename=filepath) 
               


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
    stats["pass_filter"] = read_hash["Post-Filter Reads"]
    return stats

if __name__ == "__main__":
    main()
