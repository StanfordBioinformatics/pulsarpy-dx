#!/usr/bin/env python3

###
#Nathaniel Watson
#Stanford School of Medicine
#Nov. 6, 2018
#nathankw@stanford.edu
###

"""
Checks DNAnexus for new sequencing result projects and imports metadata (i.e. number of reads) into
Pulsar by creating a SequencingRun object if necessary and one or more SequencingResult objects. 

See wiki documentation in the pulsar_lims GitHub repo at https://github.com/nathankw/pulsar_lims/wiki/Importing-Sequencing-Results.

If the --log-s3 flag is set, then the log files will be uploaded to S3 in the bucket specified by the
environment variable PULSARPYDX_S3. The log files will be stored in this bucket by timestamp.
"""

import argparse
import datetime
import time
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
    since_datetime = datetime.datetime.utcnow() - datetime.timedelta(days=days_ago)
    since_timestamp_milliseconds = int(since_datetime.timestamp() * 1000)
   
    projects = dxpy.api.org_find_projects(object_id=ENCODE_ORG, input_params={"created": {"after": since_timestamp_milliseconds}})
    projects = projects["results"]
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
        print(proj_id)
        du.share_with_org(project_ids=[proj_id], org=ENCODE_ORG, access_level="CONTRIBUTE")
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
